// Command open-service-bus runs a local Azure Service Bus emulator.
//
// Usage:
//
//	open-service-bus [-listen :5672] [-amqp-addr addr] [-http-addr addr] [-db file] [-terraform dir] [-config Config.json] [-queue name]... [-topic name]... [-subscription topic/name]...
//
// The port serves AMQP, AMQP over TLS, the management API over HTTP and
// HTTPS, and AMQP over WebSocket at /$servicebus/websocket. -amqp-addr and
// -http-addr add ports restricted to AMQP or to HTTP; -listen "" leaves
// only those. Go administration clients always use HTTPS: write the certificate
// with -tls-cert-out and trust it (SSL_CERT_FILE on Linux), or pass a
// certificate of your own with -tls-cert and -tls-key.
//
// -terraform reads the entities of a production Terraform configuration: the
// azurerm_servicebus_* resources of the .tf files in a directory, with
// their modules. Set its variables with -tf-var name=value or -tf-var-file;
// terraform.tfvars, *.auto.tfvars and TF_VAR_ variables are read too. As in
// Azure, a subscription that declares rules keeps its $Default rule, which
// matches every message; -tf-drop-default-rule removes it.
//
// -config reads entities and their properties from a Config.json file in the
// format of the official emulator; entities it shares with -terraform take
// its properties. -queue, -topic and -subscription add entities with the
// default properties.
//
// -mcp-addr turns on the MCP (Model Context Protocol) endpoint at /mcp of that
// address, which lets an agent manage the emulator: entities, rules, messages,
// configuration and status. It is off by default, has a port of its own and no
// login, so bind it to a loopback address. -mcp-readonly leaves out the tools
// that change anything, and the tools that touch messages need -mcp-allow-data.
// At startup the three also come from the environment, as OSB_MCP_ADDR,
// OSB_MCP_READONLY and OSB_MCP_ALLOW_DATA; a flag overrides its variable.
//
// Clients connect with a development emulator connection string such as:
//
//	Endpoint=sb://localhost:5672;SharedAccessKeyName=x;SharedAccessKey=x;UseDevelopmentEmulator=true
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/config"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/export"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/mcp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/terraform"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/unsupported"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "open-service-bus:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("open-service-bus", flag.ContinueOnError)
	listen := fs.String("listen", ":5672", "TCP address shared by every protocol; empty disables it")
	amqpAddr := fs.String("amqp-addr", "", "extra TCP address for AMQP and AMQPS alone")
	httpAddr := fs.String("http-addr", "", "extra TCP address for HTTP and HTTPS alone: management API and AMQP over WebSocket")
	showVersion := fs.Bool("version", false, "print the version and exit")
	dbPath := fs.String("db", "", "SQLite database file; empty keeps state in memory")
	configPath := fs.String("config", "", "Config.json file of the official emulator declaring entities")
	tfPath := fs.String("terraform", "", "Terraform directory or .tf file declaring the entities")
	tfVars := map[string]string{}
	fs.Func("tf-var", "Terraform variable as name=value; repeatable", func(v string) error {
		name, value, ok := strings.Cut(v, "=")
		if !ok || name == "" {
			return fmt.Errorf("want name=value, got %q", v)
		}
		tfVars[name] = value
		return nil
	})
	var tfVarFiles []string
	fs.Func("tf-var-file", "Terraform .tfvars file; repeatable", func(v string) error {
		tfVarFiles = append(tfVarFiles, v)
		return nil
	})
	tfDropDefault := fs.Bool("tf-drop-default-rule", false, "remove the $Default rule of Terraform subscriptions that declare rules")
	tfModuleMap := fs.String("tf-module-map", "", "JSON file naming the entity list arguments of Terraform modules whose source is not available")
	authMode := fs.String("auth", "none", "authentication: none accepts every client; sas validates shared access signatures; full also accepts Entra ID tokens")
	disableLocalAuth := fs.Bool("disable-local-auth", false, "reject shared access signatures, as local_auth_enabled = false does")
	devIssuer := fs.Bool("dev-issuer", false, "with -auth full, issue test tokens at /$auth/token and publish their key at /$auth/jwks.json")
	oidcIssuer := fs.String("oidc-issuer", "", "with -auth full, the issuer of the Entra ID tokens to accept; empty accepts any")
	oidcJWKS := fs.String("oidc-jwks", "", "with -auth full, the URL of the signing keys (JWKS) of the Entra ID tokens")
	exportDir := fs.String("export", "", "write the state of the emulator (after loading -db, -config and -terraform) as Config.json and main.tf into this directory, then exit")
	nsName := fs.String("namespace", "", "name of the default namespace, which serves every hostname that names no other; empty takes the first declared, or \"emulator\"")
	partitions := fs.Int("partitions", 16, "partitions of a partitioned queue or topic; the size quota is MaxSizeInMegabytes times this")
	txnTimeout := fs.Duration("txn-timeout", 2*time.Minute, "how long an AMQP transaction may stay open")
	maxBatchDelete := fs.Int("max-batch-delete", 4000, "most messages one batch delete (purge) request removes")
	portalOn := fs.Bool("portal", true, "serve the web portal at /$portal/")
	prometheus := fs.Bool("prometheus", false, "serve metrics in the Prometheus format at /$metrics")
	metricsRetention := fs.Duration("metrics-retention", 30*24*time.Hour, "how long metrics and the activity log are kept")
	pprofAddr := fs.String("pprof", "", "serve Go profiling data (net/http/pprof, at /debug/pprof/) on this address, such as localhost:6060; empty turns it off")
	mcpReadOnlyEnv, err := envBool("OSB_MCP_READONLY")
	if err != nil {
		return err
	}
	mcpAllowDataEnv, err := envBool("OSB_MCP_ALLOW_DATA")
	if err != nil {
		return err
	}
	mcpAddr := fs.String("mcp-addr", os.Getenv("OSB_MCP_ADDR"), "serve the MCP (Model Context Protocol) management endpoint on this address, such as 127.0.0.1:5680; empty turns it off. It has no login. Default: $OSB_MCP_ADDR")
	mcpReadOnly := fs.Bool("mcp-readonly", mcpReadOnlyEnv, "with -mcp-addr, offer only the tools that read. Default: $OSB_MCP_READONLY")
	mcpAllowData := fs.Bool("mcp-allow-data", mcpAllowDataEnv, "with -mcp-addr, also offer the tools that read, send and delete messages. Default: $OSB_MCP_ALLOW_DATA")
	otelEndpoint := fs.String("otel-endpoint", "", "export traces, metrics and logs with OpenTelemetry (OTLP over HTTP) to this base URL, such as http://localhost:4318; empty turns it off. OTEL_EXPORTER_OTLP_HEADERS and OTEL_RESOURCE_ATTRIBUTES are honored")
	otelSignals := fs.String("otel-signals", "traces,metrics,logs", "with -otel-endpoint, what to export: traces, metrics, logs")
	otelSample := fs.Float64("otel-sample", 1, "with -otel-endpoint, the fraction of traces to record, 0 to 1")
	otelInterval := fs.Duration("otel-metric-interval", 10*time.Second, "with -otel-endpoint, how often metrics are exported")
	otelService := fs.String("otel-service-name", "open-service-bus", "with -otel-endpoint, the service.name of the telemetry")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn or error")
	tlsCert := fs.String("tls-cert", "", "PEM certificate for TLS connections; empty generates a self-signed one")
	tlsKey := fs.String("tls-key", "", "PEM private key of -tls-cert")
	tlsCertOut := fs.String("tls-cert-out", "", "file to write the TLS certificate to, for clients to trust")
	var tlsHosts []string
	fs.Func("tls-hosts", "extra host names or IPs for the generated certificate; repeatable or comma separated", appendNames(&tlsHosts))
	maxMessageSize := fs.Uint64("max-message-size", server.DefaultMaxMessageSize, "largest accepted message in bytes")
	var queues, topics, subs []string
	fs.Func("queue", "queue to create; repeatable or comma separated", appendNames(&queues))
	fs.Func("topic", "topic to create; repeatable or comma separated", appendNames(&topics))
	fs.Func("subscription", "subscription to create as topic/name; repeatable or comma separated", appendNames(&subs))
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := applyEnv(fs); err != nil {
		return err
	}
	if *showVersion {
		fmt.Println("open-service-bus", version)
		return nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("invalid -log-level: %w", err)
	}
	levelVar := new(slog.LevelVar)
	levelVar.Set(level)
	signals, err := telemetry.ParseSignals(*otelSignals)
	if err != nil {
		return fmt.Errorf("invalid -otel-signals: %w", err)
	}
	tel, err := telemetry.Setup(context.Background(), telemetry.Config{
		Endpoint: *otelEndpoint, Signals: signals, SampleRatio: *otelSample, MetricInterval: *otelInterval,
		ServiceName: *otelService, Version: version,
		Attributes: []attribute.KeyValue{
			attribute.String("osb.auth.mode", *authMode),
			attribute.Bool("osb.db.file", *dbPath != ""),
			attribute.Int("osb.partitions", *partitions),
		},
	})
	if err != nil {
		return err
	}
	// Flush what is pending once everything else has stopped.
	defer func() {
		flush, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tel.Shutdown(flush)
	}()
	logger := slog.New(tel.LogHandler(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: levelVar}), levelVar))
	if *otelEndpoint != "" {
		logger.Info("opentelemetry on", "endpoint", *otelEndpoint, "signals", *otelSignals, "sample", *otelSample)
	}

	opts := server.Options{Addr: *listen, AMQPAddr: *amqpAddr, HTTPAddr: *httpAddr, DBPath: *dbPath, MaxMessageSize: *maxMessageSize, MaxBatchDelete: *maxBatchDelete, TxnTimeout: *txnTimeout, Partitions: *partitions, DisablePortal: !*portalOn, Version: version, Prometheus: *prometheus, MetricsRetention: *metricsRetention, Logger: logger}
	entities := &config.Config{}
	if *tfPath != "" {
		var moduleMap map[string]terraform.ModuleLists
		if *tfModuleMap != "" {
			var err error
			if moduleMap, err = terraform.LoadModuleMap(*tfModuleMap); err != nil {
				return fmt.Errorf("-tf-module-map: %w", err)
			}
		}
		_, loadDone := telemetry.Operation(context.Background(), "startup.load_terraform", attribute.String("osb.path", *tfPath))
		r, err := terraform.Load(*tfPath, terraform.Options{Vars: tfVars, VarFiles: tfVarFiles, DropDefaultRule: *tfDropDefault, ModuleMap: moduleMap})
		loadDone(&err)
		if err != nil {
			return fmt.Errorf("-terraform: %w", err)
		}
		for _, w := range r.Warnings {
			logger.Warn(w, "terraform", *tfPath)
		}
		entities = r.Config
	}
	if *configPath != "" {
		_, cfgDone := telemetry.Operation(context.Background(), "startup.load_config", attribute.String("osb.path", *configPath))
		c, err := config.LoadJSONFile(*configPath)
		cfgDone(&err)
		if err != nil {
			return err
		}
		entities.Merge(c)
	}
	unsupported.Log(logger, "configuration", entities.Unsupported)
	def, others := entities.Split(*nsName)
	opts.Namespace = *nsName
	if opts.Namespace == "" {
		opts.Namespace = def.Name
	}
	// A full host name, as in "orders.servicebus.windows.net", is its first
	// label, which is what selects a namespace.
	opts.Namespace, _, _ = strings.Cut(opts.Namespace, ".")
	for _, o := range others {
		opts.Namespaces = append(opts.Namespaces, server.NamespaceOptions{
			Name: o.Name, Queues: o.Queues, Topics: o.Topics, Policies: o.Policies, Assignments: o.Assignments,
			DisableLocalAuth: o.LocalAuthDisabled, GeoReplication: o.GeoReplication,
			Tags: o.Tags, Locks: o.Locks, Encryption: o.Encryption,
		})
	}
	opts.GeoDR, opts.GeoReplication = entities.GeoDR, def.GeoReplication
	entities = def
	opts.Queues, opts.Topics = entities.Queues, entities.Topics
	opts.Tags, opts.Locks, opts.Encryption = entities.Tags, entities.Locks, entities.Encryption
	mode, err := auth.ParseMode(*authMode)
	if err != nil {
		return fmt.Errorf("-auth: %w", err)
	}
	opts.Auth = mode
	opts.DevIssuer, opts.OIDCIssuer, opts.OIDCJWKS = *devIssuer, *oidcIssuer, *oidcJWKS
	// A premium_messaging_partitions in the Terraform sets the count unless
	// -partitions was passed.
	explicit := false
	fs.Visit(func(f *flag.Flag) { explicit = explicit || f.Name == "partitions" })
	if entities.Partitions > 0 && !explicit {
		opts.Partitions = entities.Partitions
	}
	opts.DisableLocalAuth = *disableLocalAuth || entities.LocalAuthDisabled
	opts.Policies, opts.Assignments = entities.Policies, entities.Assignments
	if err := addEntities(&opts, queues, topics, subs); err != nil {
		return err
	}
	opts.TLSHosts = tlsHosts
	if *tlsCert != "" || *tlsKey != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			return fmt.Errorf("loading -tls-cert and -tls-key: %w", err)
		}
		opts.TLSCertificate = &cert
	}

	if *pprofAddr != "" {
		if err := servePprof(*pprofAddr, logger); err != nil {
			return fmt.Errorf("-pprof: %w", err)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opts.Export = export.Files
	_, newDone := telemetry.Operation(ctx, "startup.new_server")
	srv, err := server.New(ctx, opts)
	newDone(&err)
	if err != nil {
		return err
	}
	if *exportDir != "" {
		err := export.Write(srv, *exportDir)
		return errors.Join(err, srv.Close())
	}
	if err := srv.Start(); err != nil {
		return err
	}
	if *tlsCertOut != "" {
		if err := os.WriteFile(*tlsCertOut, srv.CertificatePEM(), 0o644); err != nil {
			srv.Close()
			return fmt.Errorf("writing -tls-cert-out: %w", err)
		}
	}
	var m *mcp.Server
	if *mcpAddr != "" {
		m = mcp.New(mcp.Options{Addr: *mcpAddr, Emulator: srv, ReadOnly: *mcpReadOnly, AllowData: *mcpAllowData, LogLevel: levelVar, Logger: logger})
		if err := m.Start(); err != nil {
			srv.Close()
			return err
		}
		if !isLoopback(m.Addr()) {
			logger.Warn("the MCP endpoint listens beyond this machine and has no login: anyone who reaches it manages the emulator", "mcp_addr", m.Addr().String())
		}
		logger.Info("mcp listening", "addr", m.Addr().String(), "url", "http://"+m.Addr().String()+"/mcp", "readonly", *mcpReadOnly, "allow_data", *mcpAllowData)
	}
	logger.Info("open-service-bus listening",
		"version", version,
		"addr", srv.Addr().String(),
		"http_addr", srv.HTTPAddr().String(),
		"auth", string(mode),
		"db", opts.DBPath,
		"config", *configPath,
		"terraform", *tfPath,
		"queues", len(opts.Queues),
		"topics", len(opts.Topics),
		"connection_string", srv.ConnectionString(),
		"http_connection_string", srv.HTTPConnectionString())

	<-ctx.Done()
	logger.Info("shutting down")
	// The MCP endpoint goes first: its calls use the server.
	if m != nil {
		m.Close()
	}
	return srv.Close()
}

// envName is the environment variable that stands for the flag name:
// -max-message-size is OSB_MAX_MESSAGE_SIZE.
func envName(name string) string {
	return "OSB_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

// applyEnv gives each flag that the command line left out the value of its
// environment variable, if it is set and not empty, so a container can be
// configured without arguments. Flags that repeat take several values
// separated by commas (-queue, -topic, -subscription, -tls-hosts) or by
// newlines (-tf-var, -tf-var-file).
func applyEnv(fs *flag.FlagSet) error {
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	var err error
	fs.VisitAll(func(f *flag.Flag) {
		if err != nil || given[f.Name] || f.Name == "version" || strings.HasPrefix(f.Name, "mcp-") {
			return
		}
		v := os.Getenv(envName(f.Name))
		if v == "" {
			return
		}
		values := []string{v}
		if f.Name == "tf-var" || f.Name == "tf-var-file" {
			values = strings.Split(v, "\n")
		}
		for _, v := range values {
			if v = strings.TrimSpace(v); v == "" {
				continue
			}
			if e := fs.Set(f.Name, v); e != nil {
				err = fmt.Errorf("%s: %w", envName(f.Name), e)
				return
			}
		}
	})
	return err
}

// envBool reads a boolean environment variable; unset means false.
func envBool(name string) (bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: want true or false, got %q", name, v)
	}
	return b, nil
}

// isLoopback reports whether addr is reachable only from this machine.
func isLoopback(addr net.Addr) bool {
	t, ok := addr.(*net.TCPAddr)
	return ok && t.IP.IsLoopback()
}

// addEntities adds the entities named by the -queue, -topic and
// -subscription flags, with the default properties. Names already declared,
// such as by -config, are kept as declared.
func addEntities(opts *server.Options, queues, topics, subs []string) error {
	declared := map[string]bool{}
	for _, q := range opts.Queues {
		declared[strings.ToLower(q.Name)] = true
	}
	for _, q := range queues {
		if !declared[strings.ToLower(q)] {
			declared[strings.ToLower(q)] = true
			opts.Queues = append(opts.Queues, server.Queue{Name: q})
		}
	}

	topicIndex := map[string]int{}
	subDeclared := map[string]bool{}
	for i, t := range opts.Topics {
		topicIndex[strings.ToLower(t.Name)] = i
		for _, s := range t.Subscriptions {
			subDeclared[strings.ToLower(t.Name+"/"+s.Name)] = true
		}
	}
	addTopic := func(name string) int {
		if i, ok := topicIndex[strings.ToLower(name)]; ok {
			return i
		}
		opts.Topics = append(opts.Topics, server.Topic{Name: name})
		topicIndex[strings.ToLower(name)] = len(opts.Topics) - 1
		return len(opts.Topics) - 1
	}
	for _, t := range topics {
		addTopic(t)
	}
	for _, s := range subs {
		topic, name, ok := strings.Cut(s, "/")
		if !ok || topic == "" || name == "" {
			return fmt.Errorf("invalid -subscription %q: want topic/name", s)
		}
		i := addTopic(topic)
		if key := strings.ToLower(topic + "/" + name); !subDeclared[key] {
			subDeclared[key] = true
			opts.Topics[i].Subscriptions = append(opts.Topics[i].Subscriptions, server.Subscription{Name: name})
		}
	}
	return nil
}

func appendNames(dst *[]string) func(string) error {
	return func(v string) error {
		for _, name := range strings.Split(v, ",") {
			if name = strings.TrimSpace(name); name != "" {
				*dst = append(*dst, name)
			}
		}
		return nil
	}
}
