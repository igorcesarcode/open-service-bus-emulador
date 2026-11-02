// Package server assembles the emulator: listener, AMQP protocol, Service
// Bus handlers, broker and store.
package server

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// Options configures a Server.
type Options struct {
	// Addr is the TCP address of the port shared by every protocol, such
	// as ":5672" or "127.0.0.1:0". Empty means no shared port.
	Addr string
	// AMQPAddr, when set, adds a port for AMQP alone, plain or over TLS.
	AMQPAddr string
	// HTTPAddr, when set, adds a port for HTTP alone, plain or over TLS:
	// the management API and AMQP over WebSocket.
	HTTPAddr string
	// DBPath is the SQLite database file. Empty or store.Memory keeps all
	// state in memory.
	DBPath string
	// Queues and Topics are created at startup. Entities that already
	// exist in the database get the declared properties.
	Queues []Queue
	Topics []Topic
	// MaxMessageSize limits incoming messages and batches. Zero means
	// DefaultMaxMessageSize. The SDKs size batches from this value, so it
	// cannot be unlimited.
	MaxMessageSize uint64
	// Logger receives diagnostics. Nil disables logging.
	Logger *slog.Logger
	// Namespace is the name of the default namespace, which serves every
	// hostname that names no other. Empty means "emulator". Queues, Topics,
	// Policies and Assignments below declare its entities.
	Namespace string
	// Namespaces declares more namespaces. A client reaches one by the first
	// label of its hostname; each has its own entities, messages, policies
	// and database file (next to DBPath, with the name added).
	Namespaces []NamespaceOptions
	// GeoDR pairs namespaces under aliases.
	GeoDR []GeoDRConfig
	// GeoReplication replicates the default namespace's data to a second
	// region.
	GeoReplication *GeoReplication
	// Auth is the authentication mode: none (the default), sas or full.
	Auth auth.Mode
	// DisableLocalAuth rejects SAS tokens, as local_auth_enabled = false.
	DisableLocalAuth bool
	// Export writes the server's state as the files of the export template
	// (Config.json and main.tf). Nil leaves the portal's page out.
	Export func(*Server) ([]ExportFile, error)
	// Tags, Locks and Encryption are the default namespace's resource
	// settings.
	Tags       map[string]string
	Locks      []broker.Lock
	Encryption *broker.Encryption
	// Policies and Assignments are declared at startup.
	Policies    []auth.Policy
	Assignments []Assignment
	// OIDCIssuer and OIDCJWKS validate Entra ID tokens in full mode: the
	// expected issuer and the URL of its signing keys.
	OIDCIssuer, OIDCJWKS string
	// DevIssuer starts a token issuer for tests without Entra ID, in full
	// mode: it publishes /$auth/jwks.json and signs tokens at /$auth/token.
	DevIssuer bool
	// DisablePortal turns off the web portal at /$portal/.
	DisablePortal bool
	// Version is shown by the portal.
	Version string
	// Prometheus serves the metrics in the Prometheus text format at
	// /$metrics.
	Prometheus bool
	// Partitions is how many partitions a partitioned entity has. Zero
	// means 16, the Standard tier's count.
	Partitions int
	// TxnTimeout is how long an AMQP transaction may stay open. Zero means
	// two minutes, as in Service Bus.
	TxnTimeout time.Duration
	// MaxBatchDelete is the most messages one batch delete request removes.
	// Zero means 4000, the Premium tier's limit.
	MaxBatchDelete int
	// MetricsRetention is how long metrics and the activity log are kept.
	// Zero means 30 days.
	MetricsRetention time.Duration
	// Now overrides the clock, for tests.
	Now func() time.Time
	// TLSCertificate is served to TLS connections. Nil generates a
	// self-signed certificate at startup, for localhost and TLSHosts.
	TLSCertificate *tls.Certificate
	TLSHosts       []string
}

// ExportFile is a file of the export template.
type ExportFile = portal.ExportFile

// Export returns the export template of the server's current state, or nil
// when Options.Export is not set.
func (s *Server) Export() ([]ExportFile, error) {
	if s.opts.Export == nil {
		return nil, nil
	}
	return s.opts.Export(s)
}

// Assignment declares a role assignment of an Entra ID principal. Scope is
// an entity path, or empty for the namespace.
type Assignment struct {
	Principal string
	Role      auth.Role
	Scope     string
}

// Queue declares a queue. A zero Properties means the Service Bus defaults.
type Queue struct {
	Name       string
	Properties broker.Properties
}

// Topic declares a topic and its subscriptions.
type Topic struct {
	Name          string
	Properties    broker.Properties
	Subscriptions []Subscription
}

// Subscription declares a subscription.
type Subscription struct {
	Name       string
	Properties broker.Properties
	// Rules replaces the rules of the subscription when not nil. A nil
	// Rules keeps the existing rules, or the $Default rule of a new
	// subscription.
	Rules []broker.Rule
}

// DefaultMaxMessageSize matches the largest message a Premium namespace
// accepts.
const DefaultMaxMessageSize = 100 << 20

// Server is a running emulator instance.
type Server struct {
	opts  Options
	dev   *auth.DevIssuer
	entra *auth.Entra
	amqp  *amqp.Server

	router *router
	// mu guards the namespaces and aliases, which Geo-DR changes while the
	// server runs.
	mu         sync.RWMutex
	namespaces map[string]*Namespace // by lower-case name
	def        *Namespace
	aliases    map[string]*geoAlias // by lower-case alias name

	// ln, amqpLn and httpListener are the listeners of Addr, AMQPAddr and
	// HTTPAddr; each may be nil.
	ln, amqpLn, httpListener net.Listener
	httpLn                   *connListener
	http                     *http.Server
	tlsConfig                *tls.Config
	cert                     *tls.Certificate
	serveWG                  sync.WaitGroup
	connWG                   sync.WaitGroup

	// pending holds connections whose protocol is not known yet.
	pendingMu sync.Mutex
	pending   map[net.Conn]struct{}
	closing   bool
	err       error
	started   time.Time
}

// New opens the stores, creates the declared entities and prepares the
// server. It does not listen yet.
func New(ctx context.Context, opts Options) (*Server, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if opts.MaxMessageSize == 0 {
		opts.MaxMessageSize = DefaultMaxMessageSize
	}
	if opts.DBPath == "" {
		opts.DBPath = store.Memory
	}
	if opts.Namespace == "" {
		opts.Namespace = DefaultNamespace
	}
	var (
		entra *auth.Entra
		dev   *auth.DevIssuer
		err   error
	)
	if (opts.DevIssuer || opts.OIDCJWKS != "") && opts.Auth != auth.ModeFull {
		return nil, errors.New("server: the Entra ID token options need -auth full")
	}
	switch {
	case opts.DevIssuer:
		if dev, err = auth.NewDevIssuer("http://open-service-bus/dev-issuer"); err != nil {
			return nil, err
		}
		entra = dev.Entra()
	case opts.OIDCJWKS != "":
		entra = auth.NewJWKS(opts.OIDCIssuer, opts.OIDCJWKS, nil)
	}
	s := &Server{started: time.Now(), opts: opts, dev: dev, entra: entra, namespaces: map[string]*Namespace{}}
	s.router = &router{s: s}
	s.amqp = &amqp.Server{
		Handler:        s.router,
		MaxMessageSize: opts.MaxMessageSize,
		OnConnect:      s.router.connected,
		OnDisconnect:   s.router.disconnected,
		Logger:         logger,
	}
	declared := []NamespaceOptions{{
		Name: opts.Namespace, Queues: opts.Queues, Topics: opts.Topics, Policies: opts.Policies,
		Assignments: opts.Assignments, DisableLocalAuth: opts.DisableLocalAuth, GeoReplication: opts.GeoReplication,
		Tags: opts.Tags, Locks: opts.Locks, Encryption: opts.Encryption,
	}}
	declared = append(declared, opts.Namespaces...)
	for i, no := range declared {
		if _, dup := s.namespaces[strings.ToLower(no.Name)]; dup {
			s.closeNamespaces()
			return nil, fmt.Errorf("server: namespace %q is declared twice", no.Name)
		}
		path := opts.DBPath
		if i > 0 {
			path = namespaceDB(opts.DBPath, no.Name)
		}
		ns, err := s.newNamespace(ctx, no, path)
		if err != nil {
			s.closeNamespaces()
			return nil, fmt.Errorf("server: namespace %s: %w", no.Name, err)
		}
		s.namespaces[strings.ToLower(no.Name)] = ns
		if i == 0 {
			s.def = ns
		}
	}
	s.mu.Lock()
	err = s.restoreGeoDR(ctx, opts.GeoDR)
	s.mu.Unlock()
	if err != nil {
		s.stopGeo()
		s.closeNamespaces()
		return nil, fmt.Errorf("server: geo-dr: %w", err)
	}
	return s, nil
}

// namespaceDB names the database file of an additional namespace next to
// the default one's: osb.db becomes osb-ns2.db.
func namespaceDB(path, name string) string {
	if path == store.Memory {
		return path
	}
	base := strings.TrimSuffix(path, ".db")
	return base + "-" + strings.ToLower(name) + ".db"
}

func (s *Server) closeNamespaces() {
	for _, ns := range s.namespaces {
		ns.close()
	}
}

// Start listens on Options.Addr and serves connections in the background.
// AMQP, HTTP and TLS (carrying either) share the port: each connection is
// told apart by its first bytes.
//
// AMQPAddr and HTTPAddr add ports restricted to one protocol.
func (s *Server) Start() error {
	if s.opts.Addr == "" && s.opts.AMQPAddr == "" && s.opts.HTTPAddr == "" {
		return errors.New("server: no address to listen on")
	}
	if telemetry.Enabled() {
		s.registerGauges()
	}
	cert := s.opts.TLSCertificate
	if cert == nil {
		c, err := selfSignedCertificate(s.opts.TLSHosts)
		if err != nil {
			return fmt.Errorf("server: creating the TLS certificate: %w", err)
		}
		cert = &c
	}
	s.cert = cert
	s.tlsConfig = &tls.Config{Certificates: []tls.Certificate{*cert}, NextProtos: []string{"http/1.1"}}

	listeners := []struct {
		addr  string
		ln    *net.Listener
		allow func(protocol) bool
	}{
		{s.opts.Addr, &s.ln, func(protocol) bool { return true }},
		{s.opts.AMQPAddr, &s.amqpLn, func(p protocol) bool { return p == protoAMQP }},
		{s.opts.HTTPAddr, &s.httpListener, func(p protocol) bool { return p == protoHTTP }},
	}
	for _, l := range listeners {
		if l.addr == "" {
			continue
		}
		ln, err := net.Listen("tcp", l.addr)
		if err != nil {
			s.closeListeners()
			return fmt.Errorf("server: listen on %s: %w", l.addr, err)
		}
		*l.ln = ln
	}

	s.httpLn = newConnListener(s.HTTPAddr())
	s.http = &http.Server{Handler: s.httpHandler(), ReadHeaderTimeout: sniffTimeout}
	s.serveWG.Go(func() {
		if err := s.http.Serve(s.httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			s.err = err
		}
	})
	for _, l := range listeners {
		ln, allow := *l.ln, l.allow
		if ln == nil {
			continue
		}
		s.serveWG.Go(func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				s.connWG.Go(func() { s.route(c, allow) })
			}
		})
	}
	return nil
}

func (s *Server) closeListeners() {
	for _, ln := range []net.Listener{s.ln, s.amqpLn, s.httpListener} {
		if ln != nil {
			ln.Close()
		}
	}
}

// route serves a new connection with the protocol it speaks, if allow
// accepts it.
func (s *Server) route(raw net.Conn, allow func(protocol) bool) {
	if !s.track(raw, true) {
		raw.Close()
		return
	}
	c, proto, err := s.handshake(raw)
	s.track(raw, false)
	if err != nil {
		raw.Close()
		return
	}
	if !allow(proto) {
		s.logger().Debug("protocol not served on this port", "remote", raw.RemoteAddr().String(), "local", raw.LocalAddr().String())
		raw.Close()
		return
	}
	switch proto {
	case protoAMQP:
		s.amqp.ServeConn(c)
	default:
		s.httpLn.push(c)
	}
}

// serveAMQP serves an AMQP connection that did not come from a listener,
// such as one over WebSocket, unless the server is closing.
func (s *Server) serveAMQP(c net.Conn) {
	s.pendingMu.Lock()
	if s.closing {
		s.pendingMu.Unlock()
		c.Close()
		return
	}
	// Close waits for connWG only after setting closing, so this Add
	// cannot race with Wait.
	s.connWG.Add(1)
	s.pendingMu.Unlock()
	defer s.connWG.Done()
	s.amqp.ServeConn(c)
}

// track adds a connection to, or removes it from, the set Close closes
// while their protocol is not known yet. It reports false once closed.
func (s *Server) track(c net.Conn, add bool) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if !add {
		delete(s.pending, c)
		return true
	}
	if s.closing {
		return false
	}
	if s.pending == nil {
		s.pending = make(map[net.Conn]struct{})
	}
	s.pending[c] = struct{}{}
	return true
}

// handshake finds the protocol of c, unwrapping TLS.
func (s *Server) handshake(c net.Conn) (net.Conn, protocol, error) {
	c, proto, err := sniff(c)
	if err != nil || proto != protoTLS {
		return c, proto, err
	}
	tc := tls.Server(c, s.tlsConfig)
	tc.SetDeadline(time.Now().Add(sniffTimeout))
	if err := tc.Handshake(); err != nil {
		s.logger().Debug("TLS handshake failed", "remote", c.RemoteAddr().String(), "err", err)
		return nil, 0, err
	}
	tc.SetDeadline(time.Time{})
	c, proto, err = sniff(tc)
	if proto == protoTLS {
		proto = protoHTTP
	}
	return c, proto, err
}

// httpHandler serves AMQP over WebSocket and hands every other request to
// the namespace it is for.
func (s *Server) httpHandler() http.Handler {
	root := http.NewServeMux()
	root.Handle(webSocketPath, http.HandlerFunc(s.serveWebSocket))
	if s.dev != nil {
		root.HandleFunc("GET /$auth/jwks.json", s.serveJWKS)
		root.HandleFunc("POST /$auth/token", s.serveDevToken)
	}
	root.Handle("/", telemetry.HTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.namespaceForRequest(w, r).mux.ServeHTTP(w, r)
	}), "http"))
	return root
}

// nsCookie remembers the namespace a browser chose in the portal.
const nsCookie = "osb_ns"

// namespaceForRequest picks the namespace of an HTTP request: the Host
// header, as for AMQP, except in the portal, where ?ns= or the cookie it
// sets selects one regardless of the host, so a browser on localhost can
// reach every namespace.
func (s *Server) namespaceForRequest(w http.ResponseWriter, r *http.Request) *Namespace {
	if strings.HasPrefix(r.URL.Path, portal.Prefix) {
		name := r.URL.Query().Get("ns")
		if name == "" {
			if c, err := r.Cookie(nsCookie); err == nil {
				name = c.Value
			}
		}
		if name != "" {
			if ns, ok := s.Namespace(name); ok {
				if r.URL.Query().Get("ns") != "" {
					http.SetCookie(w, &http.Cookie{Name: nsCookie, Value: name, Path: portal.Prefix, SameSite: http.SameSiteStrictMode})
				}
				return ns
			}
		}
	}
	return s.namespaceFor(r.Host)
}

// httpMux serves the namespace's management API, metrics, and portal.
func (n *Namespace) httpMux() http.Handler {
	s := n.server
	mux := http.NewServeMux()
	mux.HandleFunc("GET /$api/metrics", n.manage(n.serveMetricsJSON))
	mux.HandleFunc("GET /$api/activity", n.manage(n.serveActivityJSON))
	if s.opts.Prometheus {
		mux.HandleFunc("GET /$metrics", n.manage(n.servePrometheus))
	}
	if n.portal != nil {
		mux.Handle(portal.Prefix, n.portal)
		// A browser that opens the emulator's address lands on the portal.
		mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.Header.Get("Accept"), "text/html") {
				http.Redirect(w, r, portal.Prefix, http.StatusFound)
				return
			}
			n.admin.ServeHTTP(w, r)
		})
	}
	mux.Handle("/", n.admin)
	return mux
}

func (s *Server) logger() *slog.Logger {
	if s.opts.Logger != nil {
		return s.opts.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// Addr returns the address of the shared port, or of the AMQP port when
// there is no shared port. It is nil before Start.
func (s *Server) Addr() net.Addr {
	if s.ln != nil {
		return s.ln.Addr()
	}
	return s.AMQPAddr()
}

// AMQPAddr returns the address of the AMQP port, or of the shared port
// when there is no AMQP port.
func (s *Server) AMQPAddr() net.Addr {
	switch {
	case s.amqpLn != nil:
		return s.amqpLn.Addr()
	case s.ln != nil:
		return s.ln.Addr()
	}
	return nil
}

// HTTPAddr returns the address of the HTTP port, or of the shared port
// when there is no HTTP port.
func (s *Server) HTTPAddr() net.Addr {
	switch {
	case s.httpListener != nil:
		return s.httpListener.Addr()
	case s.ln != nil:
		return s.ln.Addr()
	}
	return nil
}

// ConnectionString returns a Service Bus connection string for the
// development emulator mode of the official SDKs, on Addr. Wildcard listen
// addresses are reported as localhost. Without a shared port,
// administration clients need HTTPConnectionString.
func (s *Server) ConnectionString() string {
	return connectionString(s.Addr())
}

// HTTPConnectionString is ConnectionString on HTTPAddr, for
// administration clients and AMQP over WebSocket.
func (s *Server) HTTPConnectionString() string {
	return connectionString(s.HTTPAddr())
}

func connectionString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return fmt.Sprintf("Endpoint=sb://%s;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true",
		dialAddr(addr))
}

// dialAddr turns a listening address into one clients can dial.
func dialAddr(addr net.Addr) string {
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	if ip := net.ParseIP(host); host == "" || ip != nil && ip.IsUnspecified() {
		host = "localhost"
	}
	return net.JoinHostPort(host, port)
}

// CertificatePEM returns the TLS certificate chain in PEM form, for clients
// to trust. It is nil before Start.
func (s *Server) CertificatePEM() []byte {
	if s.cert == nil {
		return nil
	}
	var out []byte
	for _, der := range s.cert.Certificate {
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	return out
}

// Broker returns the broker of the default namespace.
func (s *Server) Broker() *broker.Broker { return s.def.broker }

// Close stops listening, closes every connection and the database.
func (s *Server) Close() error {
	s.pendingMu.Lock()
	s.closing = true
	for c := range s.pending {
		c.Close()
	}
	s.pendingMu.Unlock()
	var err error
	s.closeListeners()
	if s.http != nil {
		s.httpLn.Close()
		err = s.http.Close()
	}
	err = errors.Join(err, s.amqp.Close())
	s.serveWG.Wait()
	s.connWG.Wait()
	s.mu.Lock()
	s.stopGeo()
	for _, ns := range s.namespaces {
		err = errors.Join(err, ns.close())
	}
	s.mu.Unlock()
	return errors.Join(err, s.err)
}

// Authority returns the policies and role assignments of the default
// namespace.
func (s *Server) Authority() *auth.Authority { return s.def.auth }

// DevIssuer returns the test token issuer, or nil without Options.DevIssuer.
func (s *Server) DevIssuer() *auth.DevIssuer { return s.dev }
