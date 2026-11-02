package server

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"net"
	"net/http"
	"reflect"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/admin"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqpsb"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/auth"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// DefaultNamespace is the name of the namespace that serves every hostname
// that names no other: localhost, an IP address or a name nobody declared.
const DefaultNamespace = "emulator"

// NamespaceOptions declares a namespace.
type NamespaceOptions struct {
	// Name is the first label of the hostname that selects the namespace,
	// such as "orders" for orders.servicebus.windows.net or orders.localhost.
	Name        string
	Queues      []Queue
	Topics      []Topic
	Policies    []auth.Policy
	Assignments []Assignment
	// DisableLocalAuth rejects SAS tokens, as local_auth_enabled = false.
	DisableLocalAuth bool
	// GeoReplication replicates the namespace's data to a second region.
	GeoReplication *GeoReplication
	// Tags, Locks and Encryption are the namespace's Azure resource
	// settings. Tags and Encryption have no effect on the data; Locks
	// protect the entities and rules from changes.
	Tags       map[string]string
	Locks      []broker.Lock
	Encryption *broker.Encryption
}

// GeoDRConfig pairs two namespaces under an alias: the Geo-Disaster
// Recovery feature. The alias serves what the primary serves, and the
// primary's entities and rules are copied to the secondary.
type GeoDRConfig struct {
	Alias, Primary, Secondary string
}

// Namespace is one Service Bus namespace of the server: its own store,
// broker, policies and endpoints.
type Namespace struct {
	name   string
	server *Server
	store  *store.Ref
	broker *broker.Broker
	auth   *auth.Authority
	amqp   *amqpsb.Handler
	admin  *admin.Handler
	portal *portal.Handler
	mux    http.Handler
	dbPath string
	opts   NamespaceOptions

	geoRep *geoReplication
}

func (s *Server) newNamespace(ctx context.Context, no NamespaceOptions, dbPath string) (*Namespace, error) {
	if no.Name == "" || strings.ContainsAny(no.Name, "./: ") {
		return nil, fmt.Errorf("invalid namespace name %q", no.Name)
	}
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	n := &Namespace{name: no.Name, server: s, store: store.NewRef(st), dbPath: dbPath, opts: no}
	fail := func(err error) (*Namespace, error) {
		if n.broker != nil {
			n.broker.Close()
		}
		st.Close()
		return nil, err
	}
	n.auth, err = auth.New(ctx, st, auth.Options{Mode: s.opts.Auth, DisableLocalAuth: no.DisableLocalAuth || s.opts.DisableLocalAuth, Entra: s.entra})
	if err != nil {
		return fail(err)
	}
	for _, p := range no.Policies {
		if err := n.auth.Declare(ctx, p); err != nil {
			return fail(fmt.Errorf("policy %s: %w", p.Name, err))
		}
	}
	for _, a := range no.Assignments {
		if err := n.auth.AddAssignment(ctx, a.Principal, a.Role, a.Scope); err != nil {
			return fail(fmt.Errorf("role assignment of %s: %w", a.Principal, err))
		}
	}
	n.broker, err = broker.New(ctx, st, broker.Options{
		Now: s.opts.Now, Logger: s.logger(), MetricsRetention: s.opts.MetricsRetention, OnDelete: n.auth.DeleteScope,
		MaxBatchDelete: s.opts.MaxBatchDelete, Partitions: s.opts.Partitions,
	})
	if err != nil {
		return fail(err)
	}
	n.amqp = &amqpsb.Handler{
		Broker: n.broker, Auth: n.auth, TxnTimeout: s.opts.TxnTimeout,
		MaxBatchSize: s.opts.MaxMessageSize, Logger: s.logger(),
	}
	n.admin = admin.NewHandler(n.broker, s.logger())
	n.admin.Auth = n.auth
	if !s.opts.DisablePortal {
		po := portal.Options{
			Broker: n.broker, Auth: n.auth, Info: n.portalInfo, Certificate: s.CertificatePEM,
			Started: s.started, Logger: s.logger(), Geo: portalGeo{n},
		}
		if s.opts.Export != nil {
			po.Export = func(context.Context) ([]portal.ExportFile, error) { return s.Export() }
		}
		n.portal = portal.NewHandler(po)
	}
	n.mux = n.httpMux()
	if err := n.declare(broker.WithActor(ctx, "config")); err != nil {
		return fail(err)
	}
	if no.GeoReplication != nil {
		if err := n.enableGeoReplication(ctx, *no.GeoReplication); err != nil {
			return fail(err)
		}
	}
	return n, nil
}

func (n *Namespace) close() error {
	var err error
	if n.geoRep != nil {
		err = n.geoRep.close()
	}
	return errors.Join(err, n.broker.Close(), n.store.Get().Close())
}

// Name returns the namespace's name.
func (n *Namespace) Name() string { return n.name }

// Broker returns the namespace's broker.
func (n *Namespace) Broker() *broker.Broker { return n.broker }

// Authority returns the namespace's policies and role assignments.
func (n *Namespace) Authority() *auth.Authority { return n.auth }

// declare creates or updates the entities listed in the namespace options.
func (n *Namespace) declare(ctx context.Context) error {
	for _, q := range n.opts.Queues {
		if err := n.ensure(ctx, broker.KindQueue, q.Name, "", q.Properties); err != nil {
			return err
		}
	}
	for _, t := range n.opts.Topics {
		if err := n.ensure(ctx, broker.KindTopic, t.Name, "", t.Properties); err != nil {
			return err
		}
		for _, sub := range t.Subscriptions {
			if err := n.ensure(ctx, broker.KindSubscription, sub.Name, t.Name, sub.Properties); err != nil {
				return err
			}
			if sub.Rules != nil {
				if err := n.broker.SetRules(ctx, broker.SubscriptionPath(t.Name, sub.Name), sub.Rules); err != nil {
					return fmt.Errorf("subscription %s/%s: %w", t.Name, sub.Name, err)
				}
			}
		}
	}
	return n.declareMeta(ctx)
}

// declareMeta applies the declared tags, encryption and locks, last, so
// that a lock does not stop the entities declared with it. Settings that
// are already as declared leave no trace in the activity log.
func (n *Namespace) declareMeta(ctx context.Context) error {
	cur := n.broker.Meta()
	if n.opts.Tags != nil && !reflect.DeepEqual(n.opts.Tags, cur.Tags) {
		if err := n.broker.SetTags(ctx, n.opts.Tags); err != nil {
			return err
		}
	}
	if n.opts.Encryption != nil && !reflect.DeepEqual(n.opts.Encryption, cur.Encryption) {
		if err := n.broker.SetEncryption(ctx, n.opts.Encryption); err != nil {
			return err
		}
	}
	for _, l := range n.opts.Locks {
		same := false
		for _, c := range cur.Locks {
			same = same || c == l
		}
		if same {
			continue
		}
		if err := n.broker.SetLock(ctx, l); err != nil {
			return fmt.Errorf("lock %s: %w", l.Name, err)
		}
	}
	return nil
}

func (n *Namespace) ensure(ctx context.Context, kind broker.EntityKind, name, topic string, props broker.Properties) error {
	if props == (broker.Properties{}) {
		props = broker.DefaultProperties(kind)
	}
	path := name
	if kind == broker.KindSubscription {
		path = broker.SubscriptionPath(topic, name)
	}
	if e, err := n.broker.Entity(path); err == nil {
		if e.Kind != kind {
			return fmt.Errorf("%s %s already exists as a %s", kind, name, e.Kind)
		}
		// Redeclaring an entity as it is must leave no trace in the
		// activity log, as a no-op terraform apply leaves none in Azure.
		if reflect.DeepEqual(e.Properties, props) {
			return nil
		}
		_, err := n.broker.UpdateEntity(ctx, path, props)
		return err
	}
	var err error
	switch kind {
	case broker.KindQueue:
		_, err = n.broker.CreateQueue(ctx, name, props)
	case broker.KindTopic:
		_, err = n.broker.CreateTopic(ctx, name, props)
	case broker.KindSubscription:
		_, err = n.broker.CreateSubscription(ctx, topic, name, props)
	}
	return err
}

// portalInfo describes the namespace for the portal's Essentials block.
func (n *Namespace) portalInfo() views.Info {
	s := n.server
	db := n.dbPath
	if db == store.Memory {
		db = "in memory"
	}
	version := s.opts.Version
	if version == "" {
		version = "dev"
	}
	i := views.Info{
		Name: n.name, Version: version, Database: db,
		ConnectionStr: s.connectionStringFor(n, s.Addr()), AMQPConnection: s.connectionStringFor(n, s.AMQPAddr()),
		HTTPConnection: s.connectionStringFor(n, s.HTTPAddr()),
	}
	if a := s.Addr(); a != nil {
		i.Endpoint = "sb://" + s.hostFor(n, a)
	}
	return i
}

// hostFor is the host:port clients use to reach n on addr. The default
// namespace answers on the plain address; the others by their name as a
// label of localhost, which the TLS certificate covers.
func (s *Server) hostFor(n *Namespace, addr net.Addr) string {
	host := dialAddr(addr)
	if n == s.def {
		return host
	}
	_, port, err := net.SplitHostPort(host)
	if err != nil {
		return strings.ToLower(n.name) + ".localhost"
	}
	return net.JoinHostPort(strings.ToLower(n.name)+".localhost", port)
}

func (s *Server) connectionStringFor(n *Namespace, addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return fmt.Sprintf("Endpoint=sb://%s;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true", s.hostFor(n, addr))
}

// namespaceFor returns the namespace a hostname selects. The first label is
// the namespace or alias name; a host with no dot, an IP address, or a name
// nobody declared selects the default namespace.
func (s *Server) namespaceFor(host string) *Namespace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.namespaceForLocked(host)
}

func (s *Server) namespaceForLocked(host string) *Namespace {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(strings.Trim(host, "[]"))
	if host == "" || net.ParseIP(host) != nil {
		return s.def
	}
	label, _, dotted := strings.Cut(host, ".")
	if !dotted {
		return s.def
	}
	if a, ok := s.aliases[label]; ok {
		return a.primary
	}
	if ns, ok := s.namespaces[label]; ok {
		return ns
	}
	return s.def
}

// Namespace returns the namespace with the given name or alias.
func (s *Server) Namespace(name string) (*Namespace, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	name = strings.ToLower(name)
	if a, ok := s.aliases[name]; ok {
		return a.primary, true
	}
	ns, ok := s.namespaces[name]
	return ns, ok
}

// Namespaces lists the namespaces, the default one first.
func (s *Server) Namespaces() []*Namespace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []*Namespace{s.def}
	for _, ns := range s.namespaces {
		if ns != s.def {
			out = append(out, ns)
		}
	}
	return out
}

// router is the AMQP handler of the server: it hands each link to the
// namespace the connection's hostname selects.
type router struct{ s *Server }

func (r *router) target(ctx context.Context) *Namespace {
	return r.s.namespaceFor(amqp.ConnHostname(ctx))
}

func (r *router) AttachReceiver(ctx context.Context, l *amqp.ReceiverLink) (amqp.MessageHandler, error) {
	return r.target(ctx).amqp.AttachReceiver(ctx, l)
}

func (r *router) AttachSender(ctx context.Context, l *amqp.SenderLink) error {
	return r.target(ctx).amqp.AttachSender(ctx, l)
}

type connNamespaceKey struct{}

// connected counts the connection on the namespace it selected, and
// remembers which, so that it is counted off the same one.
func (r *router) connected(ctx context.Context) {
	ns := r.target(ctx)
	if data := amqp.ConnData(ctx); data != nil {
		data.Store(connNamespaceKey{}, ns)
	}
	ns.broker.ConnectionOpened()
	telemetry.AddUpDown(ctx, telemetry.Connections, 1, attribute.String("osb.namespace", ns.name))
}

func (r *router) disconnected(ctx context.Context) {
	ns := r.target(ctx)
	if data := amqp.ConnData(ctx); data != nil {
		if v, ok := data.Load(connNamespaceKey{}); ok {
			ns = v.(*Namespace)
		}
	}
	ns.broker.ConnectionClosed()
	telemetry.AddUpDown(ctx, telemetry.Connections, -1, attribute.String("osb.namespace", ns.name))
}
