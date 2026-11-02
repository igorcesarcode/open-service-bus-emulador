package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// geoAlias is a Geo-Disaster Recovery alias: a hostname that serves one
// namespace, the primary, while the primary's entities, rules and policies
// are copied to a paired secondary. Messages are not copied.
type geoAlias struct {
	name string

	// mu guards the fields below, and serializes replication with
	// failover.
	mu        sync.Mutex
	primary   *Namespace
	secondary *Namespace // nil when the pairing is broken
	lastSync  time.Time
	lastErr   error

	signal chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
}

// GeoDRStatus describes a Geo-DR alias.
type GeoDRStatus struct {
	Alias     string
	Primary   string
	Secondary string
	// Paired is false after a failover or a broken pairing: the alias then
	// serves its primary with no secondary.
	Paired   bool
	LastSync time.Time
	// Error is why the last replication failed, if it did.
	Error string
}

var errGeoDR = errors.New("geo-dr")

func geoErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errGeoDR, fmt.Sprintf(format, args...))
}

// ErrGeoDR wraps the errors of Geo-DR operations that the caller caused.
var ErrGeoDR = errGeoDR

func (s *Server) namespaceNamed(name string) (*Namespace, bool) {
	ns, ok := s.namespaces[strings.ToLower(name)]
	return ns, ok
}

// pairedLocked reports whether ns is in an active pairing.
func (s *Server) pairedLocked(ns *Namespace) (*geoAlias, bool) {
	for _, a := range s.aliases {
		a.mu.Lock()
		paired := a.secondary != nil && (a.primary == ns || a.secondary == ns)
		a.mu.Unlock()
		if paired {
			return a, true
		}
	}
	return nil, false
}

// PairGeoDR pairs two namespaces under an alias: the alias serves primary,
// whose entities, rules and policies are copied to secondary and kept in
// sync, and secondary refuses changes of them. An alias a failover left
// unpaired can be paired again, with its current primary and a new
// secondary: only moving forward is supported, as in Azure.
func (s *Server) PairGeoDR(ctx context.Context, alias, primary, secondary string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if alias == "" || strings.ContainsAny(alias, "./: ") {
		return geoErr("invalid alias %q", alias)
	}
	key := strings.ToLower(alias)
	p, ok := s.namespaceNamed(primary)
	if !ok {
		return geoErr("no namespace %q", primary)
	}
	sec, ok := s.namespaceNamed(secondary)
	if !ok {
		return geoErr("no namespace %q", secondary)
	}
	if p == sec {
		return geoErr("a namespace cannot be paired with itself")
	}
	if other, ok := s.namespaces[key]; ok && other != p {
		return geoErr("the alias %q is the name of another namespace; an alias can reuse only its primary's name", alias)
	}
	for _, ns := range []*Namespace{p, sec} {
		if ns.geoRep != nil {
			return geoErr("namespace %s uses Geo-Replication, which cannot be combined with Geo-DR", ns.name)
		}
		if a, paired := s.pairedLocked(ns); paired {
			return geoErr("namespace %s is already paired under the alias %s", ns.name, a.name)
		}
	}
	a, exists := s.aliases[key]
	if exists {
		a.mu.Lock()
		current := a.primary
		a.mu.Unlock()
		if current != p {
			return geoErr("the alias %s serves %s; pair it with that namespace as primary", alias, current.name)
		}
	} else {
		a = &geoAlias{name: alias}
		if s.aliases == nil {
			s.aliases = map[string]*geoAlias{}
		}
		s.aliases[key] = a
	}
	if err := s.startPairing(ctx, a, p, sec); err != nil {
		if !exists {
			delete(s.aliases, key)
		}
		return err
	}
	return s.saveAlias(ctx, a)
}

// startPairing makes sec the secondary of a, copies the metadata once and
// starts keeping it in sync.
func (s *Server) startPairing(ctx context.Context, a *geoAlias, p, sec *Namespace) error {
	a.mu.Lock()
	a.primary, a.secondary = p, sec
	signal, done := make(chan struct{}, 1), make(chan struct{})
	a.signal, a.done = signal, done
	a.mu.Unlock()
	sec.broker.SetReadOnly(true)
	if err := a.replicate(ctx); err != nil {
		sec.broker.SetReadOnly(false)
		a.mu.Lock()
		a.secondary = nil
		a.mu.Unlock()
		return geoErr("copying the metadata of %s to %s: %v", p.name, sec.name, err)
	}
	p.broker.OnMetadataChange(a.notify)
	p.auth.OnChange(a.notify)
	a.wg.Add(1)
	go a.run(signal, done)
	return nil
}

func (a *geoAlias) notify() {
	a.mu.Lock()
	ch := a.signal
	a.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// run replicates after each change, once the changes pause for a moment. It
// gets its channels from the caller: reading them from a here could race with
// stopPairing, which clears them, and then run would wait on nil forever.
func (a *geoAlias) run(signal <-chan struct{}, done <-chan struct{}) {
	defer a.wg.Done()
	for {
		select {
		case <-done:
			return
		case <-signal:
		}
		for settled := false; !settled; {
			select {
			case <-signal:
			case <-time.After(30 * time.Millisecond):
				settled = true
			case <-done:
				return
			}
		}
		a.replicate(context.Background())
	}
}

// stopPairing ends the replication of a. The caller holds the server lock.
func (a *geoAlias) stopPairing() {
	a.mu.Lock()
	done, p, sec := a.done, a.primary, a.secondary
	a.done, a.signal, a.secondary = nil, nil, nil
	a.mu.Unlock()
	if done == nil {
		return
	}
	close(done)
	a.wg.Wait()
	if p != nil {
		p.broker.OnMetadataChange(nil)
		p.auth.OnChange(nil)
	}
	if sec != nil {
		sec.broker.SetReadOnly(false)
	}
}

// replicate copies the entities, rules and policies of the primary to the
// secondary: it makes the secondary's equal to the primary's.
func (a *geoAlias) replicate(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.secondary == nil {
		return nil
	}
	err := replicateMetadata(broker.WithActor(broker.WithReplication(ctx), "geo-dr"), a.primary, a.secondary)
	a.lastSync, a.lastErr = time.Now(), err
	return err
}

func replicateMetadata(ctx context.Context, src, dst *Namespace) error {
	sb, db := src.broker, dst.broker
	want := map[string]bool{}
	var tops, subs, queues []*broker.Entity
	queues = sb.Entities(broker.KindQueue, "")
	tops = sb.Entities(broker.KindTopic, "")
	subs = sb.Entities(broker.KindSubscription, "")
	for _, group := range [][]*broker.Entity{queues, tops, subs} {
		for _, e := range group {
			want[e.Path] = true
		}
	}
	// Entities only the secondary has are removed first: a topic takes its
	// subscriptions with it, and a name may have changed kind.
	for _, kind := range []broker.EntityKind{broker.KindSubscription, broker.KindQueue, broker.KindTopic} {
		for _, e := range db.Entities(kind, "") {
			if !want[e.Path] {
				if err := db.DeleteEntity(ctx, e.Path); err != nil && !errors.Is(err, broker.ErrEntityNotFound) {
					return err
				}
				continue
			}
			if sbe, err := sb.Entity(e.Path); err == nil && sbe.Kind != e.Kind {
				if err := db.DeleteEntity(ctx, e.Path); err != nil && !errors.Is(err, broker.ErrEntityNotFound) {
					return err
				}
			}
		}
	}
	for _, group := range [][]*broker.Entity{queues, tops, subs} {
		for _, e := range group {
			cur, err := db.Entity(e.Path)
			switch {
			case err == nil:
				if cur.Properties != e.Properties {
					if _, err := db.UpdateEntity(ctx, e.Path, e.Properties); err != nil {
						return err
					}
				}
			case errors.Is(err, broker.ErrEntityNotFound):
				switch e.Kind {
				case broker.KindQueue:
					_, err = db.CreateQueue(ctx, e.Name, e.Properties)
				case broker.KindTopic:
					_, err = db.CreateTopic(ctx, e.Name, e.Properties)
				case broker.KindSubscription:
					_, err = db.CreateSubscription(ctx, e.Topic.Name, e.Name, e.Properties)
				}
				if err != nil {
					return err
				}
			default:
				return err
			}
			if e.Kind == broker.KindSubscription {
				rules, err := sb.Rules(e.Path)
				if err != nil {
					return err
				}
				if err := db.SetRules(ctx, e.Path, rules); err != nil {
					return err
				}
			}
		}
	}
	// Shared access policies are part of the namespace's configuration.
	have := map[string]bool{}
	for _, p := range src.auth.Policies("", true) {
		have[p.Scope+"\x00"+p.Name] = true
		if cur, ok := dst.auth.Policy(p.Scope, p.Name); !ok || cur != p {
			if err := dst.auth.SavePolicy(ctx, p); err != nil {
				return err
			}
		}
	}
	for _, p := range dst.auth.Policies("", true) {
		if !have[p.Scope+"\x00"+p.Name] {
			if err := dst.auth.DeletePolicy(ctx, p.Scope, p.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// SyncGeoDR copies the primary's metadata to the secondary now and returns
// when it is done, instead of waiting for the replication that follows each
// change.
func (s *Server) SyncGeoDR(ctx context.Context, alias string) error {
	s.mu.RLock()
	a, ok := s.aliases[strings.ToLower(alias)]
	s.mu.RUnlock()
	if !ok {
		return geoErr("no alias %q", alias)
	}
	return a.replicate(ctx)
}

// FailoverGeoDR makes the secondary the namespace the alias serves, and
// breaks the pairing: the old primary keeps what it has, and is not paired
// again. A safe failover copies what is pending first; a forced one does
// not wait, which loses the changes not copied yet.
func (s *Server) FailoverGeoDR(ctx context.Context, alias string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.aliases[strings.ToLower(alias)]
	if !ok {
		return geoErr("no alias %q", alias)
	}
	a.mu.Lock()
	sec := a.secondary
	a.mu.Unlock()
	if sec == nil {
		return geoErr("the alias %s is not paired", alias)
	}
	if !force {
		if err := a.replicate(ctx); err != nil {
			return geoErr("the safe failover could not copy the pending changes: %v", err)
		}
	}
	a.stopPairing()
	a.mu.Lock()
	a.primary = sec
	a.mu.Unlock()
	return s.saveAlias(ctx, a)
}

// BreakGeoDR ends the pairing of an alias: the alias keeps serving its
// primary, and the secondary becomes an ordinary namespace.
func (s *Server) BreakGeoDR(ctx context.Context, alias string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.aliases[strings.ToLower(alias)]
	if !ok {
		return geoErr("no alias %q", alias)
	}
	a.mu.Lock()
	paired := a.secondary != nil
	a.mu.Unlock()
	if !paired {
		return geoErr("the alias %s is not paired", alias)
	}
	a.stopPairing()
	return s.saveAlias(ctx, a)
}

// DeleteGeoDRAlias removes an alias, breaking its pairing first. Its
// hostname then selects the default namespace again.
func (s *Server) DeleteGeoDRAlias(ctx context.Context, alias string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(alias)
	a, ok := s.aliases[key]
	if !ok {
		return geoErr("no alias %q", alias)
	}
	a.stopPairing()
	delete(s.aliases, key)
	return s.def.store.Write(ctx, func(tx *store.Tx) error { return tx.DeleteGeoAlias(a.name) })
}

// GeoDRAliases lists the aliases and their state.
func (s *Server) GeoDRAliases() []GeoDRStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []GeoDRStatus
	for _, a := range s.aliases {
		a.mu.Lock()
		st := GeoDRStatus{Alias: a.name, Primary: a.primary.name, Paired: a.secondary != nil, LastSync: a.lastSync}
		if a.secondary != nil {
			st.Secondary = a.secondary.name
		}
		if a.lastErr != nil {
			st.Error = a.lastErr.Error()
		}
		a.mu.Unlock()
		out = append(out, st)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Alias < out[j-1].Alias; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// saveAlias persists an alias in the default namespace's database.
func (s *Server) saveAlias(ctx context.Context, a *geoAlias) error {
	a.mu.Lock()
	row := store.GeoAlias{Alias: a.name, Primary: a.primary.name, Paired: a.secondary != nil}
	if a.secondary != nil {
		row.Secondary = a.secondary.name
	}
	a.mu.Unlock()
	return s.def.store.Write(ctx, func(tx *store.Tx) error { return tx.SaveGeoAlias(row) })
}

// restoreGeoDR loads the aliases a previous run saved, then declares the
// ones the options list that no saved alias already covers: a failover that
// happened is not undone by the configuration that paired them.
func (s *Server) restoreGeoDR(ctx context.Context, declared []GeoDRConfig) error {
	var rows []store.GeoAlias
	err := s.def.store.Read(ctx, func(tx *store.Tx) error {
		var err error
		rows, err = tx.GeoAliases()
		return err
	})
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for _, r := range rows {
		have[strings.ToLower(r.Alias)] = true
		p, ok := s.namespaceNamed(r.Primary)
		if !ok {
			continue // The namespace is no longer declared.
		}
		sec, hasSec := s.namespaceNamed(r.Secondary)
		a := &geoAlias{name: r.Alias, primary: p}
		if s.aliases == nil {
			s.aliases = map[string]*geoAlias{}
		}
		s.aliases[strings.ToLower(r.Alias)] = a
		if r.Paired && hasSec {
			if err := s.startPairing(ctx, a, p, sec); err != nil {
				return err
			}
		}
	}
	for _, g := range declared {
		if have[strings.ToLower(g.Alias)] {
			continue
		}
		s.mu.Unlock()
		err := s.PairGeoDR(ctx, g.Alias, g.Primary, g.Secondary)
		s.mu.Lock()
		if err != nil {
			return err
		}
	}
	return nil
}

// stopGeo stops every replication, for Close.
func (s *Server) stopGeo() {
	for _, a := range s.aliases {
		a.stopPairing()
	}
}

// portalGeo gives a namespace's portal the server's Geo-DR and
// Geo-Replication operations.
type portalGeo struct{ n *Namespace }

func (g portalGeo) Namespaces() []string {
	var out []string
	for _, ns := range g.n.server.Namespaces() {
		out = append(out, ns.name)
	}
	return out
}

func (g portalGeo) Current() string { return g.n.name }

func (g portalGeo) GeoDRAliases() []portal.GeoDRAlias {
	var out []portal.GeoDRAlias
	for _, a := range g.n.server.GeoDRAliases() {
		out = append(out, portal.GeoDRAlias{Alias: a.Alias, Primary: a.Primary, Secondary: a.Secondary, Paired: a.Paired, LastSync: a.LastSync, Error: a.Error})
	}
	return out
}

func (g portalGeo) PairGeoDR(ctx context.Context, alias, primary, secondary string) error {
	return g.n.server.PairGeoDR(ctx, alias, primary, secondary)
}

func (g portalGeo) FailoverGeoDR(ctx context.Context, alias string, force bool) error {
	return g.n.server.FailoverGeoDR(ctx, alias, force)
}

func (g portalGeo) BreakGeoDR(ctx context.Context, alias string) error {
	return g.n.server.BreakGeoDR(ctx, alias)
}

func (g portalGeo) DeleteGeoDRAlias(ctx context.Context, alias string) error {
	return g.n.server.DeleteGeoDRAlias(ctx, alias)
}

func (g portalGeo) GeoReplication() portal.GeoReplicationInfo {
	st := g.n.GeoReplicationStatus()
	return portal.GeoReplicationInfo{
		Enabled: st.Enabled, Synchronous: st.Synchronous, MaxLag: st.MaxLag, Delay: st.Delay,
		PrimaryRegion: st.PrimaryRegion, SecondaryRegion: st.SecondaryRegion, State: st.State,
		LagDuration: st.LagDuration, LagCount: st.LagCount, Promoting: st.Promoting,
		LastPromotion: st.LastPromotion, LostChanges: st.LostChanges,
	}
}

func (g portalGeo) EnableGeoReplication(ctx context.Context, cfg portal.GeoReplicationConfig) error {
	return g.n.EnableGeoReplication(ctx, GeoReplication{
		Synchronous: cfg.Synchronous, MaxLag: cfg.MaxLag, Delay: cfg.Delay,
		PrimaryRegion: cfg.PrimaryRegion, SecondaryRegion: cfg.SecondaryRegion,
	})
}

func (g portalGeo) DisableGeoReplication() error { return g.n.DisableGeoReplication() }

func (g portalGeo) PromoteGeoReplication(ctx context.Context, forced bool) error {
	return g.n.PromoteGeoReplication(ctx, forced)
}
