package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// GeoReplication configures the replication of a namespace's data and
// metadata to a secondary region.
type GeoReplication struct {
	// Synchronous commits every change in both regions before it is
	// acknowledged. Otherwise the secondary follows asynchronously.
	Synchronous bool
	// MaxLag is how far behind the secondary may fall, in the asynchronous
	// mode, before the primary throttles. Zero means one minute.
	MaxLag time.Duration
	// Delay simulates the distance between the regions: a change reaches
	// the secondary this long after it commits, in the asynchronous mode.
	Delay time.Duration
	// PrimaryRegion and SecondaryRegion name the regions; they only label.
	PrimaryRegion, SecondaryRegion string
}

// DefaultMaxReplicationLag is the lag at which an asynchronous primary
// throttles when the configuration says none.
const DefaultMaxReplicationLag = time.Minute

// Secondary region states, as the Azure portal shows them.
const (
	// RegionInBuild is a secondary being filled: it holds no complete copy
	// yet and is not part of the quorum.
	RegionInBuild = "InBuild"
	// RegionReady is a secondary that follows the primary.
	RegionReady = "Ready"
)

// GeoReplicationStatus describes the replication of a namespace.
type GeoReplicationStatus struct {
	Enabled                        bool
	Synchronous                    bool
	MaxLag, Delay                  time.Duration
	PrimaryRegion, SecondaryRegion string
	// State is the secondary's: InBuild or Ready.
	State string
	// LagDuration is the age of the oldest change the secondary has not
	// applied, and LagCount how many such changes there are.
	LagDuration time.Duration
	LagCount    int
	// Promoting is set while a planned promotion waits for the secondary to
	// catch up: the namespace refuses changes meanwhile.
	Promoting     bool
	LastPromotion time.Time
	// LostChanges is how many changes the last forced promotion dropped.
	LostChanges int
}

// ErrGeoReplication wraps the errors of Geo-Replication operations that the
// caller caused.
var ErrGeoReplication = errors.New("geo-replication")

func georepErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrGeoReplication, fmt.Sprintf(format, args...))
}

// pendingChange is a committed change the secondary has not applied.
type pendingChange struct {
	stmts []store.Stmt
	at    time.Time
}

// geoReplication keeps a hot standby copy of a namespace's database. The
// primary's statements are replayed on it, at once in the synchronous mode
// and after the configured delay in the asynchronous one. It is a standby:
// no client reaches it, until a promotion makes it the namespace's store.
type geoReplication struct {
	n *Namespace

	mu      sync.Mutex
	cfg     GeoReplication
	replica *store.Store
	path    string
	state   string
	pending []pendingChange
	// flush makes the worker apply changes without waiting for their
	// delay, for a planned promotion.
	flush         bool
	promoting     bool
	lastPromotion time.Time
	lost          int
	closed        bool

	wake chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

// EnableGeoReplication starts replicating the namespace's data to a
// secondary region: it copies the database to a standby and keeps the copy
// up to date. It cannot be combined with Geo-DR.
func (n *Namespace) EnableGeoReplication(ctx context.Context, cfg GeoReplication) error {
	s := n.server
	s.mu.RLock()
	_, paired := s.pairedLocked(n)
	s.mu.RUnlock()
	if paired {
		return georepErr("namespace %s is in a Geo-DR pairing, which cannot be combined with Geo-Replication", n.name)
	}
	return n.enableGeoReplication(ctx, cfg)
}

func (n *Namespace) enableGeoReplication(ctx context.Context, cfg GeoReplication) error {
	if n.geoRep != nil {
		return georepErr("namespace %s already replicates its data", n.name)
	}
	if cfg.PrimaryRegion == "" {
		cfg.PrimaryRegion = "primary"
	}
	if cfg.SecondaryRegion == "" {
		cfg.SecondaryRegion = "secondary"
	}
	path := store.Memory
	if n.dbPath != store.Memory {
		path = strings.TrimSuffix(n.dbPath, ".db") + ".replica.db"
	}
	replica, err := store.Open(ctx, path)
	if err != nil {
		return err
	}
	g := &geoReplication{n: n, cfg: cfg, replica: replica, path: path, state: RegionInBuild,
		wake: make(chan struct{}, 1), done: make(chan struct{})}
	if err := g.build(ctx, n.store.Get()); err != nil {
		replica.Close()
		return err
	}
	n.geoRep = g
	g.wg.Add(1)
	go g.run()
	return nil
}

// DisableGeoReplication removes the secondary region: its data is deleted.
func (n *Namespace) DisableGeoReplication() error {
	g := n.geoRep
	if g == nil {
		return georepErr("namespace %s does not replicate its data", n.name)
	}
	n.geoRep = nil
	return g.close()
}

// build copies primary to the replica and, in the same instant, starts the
// replication of what primary commits next.
func (g *geoReplication) build(ctx context.Context, primary *store.Store) error {
	g.mu.Lock()
	g.state = RegionInBuild
	g.mu.Unlock()
	err := primary.CopyTo(ctx, g.replica, func() {
		primary.SetReplicator(g.replicate)
		primary.SetGate(g.throttle)
	})
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.state = RegionReady
	g.mu.Unlock()
	return nil
}

// replicate receives the statements of a committed write on the primary.
func (g *geoReplication) replicate(ctx context.Context, stmts []store.Stmt) error {
	g.mu.Lock()
	sync := g.cfg.Synchronous
	if !sync {
		g.pending = append(g.pending, pendingChange{stmts: stmts, at: time.Now()})
		g.mu.Unlock()
		select {
		case g.wake <- struct{}{}:
		default:
		}
		return nil
	}
	replica := g.replica
	g.mu.Unlock()
	// Committed in both regions before the writer is answered.
	return replica.ApplyBatch(ctx, stmts)
}

// throttle refuses writes while a planned promotion waits, and in the
// asynchronous mode when the secondary lags more than allowed.
func (g *geoReplication) throttle() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.promoting {
		return fmt.Errorf("%w: the namespace is being promoted", store.ErrThrottled)
	}
	if lag := g.lagLocked(); !g.cfg.Synchronous && lag > g.maxLagLocked() {
		return fmt.Errorf("%w: the secondary region lags by %v, more than %v", store.ErrThrottled, lag.Round(time.Millisecond), g.maxLagLocked())
	}
	return nil
}

func (g *geoReplication) maxLagLocked() time.Duration {
	if g.cfg.MaxLag > 0 {
		return g.cfg.MaxLag
	}
	return DefaultMaxReplicationLag
}

func (g *geoReplication) lagLocked() time.Duration {
	if len(g.pending) == 0 {
		return 0
	}
	return time.Since(g.pending[0].at)
}

// run applies the pending changes in order, each after the delay.
func (g *geoReplication) run() {
	defer g.wg.Done()
	for {
		g.mu.Lock()
		var next *pendingChange
		var wait time.Duration
		if len(g.pending) > 0 {
			next = &g.pending[0]
			if !g.flush {
				wait = time.Until(next.at.Add(g.cfg.Delay))
			}
		}
		g.mu.Unlock()
		if next != nil && wait <= 0 {
			g.apply()
			continue
		}
		var timer <-chan time.Time
		if next != nil {
			timer = time.After(wait)
		}
		select {
		case <-g.done:
			return
		case <-g.wake:
		case <-timer:
		}
	}
}

// apply replays the oldest pending change on the replica.
func (g *geoReplication) apply() {
	g.mu.Lock()
	if len(g.pending) == 0 {
		g.mu.Unlock()
		return
	}
	ch := g.pending[0]
	replica := g.replica
	g.mu.Unlock()
	err := replica.ApplyBatch(context.Background(), ch.stmts)
	if err != nil {
		g.n.server.logger().Error("replicating to the secondary region", "namespace", g.n.name, "err", err)
	}
	g.mu.Lock()
	// A promotion may have cleared the queue meanwhile.
	if len(g.pending) > 0 && &g.pending[0].stmts[0] == &ch.stmts[0] {
		g.pending = g.pending[1:]
	}
	g.mu.Unlock()
}

// status reports the replication.
func (g *geoReplication) status() GeoReplicationStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	return GeoReplicationStatus{
		Enabled: true, Synchronous: g.cfg.Synchronous, MaxLag: g.cfg.MaxLag, Delay: g.cfg.Delay,
		PrimaryRegion: g.cfg.PrimaryRegion, SecondaryRegion: g.cfg.SecondaryRegion, State: g.state,
		LagDuration: g.lagLocked(), LagCount: len(g.pending), Promoting: g.promoting,
		LastPromotion: g.lastPromotion, LostChanges: g.lost,
	}
}

func (g *geoReplication) close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.mu.Unlock()
	primary := g.n.store.Get()
	primary.SetReplicator(nil)
	primary.SetGate(nil)
	close(g.done)
	g.wg.Wait()
	return g.replica.Close()
}

// GeoReplicationStatus describes the namespace's replication; Enabled is
// false when it has none.
func (n *Namespace) GeoReplicationStatus() GeoReplicationStatus {
	if g := n.geoRep; g != nil {
		return g.status()
	}
	return GeoReplicationStatus{}
}

// PromoteGeoReplication makes the secondary region the primary, and the old
// primary a secondary that is rebuilt from it. A planned promotion puts the
// namespace in read-only mode until the secondary has applied every change,
// so none is lost; a forced one switches at once and drops the changes the
// secondary had not applied. Either way the entities' auto-delete timers
// start over.
func (n *Namespace) PromoteGeoReplication(ctx context.Context, forced bool) error {
	g := n.geoRep
	if g == nil {
		return georepErr("namespace %s does not replicate its data", n.name)
	}
	g.mu.Lock()
	if g.promoting || g.state != RegionReady {
		busy := g.promoting
		g.mu.Unlock()
		if busy {
			return georepErr("a promotion is already running")
		}
		return georepErr("the secondary region is %s, not Ready", RegionInBuild)
	}
	g.promoting = true
	g.flush = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.promoting, g.flush = false, false
		g.mu.Unlock()
	}()
	select {
	case g.wake <- struct{}{}:
	default:
	}

	if forced {
		g.mu.Lock()
		g.lost = len(g.pending)
		g.pending = nil
		g.mu.Unlock()
	} else {
		deadline := time.Now().Add(30 * time.Second)
		for {
			g.mu.Lock()
			left := len(g.pending)
			g.mu.Unlock()
			if left == 0 {
				break
			}
			if time.Now().After(deadline) {
				return georepErr("the secondary region did not catch up in 30s; try a forced promotion")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		}
		g.mu.Lock()
		g.lost = 0
		g.mu.Unlock()
	}

	// Switch: the replica becomes the store, and the old primary, wiped and
	// refilled from it, the replica.
	g.mu.Lock()
	oldPrimary, newPrimary := n.store.Get(), g.replica
	g.mu.Unlock()
	oldPrimary.SetReplicator(nil)
	oldPrimary.SetGate(nil)
	n.store.Set(newPrimary)
	if err := n.broker.SwapStore(ctx, newPrimary); err != nil {
		return err
	}
	if err := n.auth.SwapStore(ctx, newPrimary); err != nil {
		return err
	}
	if err := n.broker.RestartIdleTimers(ctx); err != nil {
		return err
	}
	g.mu.Lock()
	g.replica = oldPrimary
	g.cfg.PrimaryRegion, g.cfg.SecondaryRegion = g.cfg.SecondaryRegion, g.cfg.PrimaryRegion
	g.lastPromotion = time.Now()
	g.promoting, g.flush = false, false
	g.mu.Unlock()
	if err := g.build(ctx, newPrimary); err != nil {
		return georepErr("rebuilding the old primary as the secondary: %v", err)
	}
	return nil
}

// ---- Server-level access ----

// GeoReplicationOf returns the replication status of the named namespace.
func (s *Server) GeoReplicationOf(name string) (GeoReplicationStatus, bool) {
	ns, ok := s.Namespace(name)
	if !ok {
		return GeoReplicationStatus{}, false
	}
	return ns.GeoReplicationStatus(), true
}
