package broker

import (
	"sync"
	"sync/atomic"
)

// activity holds when entities were last used by operations that do not
// otherwise write: receive and peek. Recording them in the database would
// send every receive, empty ones included, through the single writer.
//
// Nothing here is persisted. A restart counts every entity as used (see
// Open), and a Geo-Replication promotion does the same, so the database
// value only has to be at least as old as the real one: readers take the
// later of the two.
type activity struct {
	last sync.Map // entity id -> *atomic.Int64, Unix milliseconds
	// drained says whether the last receive on an entity's sub-queue found
	// nothing, which is when looking before writing pays.
	drained sync.Map // drainKey -> *atomic.Bool
}

type drainKey struct {
	entity   int64
	subqueue Subqueue
}

// touch records use of e at the given time and, for a subscription, of its
// topic: operations on a subscription keep the topic alive too.
func (a *activity) touch(e *Entity, at int64) {
	a.touchID(e.id, at)
	if e.Topic != nil {
		a.touchID(e.Topic.id, at)
	}
}

func (a *activity) touchID(id, at int64) {
	v, ok := a.last.Load(id)
	if !ok {
		v, _ = a.last.LoadOrStore(id, new(atomic.Int64))
	}
	p := v.(*atomic.Int64)
	for {
		cur := p.Load()
		if cur >= at || p.CompareAndSwap(cur, at) {
			return
		}
	}
}

// latest returns the later of the stored time and the one in memory.
func (a *activity) latest(id, stored int64) int64 {
	if v, ok := a.last.Load(id); ok {
		return max(stored, v.(*atomic.Int64).Load())
	}
	return stored
}

func (a *activity) flag(e *Entity, sq Subqueue) *atomic.Bool {
	k := drainKey{e.id, sq}
	v, ok := a.drained.Load(k)
	if !ok {
		v, _ = a.drained.LoadOrStore(k, new(atomic.Bool))
	}
	return v.(*atomic.Bool)
}

// forget drops what is held for an entity that was deleted.
func (a *activity) forget(id int64) {
	a.last.Delete(id)
	for sq := Subqueue(0); sq <= 2; sq++ {
		a.drained.Delete(drainKey{id, sq})
	}
}
