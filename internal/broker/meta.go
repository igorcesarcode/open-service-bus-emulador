package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// LockLevel is the kind of an Azure resource lock.
type LockLevel string

// The lock levels of azurerm_management_lock.
const (
	// LockCanNotDelete lets entities be read and changed, not deleted.
	LockCanNotDelete LockLevel = "CanNotDelete"
	// LockReadOnly lets entities be read, not changed or deleted.
	LockReadOnly LockLevel = "ReadOnly"
)

// Lock is a resource lock. Like the Azure ones, it is a management-plane
// protection: it stops changes of entities and rules, never the sending
// and receiving of messages.
type Lock struct {
	Name  string    `json:"name"`
	Level LockLevel `json:"level"`
	// Scope is the path of the entity the lock covers, with the
	// subscriptions and rules below it; empty covers the whole namespace.
	Scope string `json:"scope,omitempty"`
	Notes string `json:"notes,omitempty"`
}

// Encryption is the encryption configuration of a namespace. The emulator
// keeps it for display and for the export only: its SQLite file is never
// encrypted.
type Encryption struct {
	// KeyVaultKeyIDs are the customer-managed keys.
	KeyVaultKeyIDs []string `json:"keyVaultKeyIds,omitempty"`
	// Identity is the identity that reads the keys.
	Identity string `json:"identity,omitempty"`
	// InfrastructureEncryption is the second, service-level layer.
	InfrastructureEncryption bool `json:"infrastructureEncryption,omitempty"`
}

// Meta is what a namespace has besides its entities.
type Meta struct {
	Tags       map[string]string `json:"tags,omitempty"`
	Locks      []Lock            `json:"locks,omitempty"`
	Encryption *Encryption       `json:"encryption,omitempty"`
}

// ErrLocked reports a change that a resource lock forbids.
var ErrLocked = errors.New("ScopeLocked")

const metaKey = "namespace"

func (b *Broker) loadMeta(ctx context.Context) error {
	var raw string
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		var err error
		raw, err = tx.Setting(metaKey)
		return err
	})
	if err != nil || raw == "" {
		return err
	}
	var m Meta
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return fmt.Errorf("namespace settings: %w", err)
	}
	b.metaMu.Lock()
	b.meta = m
	b.metaMu.Unlock()
	return nil
}

// Meta returns a copy of the namespace's tags, locks and encryption.
func (b *Broker) Meta() Meta {
	b.metaMu.RLock()
	defer b.metaMu.RUnlock()
	return cloneMeta(b.meta)
}

func cloneMeta(m Meta) Meta {
	out := Meta{Locks: append([]Lock(nil), m.Locks...)}
	if m.Tags != nil {
		out.Tags = make(map[string]string, len(m.Tags))
		for k, v := range m.Tags {
			out.Tags[k] = v
		}
	}
	if m.Encryption != nil {
		e := *m.Encryption
		e.KeyVaultKeyIDs = append([]string(nil), e.KeyVaultKeyIDs...)
		out.Encryption = &e
	}
	return out
}

// updateMeta applies fn to a copy of the settings, saves it and logs op.
func (b *Broker) updateMeta(ctx context.Context, op, subject string, fn func(*Meta) error) error {
	b.metaMu.Lock()
	defer b.metaMu.Unlock()
	next := cloneMeta(b.meta)
	err := fn(&next)
	if err == nil {
		sort.Slice(next.Locks, func(i, j int) bool { return next.Locks[i].Name < next.Locks[j].Name })
		var raw []byte
		if raw, err = json.Marshal(next); err == nil {
			if len(next.Tags) == 0 && len(next.Locks) == 0 && next.Encryption == nil {
				raw = nil
			}
			err = b.store.Write(ctx, func(tx *store.Tx) error { return tx.SetSetting(metaKey, string(raw)) })
		}
	}
	if err == nil {
		b.meta = next
	}
	b.audit(ctx, op, subject, err, "")
	return err
}

// SetTags replaces the namespace's tags. They have no effect on the data.
func (b *Broker) SetTags(ctx context.Context, tags map[string]string) error {
	return b.updateMeta(ctx, "UpdateNamespaceTags", "", func(m *Meta) error {
		for k := range tags {
			if k == "" {
				return fmt.Errorf("%w: a tag needs a name", ErrInvalidOperation)
			}
		}
		m.Tags = nil
		if len(tags) > 0 {
			m.Tags = make(map[string]string, len(tags))
			for k, v := range tags {
				m.Tags[k] = v
			}
		}
		return nil
	})
}

// SetEncryption sets, or with nil clears, the encryption configuration.
func (b *Broker) SetEncryption(ctx context.Context, e *Encryption) error {
	return b.updateMeta(ctx, "UpdateNamespaceEncryption", "", func(m *Meta) error {
		m.Encryption = nil
		if e != nil {
			c := *e
			c.KeyVaultKeyIDs = append([]string(nil), e.KeyVaultKeyIDs...)
			m.Encryption = &c
		}
		return nil
	})
}

// SetLock creates the lock, or replaces the one with the same name.
func (b *Broker) SetLock(ctx context.Context, l Lock) error {
	l.Scope = NormalizePath(l.Scope)
	return b.updateMeta(ctx, "CreateLock", l.Scope, func(m *Meta) error {
		if l.Name == "" {
			return fmt.Errorf("%w: a lock needs a name", ErrInvalidOperation)
		}
		if l.Level != LockCanNotDelete && l.Level != LockReadOnly {
			return fmt.Errorf("%w: lock level %q must be CanNotDelete or ReadOnly", ErrInvalidOperation, l.Level)
		}
		for i, cur := range m.Locks {
			if cur.Name == l.Name {
				m.Locks[i] = l
				return nil
			}
		}
		m.Locks = append(m.Locks, l)
		return nil
	})
}

// DeleteLock removes the lock with the given name.
func (b *Broker) DeleteLock(ctx context.Context, name string) error {
	return b.updateMeta(ctx, "DeleteLock", "", func(m *Meta) error {
		for i, cur := range m.Locks {
			if cur.Name == name {
				m.Locks = append(m.Locks[:i], m.Locks[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("%w: lock %s", ErrEntityNotFound, name)
	})
}

// lockOp is the kind of change checkLocks vets.
type lockOp string

const (
	opWrite  lockOp = "write"
	opDelete lockOp = "delete"
)

// checkLocks returns ErrLocked when a lock forbids op on the entity at
// path ("" is the namespace). A ReadOnly lock forbids every change of what
// it covers, a CanNotDelete one the deletes. A delete also needs that no
// lock below the path forbids deleting it. Startup configuration and the
// replication of a primary's metadata are not vetted.
func (b *Broker) checkLocks(ctx context.Context, op lockOp, path string) error {
	if actorOf(ctx) == "config" || ctx.Value(replicationKey{}) != nil {
		return nil
	}
	path = NormalizePath(path)
	b.metaMu.RLock()
	locks := b.meta.Locks
	b.metaMu.RUnlock()
	var held []string
	for _, l := range locks {
		covers := lockCovers(l.Scope, path)
		below := op == opDelete && path != "" && lockCovers(path, l.Scope) && l.Scope != path
		if (covers && (l.Level == LockReadOnly || op == opDelete)) || below {
			held = append(held, lockScope(l.Scope))
		}
	}
	if len(held) == 0 {
		return nil
	}
	return fmt.Errorf("%w: The scope '%s' cannot perform %s operation because following scope(s) are locked: '%s'. Please remove the lock and try again.",
		ErrLocked, lockScope(path), op, strings.Join(held, "','"))
}

// lockCovers reports whether a lock at scope covers path: they are the
// same, or scope is the namespace or an ancestor.
func lockCovers(scope, path string) bool {
	return scope == "" || scope == path || strings.HasPrefix(path, scope+"/")
}

func lockScope(p string) string {
	if p == "" {
		return "namespace"
	}
	return p
}

// lockedAgainstDelete reports whether a lock stops the deletion of the
// entity at path, as the auto-delete on idle checks.
func (b *Broker) lockedAgainstDelete(path string) bool {
	return b.checkLocks(context.Background(), opDelete, path) != nil
}
