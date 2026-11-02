package broker

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// DefaultPartitions is how many partitions a partitioned queue or topic has
// on the Standard tier.
const DefaultPartitions = 16

// Partitions returns how many partitions a partitioned entity has.
func (b *Broker) Partitions() int { return b.partitions }

// MaxSizeBytes returns the size quota of e. A partitioned entity has the
// size of its MaxSizeInMegabytes in each of its partitions, so its quota is
// that many times larger, as in Service Bus (a 5 GB partitioned queue holds
// 80 GB).
func (b *Broker) MaxSizeBytes(e *Entity) int64 {
	limit := e.Properties.MaxSizeInMegabytes << 20
	if e.Properties.EnablePartitioning {
		limit *= int64(b.partitions)
	}
	return limit
}

// partitionKey returns the explicit partition key of m: its session id, or
// else its PartitionKey property. These are the keys Service Bus pins a
// message to a partition with.
func (m IncomingMessage) partitionKey() string {
	if m.SessionID != "" {
		return m.SessionID
	}
	return m.PartitionKey
}

// TransactionKey returns the partition key a message sent in a transaction
// to e is pinned with, which is the session id, the PartitionKey property,
// or the message id on an entity with duplicate detection. A message with
// none of them has no key and cannot be sent in a transaction to a
// partitioned entity.
func TransactionKey(e *Entity, m IncomingMessage) string {
	if k := m.partitionKey(); k != "" {
		return k
	}
	if e.Properties.RequiresDuplicateDetection {
		return m.MessageID
	}
	return ""
}

// validatePartitioned applies the rules Service Bus has for sending to a
// partitioned entity: a message cannot have a session id and a different
// partition key, and the messages of one batch must share their key.
func validatePartitioned(e *Entity, msgs []IncomingMessage) error {
	if !e.Properties.EnablePartitioning {
		return nil
	}
	for _, m := range msgs {
		if m.SessionID != "" && m.PartitionKey != "" && m.SessionID != m.PartitionKey {
			return fmt.Errorf("%w: the partition key %q of a message to %s must equal its session id %q", ErrInvalidOperation, m.PartitionKey, e.DisplayPath(), m.SessionID)
		}
	}
	if len(msgs) > 1 {
		first := msgs[0].partitionKey()
		for _, m := range msgs[1:] {
			if m.partitionKey() != first {
				return fmt.Errorf("%w: the messages of a batch to the partitioned %s must have the same partition key", ErrInvalidOperation, e.DisplayPath())
			}
		}
	}
	return nil
}

// ValidateTransactionSend checks a send inside a transaction to e, given the
// key the transaction already uses (empty for none yet). It returns the key
// the transaction is pinned to. Messages sent in a transaction to a
// partitioned entity must have a key, and all of them the same one.
func ValidateTransactionSend(e *Entity, msgs []IncomingMessage, current string) (string, error) {
	if !e.Properties.EnablePartitioning {
		return current, nil
	}
	if err := validatePartitioned(e, msgs); err != nil {
		return current, err
	}
	for _, m := range msgs {
		key := TransactionKey(e, m)
		switch {
		case key == "":
			return current, fmt.Errorf("%w: a message sent in a transaction to the partitioned %s must have a session id, a partition key or, with duplicate detection, a message id", ErrInvalidOperation, e.DisplayPath())
		case current != "" && key != current:
			return current, fmt.Errorf("%w: the messages of a transaction to the partitioned %s must have the same partition key; got %q and %q", ErrInvalidOperation, e.DisplayPath(), current, key)
		}
		current = key
	}
	return current, nil
}

// A message of a partitioned entity has the id of its partition in the high
// bits of the sequence number clients see, as in Service Bus, where "the
// 16 most significant bits are the partition id". The number stored and
// used for ordering is the arrival counter of the entity in the low 48 bits,
// so delivery and peek paging keep their order: only the number shown to
// clients, and taken back from them, carries the partition.
const (
	partitionShift = 48
	seqMask        = 1<<partitionShift - 1
)

// publicSeqOf returns the sequence number clients see.
func publicSeqOf(partition int, seq int64) int64 {
	return int64(partition)<<partitionShift | seq
}

func (b *Broker) publicSeq(partition int, seq int64) int64 { return publicSeqOf(partition, seq) }

// internalSeq returns the stored sequence number of one a client sent back.
func internalSeq(public int64) int64 {
	if public < 0 {
		return public
	}
	return public & seqMask
}

func internalSeqs(public []int64) []int64 {
	out := make([]int64, len(public))
	for i, s := range public {
		out[i] = internalSeq(s)
	}
	return out
}

// partitionFor returns the partition of message m, stored under seq, of the
// entity via, the queue or the topic a subscription belongs to. The key is
// the session id, else the partition key, else the message id of an entity
// with duplicate detection; without a key the partitions take turns.
func (b *Broker) partitionFor(via *Entity, m *outgoing, seq int64) int {
	if via == nil || !via.Properties.EnablePartitioning {
		return 0
	}
	n := b.partitions
	key := m.SessionID
	if key == "" {
		key = m.PartitionKey
	}
	if key == "" && via.Properties.RequiresDuplicateDetection {
		key = m.MessageID
	}
	if key == "" {
		return int(seq % int64(n))
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// batching returns ctx for the writes of an operation on e: when e has
// batched operations disabled, they commit alone instead of with the
// operations of other callers. (A subscription follows its own setting.)
func (b *Broker) batching(ctx context.Context, e *Entity) context.Context {
	if e != nil && !e.Properties.EnableBatchedOperations {
		return store.WithSolo(ctx)
	}
	return ctx
}
