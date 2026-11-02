package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func partitionedQueue(t *testing.T, b *Broker, name string, edit func(*Properties)) {
	t.Helper()
	newQueue(t, b, name, func(p *Properties) {
		p.EnablePartitioning = true
		if edit != nil {
			edit(p)
		}
	})
}

func TestPartitionedSendValidation(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	partitionedQueue(t, b, "p", func(p *Properties) { p.RequiresSession = false })
	newQueue(t, b, "plain", nil)

	msg := func(session, key string) IncomingMessage {
		m := testMsg{body: "x", id: session + key, session: session}.incoming(t)
		m.PartitionKey = key
		return m
	}
	// A session id and a different partition key are refused on a
	// partitioned entity, and ignored on a plain one.
	if _, err := b.Send(ctx, "p", []IncomingMessage{msg("s1", "other")}); !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("session s1 with key other on a partitioned queue = %v, want ErrInvalidOperation", err)
	}
	if _, err := b.Send(ctx, "plain", []IncomingMessage{msg("s1", "other")}); err != nil {
		t.Errorf("the same message to a plain queue: %v", err)
	}
	// Equal values, a key alone and a session alone are fine.
	for _, m := range []IncomingMessage{msg("s1", "s1"), msg("", "k1"), msg("s2", ""), msg("", "")} {
		if _, err := b.Send(ctx, "p", []IncomingMessage{m}); err != nil {
			t.Errorf("Send(session %q, key %q) = %v", m.SessionID, m.PartitionKey, err)
		}
	}
	// A batch must share its key.
	if _, err := b.Send(ctx, "p", []IncomingMessage{msg("", "k1"), msg("", "k2")}); !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("a batch with two keys = %v, want ErrInvalidOperation", err)
	}
	if _, err := b.Send(ctx, "p", []IncomingMessage{msg("", "k1"), msg("", "")}); !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("a batch with a key and a message without = %v, want ErrInvalidOperation", err)
	}
	if _, err := b.Send(ctx, "p", []IncomingMessage{msg("", "k1"), msg("", "k1")}); err != nil {
		t.Errorf("a batch with one key = %v", err)
	}
	if _, err := b.Send(ctx, "plain", []IncomingMessage{msg("", "k1"), msg("", "k2")}); err != nil {
		t.Errorf("a batch with two keys to a plain queue = %v", err)
	}
}

func TestTransactionKey(t *testing.T) {
	dedup := &Entity{Properties: Properties{EnablePartitioning: true, RequiresDuplicateDetection: true}}
	plain := &Entity{Properties: Properties{EnablePartitioning: true}}
	m := func(id, session, key string) IncomingMessage {
		return IncomingMessage{MessageID: id, SessionID: session, PartitionKey: key}
	}
	for _, tc := range []struct {
		name    string
		e       *Entity
		msgs    []IncomingMessage
		current string
		want    string
		wantErr bool
	}{
		{"session", plain, []IncomingMessage{m("1", "s", "")}, "", "s", false},
		{"partition key", plain, []IncomingMessage{m("1", "", "k"), m("2", "", "k")}, "", "k", false},
		{"no key", plain, []IncomingMessage{m("1", "", "")}, "", "", true},
		{"message id is a key only with duplicate detection", dedup, []IncomingMessage{m("id-1", "", "")}, "", "id-1", false},
		{"two message ids differ", dedup, []IncomingMessage{m("a", "", ""), m("b", "", "")}, "", "a", true},
		{"two keys", plain, []IncomingMessage{m("1", "", "k"), m("2", "", "j")}, "", "k", true},
		{"a key that differs from the transaction's", plain, []IncomingMessage{m("1", "", "j")}, "k", "k", true},
		{"the transaction's key", plain, []IncomingMessage{m("1", "", "k")}, "k", "k", false},
		{"session and a different key", plain, []IncomingMessage{m("1", "s", "k")}, "", "", true},
		{"not partitioned", &Entity{}, []IncomingMessage{m("1", "", "")}, "", "", false},
	} {
		got, err := ValidateTransactionSend(tc.e, tc.msgs, tc.current)
		if (err != nil) != tc.wantErr || (!tc.wantErr && got != tc.want) {
			t.Errorf("%s: key %q, error %v; want key %q, error %v", tc.name, got, err, tc.want, tc.wantErr)
		}
		if err != nil && !errors.Is(err, ErrInvalidOperation) {
			t.Errorf("%s: error %v is not ErrInvalidOperation", tc.name, err)
		}
	}
}

func TestPartitionedQuotaIsMultiplied(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	partitionedQueue(t, b, "p", func(pr *Properties) { pr.MaxSizeInMegabytes = 1 })
	newQueue(t, b, "plain", func(pr *Properties) { pr.MaxSizeInMegabytes = 1 })
	pe, _ := b.Entity("p")
	if got, want := b.MaxSizeBytes(pe), int64(16<<20); got != want {
		t.Fatalf("MaxSizeBytes(partitioned) = %d, want %d", got, want)
	}
	big := make([]byte, 300<<10)
	body := string(big)
	for i := 0; i < 8; i++ {
		// 8 × 300 KB is over the 1 MB of a plain queue and well under 16 MB.
		if _, err := b.Send(ctx, "p", []IncomingMessage{testMsg{body: body, id: string(rune('a' + i))}.incoming(t)}); err != nil {
			t.Fatalf("send %d to the partitioned queue: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		_, err := b.Send(ctx, "plain", []IncomingMessage{testMsg{body: body, id: string(rune('a' + i))}.incoming(t)})
		if i >= 3 && !errors.Is(err, ErrQuotaExceeded) {
			t.Errorf("send %d to the plain queue = %v, want ErrQuotaExceeded", i, err)
		}
	}
	if b.Partitions() != DefaultPartitions {
		t.Errorf("Partitions() = %d, want %d", b.Partitions(), DefaultPartitions)
	}
}

func TestExpressWithDuplicateDetectionIsRefused(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	props := DefaultProperties(KindQueue)
	props.EnableExpress, props.RequiresDuplicateDetection = true, true
	_, err := b.CreateQueue(ctx, "x", props)
	if !errors.Is(err, ErrInvalidOperation) || !strings.Contains(err.Error(), "Both DelayedPersistence and RequiresDuplicateDetection property can't be enabled together.") {
		t.Errorf("CreateQueue(express and duplicate detection) error = %v", err)
	}
	props.RequiresDuplicateDetection = false
	if _, err := b.CreateQueue(ctx, "x", props); err != nil {
		t.Errorf("CreateQueue(express) error: %v", err)
	}
}

func TestPartitionedSequenceNumbersCarryThePartition(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	partitionedQueue(t, b, "p", nil)
	var parts []int64
	for i := 0; i < 16; i++ {
		seqs, err := b.Send(ctx, "p", []IncomingMessage{testMsg{body: "x", id: fmt.Sprint(i)}.incoming(t)})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, seqs[0]>>partitionShift)
	}
	seen := map[int64]bool{}
	for _, p := range parts {
		seen[p] = true
	}
	if len(seen) != 16 {
		t.Errorf("16 sends without a key reached %d partitions (%v)", len(seen), parts)
	}
	if internalSeq(publicSeqOf(5, 77)) != 77 || publicSeqOf(5, 77)>>partitionShift != 5 {
		t.Error("publicSeqOf and internalSeq do not invert each other")
	}
}
