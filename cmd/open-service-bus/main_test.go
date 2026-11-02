package main

import (
	"flag"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func TestAddEntities(t *testing.T) {
	session := broker.DefaultProperties(broker.KindQueue)
	session.RequiresSession = true
	declared := func() server.Options {
		return server.Options{
			Queues: []server.Queue{{Name: "replies", Properties: session}},
			Topics: []server.Topic{{
				Name:          "orders",
				Subscriptions: []server.Subscription{{Name: "audit", Properties: session}},
			}},
		}
	}
	tests := []struct {
		name                 string
		queues, topics, subs []string
		want                 server.Options
	}{
		{
			name:   "flags keep declared properties",
			queues: []string{"Replies", "results", "results"},
			topics: []string{"orders"},
			subs:   []string{"ORDERS/audit", "orders/billing", "events/all"},
			want: server.Options{
				Queues: []server.Queue{{Name: "replies", Properties: session}, {Name: "results"}},
				Topics: []server.Topic{
					{Name: "orders", Subscriptions: []server.Subscription{{Name: "audit", Properties: session}, {Name: "billing"}}},
					{Name: "events", Subscriptions: []server.Subscription{{Name: "all"}}},
				},
			},
		},
		{
			name: "no flags",
			want: declared(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := declared()
			if err := addEntities(&opts, tt.queues, tt.topics, tt.subs); err != nil {
				t.Fatalf("addEntities() error: %v", err)
			}
			if diff := cmp.Diff(tt.want, opts); diff != "" {
				t.Errorf("addEntities() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAddEntitiesInvalidSubscription(t *testing.T) {
	for _, s := range []string{"orders", "/audit", "orders/"} {
		var opts server.Options
		if err := addEntities(&opts, nil, nil, []string{s}); err == nil {
			t.Errorf("addEntities(-subscription %q) succeeded, want error", s)
		}
	}
}

func TestApplyEnv(t *testing.T) {
	newSet := func() (*flag.FlagSet, *string, *int, *bool, *[]string) {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		db := fs.String("db", "", "")
		partitions := fs.Int("partitions", 16, "")
		portal := fs.Bool("portal", true, "")
		var queues []string
		fs.Func("queue", "", appendNames(&queues))
		fs.Bool("mcp-readonly", false, "")
		return fs, db, partitions, portal, &queues
	}

	t.Run("fills the flags left out", func(t *testing.T) {
		t.Setenv("OSB_DB", "/data/osb.db")
		t.Setenv("OSB_PARTITIONS", "4")
		t.Setenv("OSB_PORTAL", "false")
		t.Setenv("OSB_QUEUE", "orders, payments")
		fs, db, partitions, portal, queues := newSet()
		if err := fs.Parse(nil); err != nil {
			t.Fatal(err)
		}
		if err := applyEnv(fs); err != nil {
			t.Fatal(err)
		}
		if *db != "/data/osb.db" || *partitions != 4 || *portal || !cmp.Equal(*queues, []string{"orders", "payments"}) {
			t.Errorf("db=%q partitions=%d portal=%v queues=%v", *db, *partitions, *portal, *queues)
		}
	})

	t.Run("a flag beats its variable", func(t *testing.T) {
		t.Setenv("OSB_DB", "/from/env.db")
		fs, db, _, _, _ := newSet()
		if err := fs.Parse([]string{"-db", "/from/flag.db"}); err != nil {
			t.Fatal(err)
		}
		if err := applyEnv(fs); err != nil {
			t.Fatal(err)
		}
		if *db != "/from/flag.db" {
			t.Errorf("db = %q, want the flag's", *db)
		}
	})

	t.Run("a bad value names the variable", func(t *testing.T) {
		t.Setenv("OSB_PARTITIONS", "many")
		fs, _, _, _, _ := newSet()
		fs.Parse(nil)
		err := applyEnv(fs)
		if err == nil || !strings.Contains(err.Error(), "OSB_PARTITIONS") {
			t.Errorf("applyEnv() error = %v, want one naming OSB_PARTITIONS", err)
		}
	})

	t.Run("mcp flags keep their own handling", func(t *testing.T) {
		t.Setenv("OSB_MCP_READONLY", "true")
		fs, _, _, _, _ := newSet()
		fs.Parse(nil)
		if err := applyEnv(fs); err != nil {
			t.Fatal(err)
		}
		if fs.Lookup("mcp-readonly").Value.String() != "false" {
			t.Error("applyEnv set an mcp flag")
		}
	})
}
