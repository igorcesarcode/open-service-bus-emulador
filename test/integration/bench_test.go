package integration_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// The benchmarks drive a real server through the Go SDK over AMQP, with a
// SQLite file, to find where the time goes:
//
//	go test ./test/integration -run '^$' -bench . -benchtime 5s -cpuprofile cpu.out -memprofile mem.out \
//	  -blockprofile block.out -mutexprofile mutex.out
func benchClient(b *testing.B, opts server.Options) *azservicebus.Client {
	b.Helper()
	opts.Addr = "127.0.0.1:0"
	opts.DBPath = filepath.Join(b.TempDir(), "bench.db")
	opts.Queues = append(opts.Queues, server.Queue{Name: "bench"})
	srv, err := server.New(context.Background(), opts)
	if err != nil {
		b.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { srv.Close() })
	client, err := azservicebus.NewClientFromConnectionString(srv.ConnectionString(), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { client.Close(context.Background()) })
	return client
}

func BenchmarkSendSingle(b *testing.B) {
	client := benchClient(b, server.Options{})
	body := make([]byte, 1024)
	b.SetBytes(int64(len(body)))
	b.RunParallel(func(pb *testing.PB) {
		sender, err := client.NewSender("bench", nil)
		if err != nil {
			b.Error(err)
			return
		}
		defer sender.Close(context.Background())
		for pb.Next() {
			if err := sender.SendMessage(context.Background(), &azservicebus.Message{Body: body}, nil); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkSendBatch(b *testing.B) {
	client := benchClient(b, server.Options{})
	sender, err := client.NewSender("bench", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer sender.Close(context.Background())
	body := make([]byte, 1024)
	const perBatch = 100
	b.SetBytes(int64(len(body)) * perBatch)
	for b.Loop() {
		batch, err := sender.NewMessageBatch(context.Background(), nil)
		if err != nil {
			b.Fatal(err)
		}
		for range perBatch {
			if err := batch.AddMessage(&azservicebus.Message{Body: body}, nil); err != nil {
				b.Fatal(err)
			}
		}
		if err := sender.SendMessageBatch(context.Background(), batch, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRoundTrip sends 100 messages in a batch, then receives and
// completes them one by one.
func BenchmarkRoundTrip(b *testing.B) {
	client := benchClient(b, server.Options{})
	sender, err := client.NewSender("bench", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer sender.Close(context.Background())
	receiver, err := client.NewReceiverForQueue("bench", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer receiver.Close(context.Background())
	ctx := context.Background()
	body := make([]byte, 1024)
	const perBatch = 100
	for b.Loop() {
		batch, _ := sender.NewMessageBatch(ctx, nil)
		for range perBatch {
			batch.AddMessage(&azservicebus.Message{Body: body}, nil)
		}
		if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
			b.Fatal(err)
		}
		for got := 0; got < perBatch; {
			msgs, err := receiver.ReceiveMessages(ctx, perBatch-got, nil)
			if err != nil {
				b.Fatal(err)
			}
			for _, m := range msgs {
				if err := receiver.CompleteMessage(ctx, m, nil); err != nil {
					b.Fatal(err)
				}
			}
			got += len(msgs)
		}
	}
}

// BenchmarkConcurrentRoundTrip has every goroutine send a message to the
// queue, receive one and complete it, so that settlements overlap.
func BenchmarkConcurrentRoundTrip(b *testing.B) {
	client := benchClient(b, server.Options{})
	body := make([]byte, 1024)
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		sender, err := client.NewSender("bench", nil)
		if err != nil {
			b.Error(err)
			return
		}
		defer sender.Close(ctx)
		receiver, err := client.NewReceiverForQueue("bench", nil)
		if err != nil {
			b.Error(err)
			return
		}
		defer receiver.Close(ctx)
		for pb.Next() {
			if err := sender.SendMessage(ctx, &azservicebus.Message{Body: body}, nil); err != nil {
				b.Error(err)
				return
			}
			msgs, err := receiver.ReceiveMessages(ctx, 1, nil)
			if err != nil {
				b.Error(err)
				return
			}
			for _, m := range msgs {
				if err := receiver.CompleteMessage(ctx, m, nil); err != nil {
					b.Error(err)
					return
				}
			}
		}
	})
}
