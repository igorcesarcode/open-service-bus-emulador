// Command go-sdk shows the official Go SDK talking to open-service-bus:
// a queue round trip, topic routing with subscription rules, dead-lettering
// and the administration client.
//
// Start the emulator first, with the entities of examples/config-json:
//
//	open-service-bus -config examples/config-json/Config.json
//
// then run:
//
//	go run ./examples/go-sdk
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

// defaultConnectionString is what the emulator prints at startup.
const defaultConnectionString = "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true"

func main() {
	connectionString := os.Getenv("OSB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = defaultConnectionString
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	if err := queueRoundTrip(ctx, client); err != nil {
		log.Fatal(err)
	}
	if err := topicRouting(ctx, client); err != nil {
		log.Fatal(err)
	}
	if err := deadLetter(ctx, client); err != nil {
		log.Fatal(err)
	}
	if err := administration(ctx, connectionString); err != nil {
		log.Fatal(err)
	}
}

// queueRoundTrip sends one message to a queue and receives it.
func queueRoundTrip(ctx context.Context, client *azservicebus.Client) error {
	sender, err := client.NewSender("orders", nil)
	if err != nil {
		return err
	}
	defer sender.Close(ctx)
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte(`{"id": 1}`)}, nil); err != nil {
		return err
	}

	receiver, err := client.NewReceiverForQueue("orders", nil)
	if err != nil {
		return err
	}
	defer receiver.Close(ctx)
	msgs, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		return err
	}
	fmt.Printf("queue orders: received %s\n", msgs[0].Body)
	return receiver.CompleteMessage(ctx, msgs[0], nil)
}

// topicRouting publishes to a topic; each subscription gets the messages its
// rule accepts.
func topicRouting(ctx context.Context, client *azservicebus.Client) error {
	sender, err := client.NewSender("events", nil)
	if err != nil {
		return err
	}
	defer sender.Close(ctx)
	for _, quantity := range []int{3, 50} {
		msg := &azservicebus.Message{
			Body:                  []byte(fmt.Sprintf("order of %d", quantity)),
			ApplicationProperties: map[string]any{"quantity": int64(quantity)},
		}
		if err := sender.SendMessage(ctx, msg, nil); err != nil {
			return err
		}
	}

	// The "big-orders" subscription has the rule "quantity > 10".
	receiver, err := client.NewReceiverForSubscription("events", "big-orders", nil)
	if err != nil {
		return err
	}
	defer receiver.Close(ctx)
	msgs, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		return err
	}
	fmt.Printf("events/big-orders: received %q\n", msgs[0].Body)
	return receiver.CompleteMessage(ctx, msgs[0], nil)
}

// deadLetter dead-letters a message and reads it from the dead-letter queue.
// The queue "jobs" forwards dead-lettered messages to "jobs-poison".
func deadLetter(ctx context.Context, client *azservicebus.Client) error {
	sender, err := client.NewSender("jobs", nil)
	if err != nil {
		return err
	}
	defer sender.Close(ctx)
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("bad job")}, nil); err != nil {
		return err
	}

	receiver, err := client.NewReceiverForQueue("jobs", nil)
	if err != nil {
		return err
	}
	defer receiver.Close(ctx)
	msgs, err := receiver.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		return err
	}
	reason, description := "unparseable", "the job could not be parsed"
	err = receiver.DeadLetterMessage(ctx, msgs[0], &azservicebus.DeadLetterOptions{
		Reason:           &reason,
		ErrorDescription: &description,
	})
	if err != nil {
		return err
	}

	poison, err := client.NewReceiverForQueue("jobs-poison", nil)
	if err != nil {
		return err
	}
	defer poison.Close(ctx)
	forwarded, err := poison.ReceiveMessages(ctx, 1, nil)
	if err != nil {
		return err
	}
	fmt.Printf("queue jobs-poison: received %q\n", forwarded[0].Body)
	return poison.CompleteMessage(ctx, forwarded[0], nil)
}

// administration creates a queue with the administration client. The
// emulator accepts any value, for example a lock duration of five seconds,
// which Azure would not allow.
//
// The Go administration client always uses HTTPS, and the emulator's
// certificate is self-signed, so the example skips its verification. Use
// -tls-cert-out and trust the written certificate instead if you prefer.
func administration(ctx context.Context, connectionString string) error {
	transport := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	client, err := admin.NewClientFromConnectionString(connectionString, &admin.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: transport},
	})
	if err != nil {
		return err
	}
	lock := "PT5S"
	maxDelivery := int32(1)
	_, err = client.CreateQueue(ctx, "go-sdk-demo", &admin.CreateQueueOptions{
		Properties: &admin.QueueProperties{LockDuration: &lock, MaxDeliveryCount: &maxDelivery},
	})
	if err != nil {
		return err
	}
	props, err := client.GetQueue(ctx, "go-sdk-demo", nil)
	if err != nil {
		return err
	}
	fmt.Printf("admin: created queue go-sdk-demo, lock duration %s\n", *props.LockDuration)
	_, err = client.DeleteQueue(ctx, "go-sdk-demo", nil)
	return err
}
