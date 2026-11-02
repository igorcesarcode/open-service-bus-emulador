// Command multi-namespace shows several namespaces in one emulator: the
// same queue name in two namespaces holds different messages.
//
//	open-service-bus -config examples/multi-namespace/Config.json
//	go run ./examples/multi-namespace
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func connectionString(namespace string) string {
	return fmt.Sprintf("Endpoint=sb://%s.localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true", namespace)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, namespace := range []string{"orders", "billing"} {
		// *.localhost names may not resolve on every system, so the
		// connection goes to localhost while the namespace name is kept in
		// the endpoint, which is what selects the namespace.
		client, err := azservicebus.NewClientFromConnectionString(connectionString(namespace),
			&azservicebus.ClientOptions{CustomEndpoint: "localhost:5672"})
		if err != nil {
			log.Fatal(err)
		}
		sender, err := client.NewSender("incoming", nil)
		if err != nil {
			log.Fatal(err)
		}
		if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("hello " + namespace)}, nil); err != nil {
			log.Fatal(err)
		}
		receiver, err := client.NewReceiverForQueue("incoming", nil)
		if err != nil {
			log.Fatal(err)
		}
		msgs, err := receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("namespace %s, queue incoming: %s\n", namespace, msgs[0].Body)
		receiver.CompleteMessage(ctx, msgs[0], nil)
		receiver.Close(ctx)
		sender.Close(ctx)
		client.Close(ctx)
	}
}
