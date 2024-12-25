package main

import (
	"bufio"
	"context"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"io"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	b := BlobStorageClient{}
	b.CreateBlobClient("REPLACE_WITH_AZURE_BLOB_STORAGE_ACCOUNT_URL")
}

/*
AZURE EVENT HUB PRODUCER
*/
func createEventHub(conn string) *eventhub.Hub {
	client, err := eventhub.NewHubFromConnectionString(conn)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func sendMessage(hub *eventhub.Hub, msg string) bool {
	ctx := context.Background()
	if err := hub.Send(ctx, eventhub.NewEventFromString(msg)); err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func sendMessageBatch(hub *eventhub.Hub, msgs []string) bool {
	ctx := context.Background()
	var events []*eventhub.Event
	for _, msg := range msgs {
		events = append(events, eventhub.NewEventFromString(msg))
	}

	if err := hub.SendBatch(ctx, eventhub.NewEventBatchIterator(events...)); err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func sendMessageWithTimeout(hub *eventhub.Hub, msg string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := hub.Send(ctx, eventhub.NewEventFromString(msg)); err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func sendMessageBatchWithTimeout(hub *eventhub.Hub, msgs []string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var events []*eventhub.Event
	for _, msg := range msgs {
		events = append(events, eventhub.NewEventFromString(msg))
	}

	if err := hub.SendBatch(ctx, eventhub.NewEventBatchIterator(events...)); err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func receiveMessages(hub *eventhub.Hub, handle func(c context.Context, e *eventhub.Event) error) {
	ctx := context.Background()
	info, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		log.Fatal(err)
		return
	}

	for _, id := range info.PartitionIDs {
		if _, err := hub.Receive(ctx, id, handle); err != nil {
			log.Fatal(err.Error())
			return
		}
	}
}

func receiveMessagesFromPartition(hub *eventhub.Hub, id string, handle func(c context.Context, e *eventhub.Event) error) {
	ctx := context.Background()
	if _, err := hub.Receive(ctx, id, handle); err != nil {
		log.Fatal(err.Error())
		return
	}
}

func closeEventHub(hub *eventhub.Hub) {
	schan := make(chan os.Signal, 1)
	signal.Notify(schan, os.Interrupt, os.Kill)
	<-schan

	if err := hub.Close(context.Background()); err != nil {
		log.Fatal(err.Error())
	}
}

/*
FILE FUNCTIONS
*/
func readToByteArr(file string) []byte {
	f, err := os.OpenFile(file, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer func(f *os.File) {
		if err := f.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}(f)

	fstat, err := f.Stat()
	if err != nil {
		log.Fatal(err.Error())
	}

	barr := make([]byte, fstat.Size())
	_, err = bufio.NewReader(f).Read(barr)
	if err != nil && err != io.EOF {
		log.Fatal(err.Error())
	}
	return barr
}
