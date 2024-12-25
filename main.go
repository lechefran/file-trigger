package main

import (
	"bufio"
	"context"
	"fmt"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"io"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	client := createBlobClient("REPLACE_WITH_AZURE_BLOB_STORAGE_ACCOUNT_URL")
	createBlobContainer(client, "sample-blob-container")
}

/*
AZURE BLOB STORAGE FUNCTIONS
*/
func createBlobClient(url string) *azblob.Client {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatal(err.Error())
		return nil
	}

	client, err := azblob.NewClient(url, cred, nil)
	if err != nil {
		log.Fatal(err.Error())
		return nil
	}
	return client
}

func createBlobContainer(client *azblob.Client, name string) bool {
	var res bool
	fmt.Printf("Creating blob container %s\n", name)
	ctx := context.Background()
	_, err := client.CreateContainer(ctx, name, nil)
	if err != nil {
		res = false
	} else {
		res = true
	}
	return res
}

func downloadBlob(client *azblob.Client, containerName, filename, path string) bool {
	file, err := os.Create(path)
	if err != nil {
		log.Fatal(err.Error())
		return false
	}

	_, err = client.DownloadFile(context.TODO(), containerName, filename, file, nil)
	if err != nil {
		log.Fatal(err.Error())
		return false
	}
	return true
}

func downloadToByteArr(client *azblob.Client, containerName, filename string) []byte {
	var barr []byte
	if _, err := client.DownloadBuffer(context.TODO(), containerName, filename, barr, nil); err != nil {
		log.Fatal(err.Error())
		return nil
	}
	return barr
}

func uploadBlob(client *azblob.Client, containerName, filename, path string) bool {
	file, err := os.OpenFile(path+"/"+filename, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal(err.Error())
		return false
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}(file)

	if _, err = client.UploadFile(context.TODO(), containerName, filename, file, nil); err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func uploadFromByteArr(client *azblob.Client, containerName, filename string, barr []byte) bool {
	if _, err := client.UploadBuffer(context.TODO(), containerName, filename, barr, nil); err != nil {
		log.Fatal(err.Error())
		return false
	}
	return true
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
