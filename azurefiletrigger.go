package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"log"
	"os"
)

type AzureFileTrigger struct {
	client   *azblob.Client
	consumer *azeventhubs.ConsumerClient
	producer *azeventhubs.ProducerClient
}

func (f *AzureFileTrigger) CreateBlobClient(url string) *AzureFileTrigger {
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

	return &AzureFileTrigger{
		client: client,
	}
}

func (f *AzureFileTrigger) SetConsumer(conn, hubName, consumerGroup string) {
	consumer, err := azeventhubs.NewConsumerClientFromConnectionString(conn, hubName, consumerGroup, nil)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	f.consumer = consumer
}

func (f *AzureFileTrigger) SetProducer(conn, hubName string) {
	producer, err := azeventhubs.NewProducerClientFromConnectionString(conn, hubName, nil)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	f.producer = producer
}

func (f *AzureFileTrigger) CreateBlobContainer(name string) bool {
	var res bool
	fmt.Printf("Creating blob container %s\n", name)
	ctx := context.Background()
	_, err := f.client.CreateContainer(ctx, name, nil)
	if err != nil {
		res = false
	} else {
		res = true
	}
	return res
}

func (f *AzureFileTrigger) DeleteBlobContainer(name string) bool {
	var res bool
	fmt.Printf("Creating blob container %s\n", name)
	ctx := context.Background()
	_, err := f.client.DeleteContainer(ctx, name, nil)
	if err != nil {
		res = false
	} else {
		res = true
	}
	return res
}

func (f *AzureFileTrigger) DownloadBlob(containerName, fileName string) bool {
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err.Error())
		return false
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}(file)

	if _, err = f.client.DownloadFile(context.TODO(), containerName, fileName, file, nil); err != nil {
		log.Fatal(err.Error())
		return false
	}
	return true
}

func (f *AzureFileTrigger) DownloadBlobToByteArr(containerName, fileName string) []byte {
	var res []byte
	if _, err := f.client.DownloadBuffer(context.TODO(), containerName, fileName, res, nil); err != nil {
		log.Fatal(err.Error())
		return nil
	}
	return res
}

func (f *AzureFileTrigger) UploadBlob(containerName, fileName, filePath string) bool {
	file, err := os.OpenFile(filePath+"/"+fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatal(err.Error())
		return false
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}(file)

	if _, err = f.client.UploadFile(context.TODO(), containerName, fileName, file, nil); err != nil {
		log.Fatal(err.Error())
		return false
	}
	return true
}

func (f *AzureFileTrigger) UploadBlobFromByteArr(containerName, fileName string, barr []byte) bool {
	if _, err := f.client.UploadBuffer(context.TODO(), containerName, fileName, barr, nil); err != nil {
		log.Fatal(err.Error())
		return false
	}
	return true
}
