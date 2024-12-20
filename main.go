package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"io"
	"log"
	"os"
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
	}

	client, err := azblob.NewClient(url, cred, nil)
	if err != nil {
		log.Fatal(err.Error())
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

func downloadBlob(client *azblob.Client, containerName, filename, path string) {
	file, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.DownloadFile(context.TODO(), containerName, filename, file, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func downloadToByteArr(client *azblob.Client, containerName, filename string) []byte {
	var barr []byte
	_, err := client.DownloadBuffer(context.TODO(), containerName, filename, barr, nil)
	if err != nil {
		log.Fatal(err)
	}
	return barr
}

func uploadBlob(client *azblob.Client, containerName, filename, path string) {
	file, err := os.OpenFile(path+"/"+filename, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal(err)
	}

	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}(file)

	_, err = client.UploadFile(context.TODO(), containerName, filename, file, nil)
}

func uploadFromByteArr(client *azblob.Client, containerName, filename string, barr []byte) {
	_, err := client.UploadBuffer(context.TODO(), containerName, filename, barr, nil)
	if err != nil {
		log.Fatal(err)
	}
}

/*
FILE FUNCTIONS
*/
func readFile(file string) []byte {
	f, err := os.Open(file)
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
