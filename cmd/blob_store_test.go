package cmd

import (
	"fmt"
	"testing"
)


func Setup() {
	initBlobStore()
} // Setup


func TestBlobPut(t *testing.T) {

	initBlobStore()

  BlobPut(fmt.Sprintf(BUCKET_RAW, "2023"),
	  "../2023/20240301/0022300857.json")

} // TestBlobPut


func TestBlobList(t *testing.T) {

	initBlobStore()

  BlobList(fmt.Sprintf(BUCKET_RAW, "2023"))

} // TestBlobList