package cmd

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/spf13/viper"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)


const (
  BLOB_API_SERVER				= "127.0.0.1:9000"
	BLOB_API_KEY       		= "key"
	BLOB_API_SECRET       = "secret"
	BLOB_CONF_FILE        = "config"
	BLOB_CONF_DIR         = "conf"
)


const (
	BUCKET_RAW						= "%s.nba.raw"
	BUCKET_ANALYTICS      = "%s.nba.analytics"
)


const (
	JSON_CONTENT_TYPE			= "application/json"
)


var ScheduleMap map[string] bool

var blobs *minio.Client


func readConfig() {

	viper.SetConfigName(BLOB_CONF_FILE)
	viper.SetConfigType(JSON_FILE)
	viper.AddConfigPath(BLOB_CONF_DIR)

	err := viper.ReadInConfig()

	if err != nil {
		log.Fatal(err)
	}

} // readConfig


func checkBuckets() {

	ctx := context.Background()

	raw 			:= BucketRaw(cy)
	analytics := BucketAnalytics(cy)

	ok, err := blobs.BucketExists(ctx, raw)

	if err != nil {
		log.Println(err)
	}

	if !ok {
		blobs.MakeBucket(ctx, raw, minio.MakeBucketOptions{})
	}

	ok, err = blobs.BucketExists(ctx, analytics)

	if err != nil {
		log.Println(err)
	}

	if !ok {
		blobs.MakeBucket(ctx, analytics, minio.MakeBucketOptions{})
	}

} // checkBuckets


func initBlobStore() {

	readConfig()

	c, err := minio.New(BLOB_API_SERVER, &minio.Options{
		Creds:  credentials.NewStaticV4(viper.GetString(BLOB_API_KEY),
		viper.GetString(BLOB_API_SECRET), ""),
		Secure: false,
	})

	if err != nil {
		log.Fatal(err)
	}

	blobs = c

	checkBuckets()

} // initBlobStore



func BucketRaw(y string) string {
	return fmt.Sprintf(BUCKET_RAW, y)
} // BucketRaw


func BucketAnalytics(y string) string {
	return fmt.Sprintf(BUCKET_ANALYTICS, y)
} // BucketAnalytics


func BlobExists(b string, k string) bool {
  
	ctx := context.Background()

	_, err := blobs.GetObject(ctx, b, k,
		minio.GetObjectOptions{})

	if err != nil {
		
		log.Println(err)
		return false

	} else {
		return true
	}

} // BlobExists


func BlobList(b string) {

	ctx := context.Background()

	ScheduleMap = make(map[string] bool)

	blobs := blobs.ListObjects(ctx, b,
		minio.ListObjectsOptions{})

	for b := range blobs {
		ScheduleMap[b.Key] = true
	}

} // BlobList


func BlobPut(b string, k string, r []byte) {

	ctx := context.Background()

	buf := bytes.NewReader(r)

	_, err := blobs.PutObject(ctx, b, k, buf, int64(buf.Len()),
	  minio.PutObjectOptions{ContentType: JSON_CONTENT_TYPE}) 

	if err != nil {
		log.Println(err)
	}

} // BlobPut


func BlobGet(b string, f string) []byte {

	ctx := context.Background()

	o, err := blobs.GetObject(ctx, b, f, minio.GetObjectOptions{})

	if err != nil {
		log.Println(err)
	} else {

		info, err := o.Stat()

		if err != nil {
			log.Println(err)
		} else {

			buf := make([]byte, info.Size)
	
			o.Read(buf)
	
			return buf
	
		}

	}

	return nil

} // BlobGet
