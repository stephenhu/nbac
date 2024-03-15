package cmd

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"

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


var ScheduleIndex map[string] bool
var PlaysIndex map[string] bool

var blobs *minio.Client
var rawBlobs <-chan minio.ObjectInfo
var analyticsBlobs <-chan minio.ObjectInfo


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


func BlobName(b string) string {
	return strings.TrimSuffix(b, EXT_JSON)
} // BlobName


func LoadBlobIndexes() {

	ScheduleIndex 	= make(map[string] bool)
	PlaysIndex 			= make(map[string] bool)

	rawBlobs 				= BlobList(BucketRaw(cy))
	analyticsBlobs 	= BlobList(BucketAnalytics(cy))

	for b := range rawBlobs {

		name := BlobName(b.Key)

		if strings.Contains(name, PBP_SUFFIX) {
			PlaysIndex[name] = true
		} else if name != SCHEDULE_BLOB {
			ScheduleIndex[name] = true
		}

	}

} // LoadBlobIndexes


func BlobList(b string) <-chan minio.ObjectInfo {

	ctx := context.Background()
	
	return blobs.ListObjects(ctx, b,
		minio.ListObjectsOptions{})

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


func BlobPutFile(b string, k string) {

	ctx := context.Background()

	_, err := blobs.FPutObject(ctx, b, k, k, minio.PutObjectOptions{
		ContentType: JSON_CONTENT_TYPE})

	if err != nil {
		log.Println(err)
	}

} // BlobPutFile


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
