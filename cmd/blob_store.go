package cmd

import (
	"fmt"
	"strings"

	"github.com/madsportslab/nbalake"
	"github.com/minio/minio-go/v7"
)


var ScheduleIndex map[string] bool
var PlaysIndex map[string] bool

var rawBlobs <-chan minio.ObjectInfo
var analyticsBlobs <-chan minio.ObjectInfo


func LoadBlobIndexes() {

	ScheduleIndex 	= make(map[string] bool)
	PlaysIndex 			= make(map[string] bool)

	rawBlobs 				= nbalake.List(nbalake.BucketName(
		cy, nbalake.BUCKET_RAW))
	analyticsBlobs 	= nbalake.List(nbalake.BucketName(
		cy, nbalake.BUCKET_ANALYTICS))

	for b := range rawBlobs {

		name := fmt.Sprintf(b.Key, EXT_JSON)

		if strings.Contains(name, PBP_SUFFIX) {
			PlaysIndex[name] = true
		} else if name != SCHEDULE_BLOB {
			ScheduleIndex[name] = true
		}

	}

} // LoadBlobIndexes
