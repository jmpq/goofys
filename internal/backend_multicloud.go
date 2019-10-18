// Copyright 2019 Pan Jiaming (jiaming.pan@gmail.com)

package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	. "github.com/kahing/goofys/api/common"
	uuid "github.com/satori/go.uuid"
)

var mlog = GetLogger("multi")

type MultiCloud struct {
	config           *MultiCloudConfig
	backends         map[string]StorageBackend
	bucket           string
	cap              Capabilities
	kv               KVDB
	backendNodes     []string
	deadBackendNodes []string
	backendStatus    map[string]bool
	mutex            sync.RWMutex
}

type BlobInfo struct {
	Node         string
	Etag         *string
	LastModified *time.Time
	Size         *uint64
	StorageClass *string
}

type MultiPartUploadInfo struct {
	Node string
}

func httpStatusCode(err error) int {
	switch err {
	case syscall.EINVAL:
		return 400
	case syscall.EACCES:
		return 401
	case syscall.ENOENT:
		return 404
	case syscall.ENOTSUP:
		return 405
	case syscall.EAGAIN:
		return 429
	default:
		return 500
	}
}

func asAwsError(err error) awserr.Error {
	return awserr.New("", "", err)
}

func asAwsRequestError(err error, reqId string) awserr.RequestFailure {
	return awserr.NewRequestFailure(
		awserr.New("", "", err), httpStatusCode(err), reqId)
}

func derefString(pointer *string) string {
	if pointer != nil {
		return *pointer
	} else {
		return "<nil>"
	}
}

func NewMultiCloud(bucket string, flags *FlagStorage, config *MultiCloudConfig) (*MultiCloud, error) {
	kvdb, err := NewEtcdDB(flags.EtcdEndpoints, false)
	if err != nil {
		return nil, err
	}

	cloud := &MultiCloud{
		bucket:        bucket,
		cap:           Capabilities{Name: "s3"},
		kv:            kvdb,
		backends:      make(map[string]StorageBackend),
		backendStatus: make(map[string]bool),
	}

	for _, bc := range config.Backends {
		var config S3Config
		config.AccessKey = bc.AccessKey
		config.SecretKey = bc.SecretKey
		config.Region = "us-east-1"
		config.RegionSet = true

		if bc.Type == "s3" {
			flags.Endpoint = bc.Endpoint
			mlog.Debugf("S3 backend config %+v", config)
			backend, err := NewS3(bucket, flags, &config)
			if err != nil {
				continue
			}

			cloud.backends[bc.Name] = backend
			cloud.backendNodes = append(cloud.backendNodes, bc.Name)
			cloud.backendStatus[bc.Name] = true
		}
	}
	sort.Strings(cloud.backendNodes)

	ticker := time.NewTicker(30 * time.Second)

	go func(t *time.Ticker) {
		for {
			<-t.C
			cloud.checkNodeStatus()
		}
	}(ticker)

	return cloud, nil
}

func (cloud *MultiCloud) markNodeStatus(node string, healthy bool) {
	// check if it is already in the dead/alive list
	cloud.mutex.RLock()
	if cloud.backendStatus[node] == healthy {
		cloud.mutex.RUnlock()
		return
	}
	cloud.mutex.RUnlock()

	mlog.Debugf("Mark node healhty of %v to %v", node, healthy)
	mlog.Debugf("backendNodes %+v, deadBackendNodes %+v", cloud.backendNodes, cloud.deadBackendNodes)

	cloud.mutex.Lock()
	defer cloud.mutex.Unlock()

	cloud.backendStatus[node] = healthy

	newBackendNodes := []string{}
	newDeadBackendNodes := []string{}

	if healthy {
		for _, n := range cloud.backendNodes {
			newBackendNodes = append(newBackendNodes, n)
		}
		for _, n := range cloud.deadBackendNodes {
			if n == node {
				newBackendNodes = append(newBackendNodes, n)
			} else {
				newDeadBackendNodes = append(newDeadBackendNodes, n)
			}
		}
	} else {
		for _, n := range cloud.deadBackendNodes {
			newDeadBackendNodes = append(newDeadBackendNodes, n)
		}
		for _, n := range cloud.backendNodes {
			if n == node {
				newDeadBackendNodes = append(newDeadBackendNodes, n)
			} else {
				newBackendNodes = append(newBackendNodes, n)
			}
		}
	}

	cloud.backendNodes = newBackendNodes
	cloud.deadBackendNodes = newDeadBackendNodes

	sort.Strings(cloud.backendNodes)
	sort.Strings(cloud.deadBackendNodes)

	mlog.Debugf("backendNodes %+v, deadBackendNodes %+v", cloud.backendNodes, cloud.deadBackendNodes)
}

func (cloud *MultiCloud) checkNodeStatus() {
	for n, backend := range cloud.backends {
		_, err := backend.HeadBlob(&HeadBlobInput{Key: "."})
		if err != nil {
			cloud.markNodeStatus(n, false)
		} else {
			cloud.markNodeStatus(n, true)
		}
	}
}

func (cloud *MultiCloud) selectBackend(key string) (string, StorageBackend, error) {
	cloud.mutex.RLock()
	defer cloud.mutex.RUnlock()

	if len(cloud.backendNodes) == 0 {
		return "", nil, errors.New("no healthy node available")
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	index := int(h.Sum32()) % len(cloud.backendNodes)
	name := cloud.backendNodes[index]

	return name, cloud.backends[name], nil
}

func (cloud *MultiCloud) getBackendChecked(node string) (StorageBackend, error) {
	cloud.mutex.RLock()
	defer cloud.mutex.RUnlock()

	mlog.Debugf("node %s backendStatus %+v", node, cloud.backendStatus)
	healthy, ok := cloud.backendStatus[node]
	if !ok {
		return nil, syscall.ENOENT
	}
	if !healthy {
		return nil, syscall.EAGAIN
	}
	backend, ok := cloud.backends[node]
	if !ok {
		return nil, syscall.ENOENT
	}

	return backend, nil
}

func (cloud *MultiCloud) kvBlobKey(key string) string {
	return "blob/" + cloud.bucket + ":" + key
}

func (cloud *MultiCloud) kvIntentKey(reqId string, op string) string {
	return "intent/" + reqId + "-" + op
}

func (cloud *MultiCloud) kvPut(key string, value interface{}) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	err = cloud.kv.Set(key, bytes)

	return err
}

func (cloud *MultiCloud) kvGet(key string, value interface{}) error {
	bytes, err := cloud.kv.Get(key)
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, value)
}

func (cloud *MultiCloud) kvDelete(key string) error {
	err := cloud.kv.Delete(key)
	return err
}

func (cloud *MultiCloud) putBlobInfo(key string, info *BlobInfo) error {
	return cloud.kvPut(cloud.kvBlobKey(key), info)
}

func (cloud *MultiCloud) getBlobInfo(key string) (*BlobInfo, error) {
	var info BlobInfo
	err := cloud.kvGet(cloud.kvBlobKey(key), &info)
	return &info, err
}

func (cloud *MultiCloud) deleteBlobInfo(key string) error {
	err := cloud.kvDelete(cloud.kvBlobKey(key))
	return err
}

func (cloud *MultiCloud) listBlobs(param *ListBlobsInput, reqId string) (*ListBlobsOutput, error) {
	items := []string{}
	prefixes := make(map[string]int)
	blobInfos := make(map[string]*BlobInfo)
	output := &ListBlobsOutput{}

	prefix := cloud.kvBlobKey("")
	if param.Prefix != nil {
		prefix = cloud.kvBlobKey(*param.Prefix)
	}
	var start *string = nil
	if param.ContinuationToken != nil {
		start = param.ContinuationToken
	}

	maxKeys := 10000000000
	if param.MaxKeys != nil {
		maxKeys = int(*param.MaxKeys)
	}

	iteration := 0
	for {
		// scan keys from start key, limit 100, with prefix
		s := ""
		if start != nil {
			s = *start
		}

		kvs, err := cloud.kv.Scan(&prefix, start, 20)
		if err != nil || len(kvs) == 0 {
			if err != nil {
				mlog.Debugf("ListBlobs error %+v, reqId %s", err, reqId)
				err = nil
			}
			mlog.Debugf("prefix %s, start %v, got 0 items, reqId %s", prefix, s, reqId)
			goto out
		}
		mlog.Debugf("prefix %s, start %v, got %d items, reqId %s", prefix, s, len(kvs), reqId)

		for _, kv := range kvs {
			var subKey string
			key := string(kv.Key)

			if maxKeys == 0 {
				output.NextContinuationToken = &key
				output.IsTruncated = true
				goto out
			}

			blobKey := strings.TrimPrefix(key, cloud.kvBlobKey(""))
			delimiterIndex := -1

			//mlog.Debugf("blogKey => %s, value => %s, reqId %s", blobKey, kv.Value, reqId)

			if param.Prefix != nil {
				subKey = strings.TrimPrefix(blobKey, *param.Prefix)
			} else {
				subKey = blobKey
			}

			if param.Delimiter != nil {
				delimiterIndex = strings.Index(subKey, *param.Delimiter)
			}

			if delimiterIndex != -1 {
				// This key is a prefix
				prefix := subKey[:delimiterIndex+1]
				if param.Prefix != nil {
					prefix = *param.Prefix + prefix
				}
				if cnt, ok := prefixes[prefix]; ok {
					prefixes[prefix] = cnt + 1
				} else {
					prefixes[prefix] = 1
					maxKeys--
				}
				nextKey := cloud.kvBlobKey(prefix)
				bytes := []byte(nextKey)
				bytes[len(bytes)-1]++
				nextKey = string(bytes)

				start = &nextKey
			} else {
				// This key is an item
				var info BlobInfo

				items = append(items, blobKey)

				json.Unmarshal(kv.Value, &info)
				blobInfos[blobKey] = &info
				maxKeys--

				nextKey := key + " "
				start = &nextKey
			}
		}
		iteration++
	}

out:
	for _, item := range items {
		info := blobInfos[item]
		if info.Size == nil {
			size := uint64(0)

			out, e := cloud.HeadBlob(&HeadBlobInput{Key: item})
			if e == nil {
				size = out.Size

				info.Size = &size
				info.Etag = out.ETag
				info.LastModified = out.LastModified
				info.StorageClass = out.StorageClass

				cloud.putBlobInfo(item, info)
			} else {
				info.Size = &size
			}
		}
		i := item
		output.Items = append(output.Items,
			BlobItemOutput{Key: &i,
				ETag:         info.Etag,
				LastModified: info.LastModified,
				Size:         *info.Size,
				StorageClass: info.StorageClass})
	}

	for prefix, _ := range prefixes {
		p := prefix
		output.Prefixes = append(output.Prefixes, BlobPrefixOutput{&p})
	}

	return output, nil
}

func (cloud *MultiCloud) putIntentLog(reqId string, op string, param interface{}) error {
	key := cloud.kvIntentKey(reqId, op)
	err := cloud.kvPut(key, param)
	return err
}

func (cloud *MultiCloud) deleteIntentLog(reqId string, op string) error {
	key := cloud.kvIntentKey(reqId, op)
	err := cloud.kv.Delete(key)
	return err
}

func (cloud *MultiCloud) putMultiPartInfo(uploadId string, info *MultiPartUploadInfo) error {
	key := "multipart/" + uploadId
	err := cloud.kvPut(key, info)
	return err
}

func (cloud *MultiCloud) deleteMultiPartInfo(uploadId string) error {
	key := "multipart/" + uploadId
	err := cloud.kv.Delete(key)
	return err
}

func (cloud *MultiCloud) getMultiPartInfo(uploadId string) (*MultiPartUploadInfo, error) {
	key := "multipart/" + uploadId
	var info MultiPartUploadInfo

	err := cloud.kvGet(key, &info)

	return &info, err

}

func requestId() string {
	id := uuid.Must(uuid.NewV4())
	return fmt.Sprintf("%v", id)[:8]
}

func (cloud *MultiCloud) Init(key string) error {
	for node, backend := range cloud.backends {
		err := backend.Init(key)
		if err != nil {
			cloud.markNodeStatus(node, false)
			return err
		}
	}
	return nil
}

func (cloud *MultiCloud) Capabilities() *Capabilities {
	return &cloud.cap
}

// typically this would return bucket/prefix
func (cloud *MultiCloud) Bucket() string {
	return cloud.bucket
}

func (cloud *MultiCloud) HeadBlob(param *HeadBlobInput) (out *HeadBlobOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter HeadBlob %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave HeadBlog out %+v, error %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getBlobInfo(param.Key)
	if err != nil {
		mlog.Debugf("Failed to get blobInfo for %s error %+v, reqId %s", param.Key, err, reqId)
		err = asAwsRequestError(err, reqId)
		return
	}
	backend, err := cloud.getBackendChecked(info.Node)
	if err != nil {
		mlog.Debugf("Failed to get backend for %s, reqId %s", info.Node, reqId)
		err = asAwsRequestError(err, reqId)
		return
	}

	mlog.Debugf("Got backend %s for %s, reqId %s", info.Node, param.Key, reqId)
	out, err = backend.HeadBlob(param)

	return
}

func (cloud *MultiCloud) ListBlobs(param *ListBlobsInput) (out *ListBlobsOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter ListBlobs, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave ListBlobs, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	if param.Prefix != nil {
		mlog.Debugf("    Prefix %v", *param.Prefix)
	}
	if param.StartAfter != nil {
		mlog.Debugf("    StartAfter %v", *param.StartAfter)
	}
	if param.Delimiter != nil {
		mlog.Debugf("    Delimiter %v", *param.Delimiter)
	}
	if param.ContinuationToken != nil {
		mlog.Debugf("    ContinuationToken %v", *param.ContinuationToken)
	}
	if param.MaxKeys != nil {
		mlog.Debugf("    maxKeys %v", *param.MaxKeys)
	}

	out, err = cloud.listBlobs(param, reqId)
	if err != nil {
		err = asAwsRequestError(err, reqId)
	}

	return
}

func (cloud *MultiCloud) DeleteBlob(param *DeleteBlobInput) (out *DeleteBlobOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter DeleteBlob, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave DeleteBlob, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	err = cloud.putIntentLog(reqId, "DeleteBlob", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(reqId, "DeleteBlob")

	info, err := cloud.getBlobInfo(param.Key)
	if err != nil {
		err = asAwsRequestError(err, reqId)
		return
	}

	backend, err := cloud.getBackendChecked(info.Node)
	if err != nil {
		err = asAwsRequestError(err, reqId)
		return
	}

	out, err = backend.DeleteBlob(param)
	if err != nil {
		cloud.kvDelete(param.Key)
	}

	return
}

func (cloud *MultiCloud) DeleteBlobs(param *DeleteBlobsInput) (out *DeleteBlobsOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter DeleteBlobs, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave DeleteBlobs, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	err = cloud.putIntentLog(reqId, "DeleteBlobs", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(reqId, "DeleteBlobs")

	itemsMap := make(map[string][]string)
	for _, item := range param.Items {
		info, e := cloud.getBlobInfo(item)
		if e != nil {
			err = asAwsRequestError(e, reqId)
			return
		}
		if l, ok := itemsMap[info.Node]; ok {
			itemsMap[info.Node] = append(l, item)
		} else {
			itemsMap[info.Node] = []string{item}
		}
	}

	for name, items := range itemsMap {
		backend, e := cloud.getBackendChecked(name)
		if e != nil {
			err = asAwsRequestError(e, reqId)
			return
		}
		input := &DeleteBlobsInput{Items: items}
		_, e = backend.DeleteBlobs(input)
		if e != nil {
			err = asAwsRequestError(e, reqId)
			return
		}

		for _, item := range items {
			cloud.kvDelete(item)
		}
	}

	out = &DeleteBlobsOutput{RequestId: reqId}
	err = nil

	return
}

func (cloud *MultiCloud) RenameBlob(param *RenameBlobInput) (out *RenameBlobOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter RenameBlob, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave RenameBlob, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	err = cloud.putIntentLog(reqId, "RenameBlob", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(reqId, "RenameBlob")

	info, err := cloud.getBlobInfo(param.Source)
	if err != nil {
		err = asAwsRequestError(err, reqId)
		return
	}

	backend, e := cloud.getBackendChecked(info.Node)
	if e != nil {
		err = asAwsRequestError(e, reqId)
		return
	}

	if info2, e := cloud.getBlobInfo(param.Destination); e == nil {
		if info2.Node != info.Node {
			backend2, e := cloud.getBackendChecked(info2.Node)
			if e != nil {
				err = asAwsRequestError(e, reqId)
				return
			}
			_, err = backend2.DeleteBlob(&DeleteBlobInput{Key: param.Destination})
			if err == nil {
				err = asAwsRequestError(err, reqId)
				return
			}
		}
	}

	out, err = backend.RenameBlob(param)
	if err != nil {
		return nil, err
	}

	cloud.deleteBlobInfo(param.Source)
	cloud.putBlobInfo(param.Destination, info)

	return out, nil
}

func (cloud *MultiCloud) CopyBlob(param *CopyBlobInput) (out *CopyBlobOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter CopyBlob, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave CopyBlob, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	err = cloud.putIntentLog(reqId, "CopyBlob", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(reqId, "CopyBlob")

	info, err := cloud.getBlobInfo(param.Source)
	if err != nil {
		err = asAwsRequestError(err, reqId)
		return
	}

	if info2, e := cloud.getBlobInfo(param.Destination); e == nil {
		if info.Node != info2.Node {
			backend2, e := cloud.getBackendChecked(info2.Node)
			if e != nil {
				err = asAwsRequestError(e, reqId)
				return
			}
			_, e = backend2.DeleteBlob(&DeleteBlobInput{Key: param.Destination})
			if e == nil {
				err = asAwsRequestError(err, reqId)
				return
			}
		}
	}

	backend, e := cloud.getBackendChecked(info.Node)
	if e != nil {
		err = asAwsRequestError(err, reqId)
		return
	}

	cloud.putBlobInfo(param.Destination, info)

	out, err = backend.CopyBlob(param)

	return
}

func (cloud *MultiCloud) GetBlob(param *GetBlobInput) (out *GetBlobOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter GetBlob, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave GetBlob, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getBlobInfo(param.Key)
	if err != nil {
		err = asAwsRequestError(err, reqId)
		return
	}
	backend, e := cloud.getBackendChecked(info.Node)
	if e != nil {
		err = asAwsRequestError(e, reqId)
		return
	}

	out, err = backend.GetBlob(param)

	return
}

func (cloud *MultiCloud) PutBlob(param *PutBlobInput) (out *PutBlobOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter PutBlob, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave PutBlob, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	name, backend, err := cloud.selectBackend(param.Key)
	if err != nil {
		mlog.Debugf("error %+v", err)
		return nil, err
	}
	out, err = backend.PutBlob(param)
	if err != nil {
		mlog.Debugf("error %+v", err)
		return nil, err
	}
	blobInfo := BlobInfo{}
	blobInfo.Size = param.Size
	blobInfo.Etag = out.ETag
	blobInfo.StorageClass = out.StorageClass

	now := time.Now()
	blobInfo.LastModified = &now
	blobInfo.Node = name

	key := cloud.kvBlobKey(param.Key)
	err = cloud.kvPut(key, &blobInfo)
	if err != nil {
		mlog.Debugf("error %+v", err)
		return nil, err
	}
	return out, nil
}

func (cloud *MultiCloud) MultipartBlobBegin(param *MultipartBlobBeginInput) (out *MultipartBlobCommitInput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MultiPartBlobBegin, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave MultiPartBlobBegin, err %v, reqId %s", err, reqId)
	}()

	name, backend, err := cloud.selectBackend(param.Key)
	if err != nil {
		mlog.Errorf("Failed to find backend for %s, reqId %s", param.Key, reqId)
		return
	}

	out, err = backend.MultipartBlobBegin(param)
	if err != nil {
		mlog.Errorf("Failed to begin multipart upload for %s, reqId %s", param.Key, reqId)
		return
	}

	info := MultiPartUploadInfo{Node: name}

	mlog.Debugf("save uploadId %s, node %s", derefString(out.UploadId), name)

	err = cloud.putMultiPartInfo(*out.UploadId, &info)
	if err != nil {
		mlog.Errorf("Failed to put multipart upload information for %s, reqId %s", param.Key, reqId)
		backend.MultipartBlobAbort(out)
		return
	}

	return
}

func (cloud *MultiCloud) MultipartBlobAdd(param *MultipartBlobAddInput) (out *MultipartBlobAddOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MultiPartBlobAdd, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave MultiPartBlobAdd, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getMultiPartInfo(*param.Commit.UploadId)
	if err != nil {
		mlog.Errorf("Failed to get multipart info for %s, error %v, reqId %s",
			derefString(param.Commit.Key), err, reqId)
		return
	}
	backend, e := cloud.getBackendChecked(info.Node)
	if e != nil {
		mlog.Errorf("Failed to get backend for %s, reqId %s", info.Node, reqId)
		err = asAwsRequestError(e, reqId)
		return
	}
	out, err = backend.MultipartBlobAdd(param)
	if err != nil {
		mlog.Errorf("Failed to add blob part for %s, error %s, reqId %s", *param.Commit.Key, err, reqId)
	}

	return
}

func (cloud *MultiCloud) MultipartBlobAbort(param *MultipartBlobCommitInput) (out *MultipartBlobAbortOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MultiPartBlobAbort, UploadId %s, Key %s, reqId %s",
		derefString(param.UploadId), derefString(param.Key), reqId)
	defer func() {
		mlog.Debugf("Leave MultiPartBlobAbort, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getMultiPartInfo(*param.UploadId)
	if err != nil {
		return nil, err
	}
	backend, e := cloud.getBackendChecked(info.Node)
	if e != nil {
		err = asAwsRequestError(e, reqId)
		return
	}

	out, err = backend.MultipartBlobAbort(param)

	return
}

func (cloud *MultiCloud) MultipartBlobCommit(param *MultipartBlobCommitInput) (out *MultipartBlobCommitOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MultiPartBlobCommit, uploadId %s, key %s, reqId %s",
		derefString(param.UploadId), derefString(param.Key), reqId)
	defer func() {
		mlog.Debugf("Leave MultiPartBlobCommit, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getMultiPartInfo(derefString(param.UploadId))
	if err != nil {
		mlog.Errorf("Failed to get multiPart upload info for %s, reqId %s",
			derefString(param.UploadId), reqId)
		return
	}

	backend, err := cloud.getBackendChecked(info.Node)
	if err != nil {
		mlog.Errorf("Failed to get backend for %s, reqId %s", info.Node, reqId)
		err = asAwsRequestError(syscall.EAGAIN, reqId)
		return
	}

	mlog.Debugf("Found node %s", info.Node)
	out, err = backend.MultipartBlobCommit(param)
	if err != nil {
		mlog.Errorf("Failed to commit blob upload for %s, reqId %s", derefString(param.Key), reqId)
		return
	}

	blobInfo := &BlobInfo{}
	output, err := backend.HeadBlob(&HeadBlobInput{Key: *param.Key})
	if err == nil {
		blobInfo.Size = &output.Size
		blobInfo.Etag = output.ETag
		blobInfo.LastModified = output.LastModified
		blobInfo.StorageClass = output.StorageClass
		blobInfo.Node = info.Node
	} else {
		blobInfo.Etag = out.ETag
		blobInfo.Node = info.Node
	}

	cloud.putBlobInfo(*param.Key, blobInfo)
	if param.UploadId != nil {
		cloud.deleteMultiPartInfo(*param.UploadId)
	}

	err = nil

	return out, err
}

func (cloud *MultiCloud) MultipartExpire(param *MultipartExpireInput) (out *MultipartExpireOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MultiPartExpire, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave MultiPartExipre, %+v, err %v, reqId %s", out, err, reqId)
	}()

	for _, backend := range cloud.backends {
		backend.MultipartExpire(param)
	}

	out = &MultipartExpireOutput{}
	err = nil

	return
}

func (cloud *MultiCloud) RemoveBucket(param *RemoveBucketInput) (out *RemoveBucketOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter RemoveBucket, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave RemoveBucket, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	err = cloud.putIntentLog(reqId, "RemoveBucket", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(reqId, "RemoveBucket")

	for _, backend := range cloud.backends {
		backend.RemoveBucket(param)
	}
	out = &RemoveBucketOutput{RequestId: reqId}
	err = nil

	return
}

func (cloud *MultiCloud) MakeBucket(param *MakeBucketInput) (out *MakeBucketOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MakeBucket, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave MakeBucket, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	err = cloud.putIntentLog(reqId, "CreateBucket", param)
	if err != nil {
		return nil, err
	}
	for _, backend := range cloud.backends {
		backend.MakeBucket(param)
	}

	out = &MakeBucketOutput{RequestId: reqId}
	err = nil

	return
}
