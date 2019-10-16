// Copyright 2019 Pan Jiaming (jiaming.pan@gmail.com)

package internal

import (
	"context"
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
	"go.etcd.io/etcd/clientv3"
)

var mlog = GetLogger("multi")

type MultiCloud struct {
	config           *MultiCloudConfig
	backends         map[string]StorageBackend
	bucket           string
	cap              Capabilities
	kv               *clientv3.Client
	requestIDs       map[string]interface{}
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

func NewMultiCloud(bucket string, flags *FlagStorage, config *MultiCloudConfig) (*MultiCloud, error) {
	cfg := clientv3.Config{
		Endpoints:   strings.Split(flags.EtcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	cloud := &MultiCloud{
		bucket:        bucket,
		cap:           Capabilities{Name: "s3"},
		kv:            c,
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
	_, err = cloud.kv.Put(context.TODO(), key, string(bytes))
	return err
}

func (cloud *MultiCloud) kvGet(key string, value interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := cloud.kv.Get(ctx, key)
	defer cancel()
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return syscall.ENOENT
	}

	return json.Unmarshal([]byte(resp.Kvs[0].Value), value)
}

func (cloud *MultiCloud) kvDelete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cloud.kv.Delete(ctx, key)
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

func (cloud *MultiCloud) listBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	count := 0
	items := []string{}
	prefixes := make(map[string]int)
	blobInfos := make(map[string]*BlobInfo)
	output := &ListBlobsOutput{}
	var nextToken *string

	for {
		var resp *clientv3.GetResponse
		var err error
		if nextToken != nil {
			resp, err = cloud.kv.Get(ctx, cloud.kvBlobKey(*nextToken),
				clientv3.WithFromKey(),
				clientv3.WithLimit(50),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		} else if param.ContinuationToken != nil {
			resp, err = cloud.kv.Get(ctx, cloud.kvBlobKey(*param.ContinuationToken),
				clientv3.WithFromKey(),
				clientv3.WithLimit(50),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		} else if param.Prefix != nil {
			resp, err = cloud.kv.Get(ctx, cloud.kvBlobKey(*param.Prefix),
				clientv3.WithPrefix(),
				clientv3.WithLimit(50),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		} else {
			mlog.Debugf("No ContinuationToken neither Prefix")
			return nil, syscall.EINVAL
		}
		if err != nil {
			mlog.Debugf("ListBlobs Got error %+v", err)
			return nil, err
		}

		if len(resp.Kvs) == 0 {
			goto out
		}

		for _, kv := range resp.Kvs {
			var subKey string
			key := string(kv.Key)
			key = strings.TrimPrefix(key, cloud.kvBlobKey(""))
			delimiterIndex := -1

			if param.Prefix != nil {
				subKey = strings.TrimPrefix(key, *param.Prefix)
			} else {
				subKey = key
			}

			if param.Delimiter != nil {
				delimiterIndex = strings.Index(subKey, *param.Delimiter)
			}

			if delimiterIndex != -1 {
				// This key is a prefix
				prefix := subKey[:delimiterIndex+1]
				if cnt, ok := prefixes[prefix]; ok {
					prefixes[prefix] = cnt + 1
				} else {
					count++
					if param.MaxKeys != nil && count > int(*param.MaxKeys) {
						output.NextContinuationToken = &key
						output.IsTruncated = true
						goto out
					}
					prefixes[prefix] = 1
				}
			} else {
				// This key is not a prefix
				count++
				if param.MaxKeys != nil && count > int(*param.MaxKeys) {
					output.NextContinuationToken = &key
					output.IsTruncated = true
					goto out
				}
				items = append(items, key)
				var info BlobInfo
				json.Unmarshal(kv.Value, &info)
				blobInfos[key] = &info
			}
		}

		if !resp.More {
			goto out
		} else {
		}
	}
out:
	sort.Strings(items)
	for _, item := range items {
		info := blobInfos[item]
		if info.Size == nil {
			size := uint64(0)
			info.Size = &size
		}
		output.Items = append(output.Items,
			BlobItemOutput{Key: &item,
				ETag:         info.Etag,
				LastModified: info.LastModified,
				Size:         *info.Size,
				StorageClass: info.StorageClass})
	}

	for prefix, _ := range prefixes {
		output.Prefixes = append(output.Prefixes, BlobPrefixOutput{&prefix})
	}

	mlog.Debugf("Got listKV %+v", output)

	return output, nil
}

func (cloud *MultiCloud) putIntentLog(reqId string, op string, param interface{}) error {
	key := cloud.kvIntentKey(reqId, op)
	err := cloud.kvPut(key, param)
	return err
}

func (cloud *MultiCloud) deleteIntentLog(reqId string, op string) error {
	key := cloud.kvIntentKey(reqId, op)
	_, err := cloud.kv.Delete(context.Background(), key, nil)
	return err
}

func (cloud *MultiCloud) putMultiPartInfo(uploadId string, info *MultiPartUploadInfo) error {
	key := "multipart/" + uploadId
	err := cloud.kvPut(key, info)
	return err
}

func (cloud *MultiCloud) deleteMultiPartInfo(uploadId string) error {
	key := "multipart/" + uploadId
	_, err := cloud.kv.Delete(context.Background(), key, nil)
	return err
}

func (cloud *MultiCloud) getMultiPartInfo(uploadId string) (*MultiPartUploadInfo, error) {
	key := "multipart/" + uploadId
	var info MultiPartUploadInfo
	err := cloud.kvGet(key, &info)
	return &info, err

}

func requestId() string {
	return fmt.Sprintf("%x", uuid.Must(uuid.NewV4()))[:8]
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
		mlog.Debugf("Leave HeadBlog out %v, error %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getBlobInfo(param.Key)
	if err != nil {
		mlog.Debugf("Failed to get backend error %+v", err)
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, reqId)
		}
		return nil, asAwsError(err)
	}
	backend, _ := cloud.backends[info.Node]
	mlog.Debugf("Got backend %+v", backend)

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
	out, err = cloud.listBlobs(param)

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
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, reqId)
		}
		return nil, asAwsError(err)
	}

	backend := cloud.backends[info.Node]

	out, err = backend.DeleteBlob(param)

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
		info, err := cloud.getBlobInfo(item)
		if err != nil {
			if err == syscall.ENOENT {
				return nil, asAwsRequestError(err, reqId)
			}
			return nil, asAwsError(err)
		}
		if l, ok := itemsMap[info.Node]; ok {
			itemsMap[info.Node] = append(l, item)
		} else {
			itemsMap[info.Node] = []string{item}
		}
	}

	for name, items := range itemsMap {
		backend, _ := cloud.backends[name]
		input := &DeleteBlobsInput{Items: items}
		_, err := backend.DeleteBlobs(input)
		if err != nil {
			if err == syscall.ENOENT {
				return nil, asAwsRequestError(err, reqId)
			}
			return nil, asAwsError(err)
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
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, reqId)
		}
		return nil, asAwsError(err)
	}

	backend, ok := cloud.backends[info.Node]
	if !ok {
		return nil, asAwsError(syscall.EAGAIN)
	}

	if info2, err := cloud.getBlobInfo(param.Destination); err == nil {
		if info2.Node != info.Node {
			backend2, ok := cloud.backends[info2.Node]
			if !ok {
				return nil, asAwsError(syscall.EAGAIN)
			}
			_, err := backend2.DeleteBlob(&DeleteBlobInput{Key: param.Destination})
			if err == nil {
				if err == syscall.ENOENT {
					return nil, asAwsRequestError(err, reqId)
				}
				return nil, asAwsError(err)
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
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, reqId)
		} else {
			return nil, asAwsError(err)
		}
	}

	if info2, err := cloud.getBlobInfo(param.Destination); err == nil {
		if info.Node != info2.Node {
			backend, _ := cloud.backends[info2.Node]
			_, err := backend.DeleteBlob(&DeleteBlobInput{Key: param.Destination})
			if err == nil {
				if err == syscall.ENOENT {
					return nil, asAwsRequestError(err, reqId)
				} else {
					return nil, asAwsError(err)
				}
			}
		}
	}

	backend, _ := cloud.backends[info.Node]

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
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, reqId)
		}
		return nil, asAwsError(err)
	}
	backend := cloud.backends[info.Node]

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
		mlog.Debugf("Leave MultiPartBlobBegin, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	name, backend, err := cloud.selectBackend(param.Key)
	if err != nil {
		return nil, err
	}

	out, err = backend.MultipartBlobBegin(param)
	info := MultiPartUploadInfo{Node: name}
	err = cloud.putMultiPartInfo(*out.UploadId, &info)
	if err != nil {
		backend.MultipartBlobAbort(out)
		return nil, err
	}

	return out, err
}

func (cloud *MultiCloud) MultipartBlobAdd(param *MultipartBlobAddInput) (out *MultipartBlobAddOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MultiPartBlobAdd, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave MultiPartBlobAdd, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getMultiPartInfo(*param.Commit.UploadId)
	if err != nil {
		return nil, err
	}
	backend, ok := cloud.backends[info.Node]
	if !ok {
		return nil, asAwsError(syscall.EAGAIN)
	}
	out, err = backend.MultipartBlobAdd(param)

	return
}

func (cloud *MultiCloud) MultipartBlobAbort(param *MultipartBlobCommitInput) (out *MultipartBlobAbortOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MultiPartBlobAbort, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave MultiPartBlobAbort, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getMultiPartInfo(*param.UploadId)
	if err != nil {
		return nil, err
	}
	backend, ok := cloud.backends[info.Node]
	if !ok {
		return nil, asAwsError(syscall.EAGAIN)
	}

	out, err = backend.MultipartBlobAbort(param)

	return
}

func (cloud *MultiCloud) MultipartBlobCommit(param *MultipartBlobCommitInput) (out *MultipartBlobCommitOutput, err error) {
	reqId := requestId()

	mlog.Debugf("Enter MultiPartBlobCommit, param %+v, reqId %s", param, reqId)
	defer func() {
		mlog.Debugf("Leave MultiPartBlobCommit, out %+v, err %v, reqId %s", out, err, reqId)
	}()

	info, err := cloud.getMultiPartInfo(*param.UploadId)
	if err != nil {
		return nil, err
	}
	backend, ok := cloud.backends[info.Node]
	if !ok {
		return nil, asAwsError(syscall.EAGAIN)
	}

	out, err = backend.MultipartBlobCommit(param)
	if err != nil {
		return nil, err
	}

	blobInfo := &BlobInfo{}
	blobInfo.Etag = out.ETag
	blobInfo.Node = info.Node

	key := cloud.kvBlobKey(*param.Key)
	cloud.kvPut(key, &blobInfo)

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
