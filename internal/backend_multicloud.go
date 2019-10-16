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
	mutex            sync.RWMutex
}

type BlobInfo struct {
	Node         string
	Etag         *string
	LastModified *time.Time
	Size         *uint64
	StorageClass *string
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

func asAwsRequestError(err error, requestId string) awserr.RequestFailure {
	return awserr.NewRequestFailure(
		awserr.New("", "", err), httpStatusCode(err), requestId)
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
		bucket:   bucket,
		cap:      Capabilities{Name: "s3"},
		kv:       c,
		backends: make(map[string]StorageBackend),
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
	if healthy {
		for _, n := range cloud.backendNodes {
			if n == node {
				cloud.mutex.RUnlock()
				return
			}
		}
	} else {
		for _, n := range cloud.deadBackendNodes {
			if n == node {
				cloud.mutex.RUnlock()
				return
			}
		}
	}
	cloud.mutex.RUnlock()

	mlog.Debugf("Mark node healhty of %v to %v", node, healthy)
	mlog.Debugf("backendNodes %+v, deadBackendNodes %+v", cloud.backendNodes, cloud.deadBackendNodes)

	cloud.mutex.Lock()
	defer cloud.mutex.Unlock()

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

func (cloud *MultiCloud) getBlogBackend(key string) (string, StorageBackend, error) {
	key = cloud.kvBlobKey(key)
	mlog.Debugf("key is %s for blob", key)

	var info BlobInfo
	err := cloud.kvGet(key, &info)
	if err != nil {
		return "", nil, err
	}

	node := info.Node
	mlog.Debugf("Get node name %s for blob", node)
	if backend, ok := cloud.backends[node]; ok {
		return node, backend, nil
	} else {
		return "", nil, syscall.ENOENT
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

func (cloud *MultiCloud) kvIntentKey(requestId string, op string) string {
	return "intent/" + requestId + "-" + op
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

func (cloud *MultiCloud) kvList(param *ListBlobsInput) (*ListBlobsOutput, error) {
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
			mlog.Debugf("Got listKV error %+v", err)
			return nil, err
		}

		mlog.Debugf("listKV response %+v", resp)
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

func (cloud *MultiCloud) writeIntentLog(requestId string, op string, param interface{}) error {
	key := cloud.kvIntentKey(requestId, op)
	err := cloud.kvPut(key, param)
	return err
}

func (cloud *MultiCloud) deleteIntentLog(requestId string, op string) error {
	key := cloud.kvIntentKey(requestId, op)
	_, err := cloud.kv.Delete(context.Background(), key, nil)
	return err
}

func (cloud *MultiCloud) writeMultiPartNodeName(uploadId string, name string) error {
	key := "multipart/" + uploadId
	err := cloud.kvPut(key, &name)
	return err
}

func (cloud *MultiCloud) deleteMultiPartNodeName(uploadId string) error {
	key := "multipart/" + uploadId
	_, err := cloud.kv.Delete(context.Background(), key, nil)
	return err
}

func (cloud *MultiCloud) getMultiPartNodeName(uploadId string) (string, error) {
	key := "multipart/" + uploadId
	var node string
	err := cloud.kvGet(key, &node)
	return node, err

}

func requestId() string {
	return fmt.Sprintf("%x", uuid.Must(uuid.NewV4()))[:8]
}

func (cloud *MultiCloud) Init(key string) error {
	for _, backend := range cloud.backends {
		err := backend.Init(key)
		if err != nil {
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

func (cloud *MultiCloud) HeadBlob(param *HeadBlobInput) (output *HeadBlobOutput, err error) {
	requestId := requestId()
	mlog.Debugf("HeadBlob %+v", param)

	defer func() {
		mlog.Debugf("HeadBlog %+v output %v, error %v", param, output, err)
	}()

	_, backend, err := cloud.getBlogBackend(param.Key)
	if err != nil {
		mlog.Debugf("Failed to get backend error %+v", err)
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, requestId)
		}
		return nil, asAwsError(err)
	}
	mlog.Debugf("Got backend %+v", backend)

	output, err = backend.HeadBlob(param)
	return
}

func (cloud *MultiCloud) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {

	mlog.Debugf("list Blobs")
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
	return cloud.kvList(param)
}

func (cloud *MultiCloud) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	requestId := requestId()
	err := cloud.writeIntentLog(requestId, "DeleteBlob", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(requestId, "DeleteBlob")

	_, backend, err := cloud.getBlogBackend(param.Key)
	if err != nil {
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, requestId)
		}
		return nil, asAwsError(err)
	}
	return backend.DeleteBlob(param)
}

func (cloud *MultiCloud) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	requestId := requestId()

	err := cloud.writeIntentLog(requestId, "DeleteBlobs", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(requestId, "DeleteBlobs")

	itemsMap := make(map[string][]string)
	for _, item := range param.Items {
		name, _, err := cloud.getBlogBackend(item)
		if err != nil {
			if err == syscall.ENOENT {
				return nil, asAwsRequestError(err, requestId)
			}
			return nil, asAwsError(err)
		}
		if l, ok := itemsMap[name]; ok {
			itemsMap[name] = append(l, item)
		} else {
			itemsMap[name] = []string{item}
		}
	}

	for name, items := range itemsMap {
		backend, _ := cloud.backends[name]
		input := &DeleteBlobsInput{Items: items}
		_, err := backend.DeleteBlobs(input)
		if err != nil {
			if err == syscall.ENOENT {
				return nil, asAwsRequestError(err, requestId)
			}
			return nil, asAwsError(err)
		}
	}

	return &DeleteBlobsOutput{RequestId: requestId}, nil
}

func (cloud *MultiCloud) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	requestId := requestId()

	err := cloud.writeIntentLog(requestId, "RenameBlob", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(requestId, "RenameBlob")

	name, backend, err := cloud.getBlogBackend(param.Source)
	if err != nil {
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, requestId)
		}
		return nil, asAwsError(err)
	}

	if name2, backend2, err := cloud.getBlogBackend(param.Destination); err == nil {
		if name2 != name {
			_, err := backend2.DeleteBlob(&DeleteBlobInput{Key: param.Destination})
			if err == nil {
				if err == syscall.ENOENT {
					return nil, asAwsRequestError(err, requestId)
				}
				return nil, asAwsError(err)
			}
		}
	}

	cloud.kv.Delete(context.Background(), cloud.kvBlobKey(param.Source), nil)

	output, err := backend.RenameBlob(param)
	if err != nil {
		return nil, err
	}

	cloud.kvPut(cloud.kvBlobKey(param.Destination), &name)
	return output, nil
}

func (cloud *MultiCloud) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	requestId := requestId()

	err := cloud.writeIntentLog(requestId, "CopyBlob", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(requestId, "CopyBlob")

	name, backend, err := cloud.getBlogBackend(param.Source)
	if err != nil {
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, requestId)
		} else {
			return nil, asAwsError(err)
		}
	}

	if name2, backend2, err := cloud.getBlogBackend(param.Destination); err == nil {
		if name2 != name {
			_, err := backend2.DeleteBlob(&DeleteBlobInput{Key: param.Destination})
			if err == nil {
				if err == syscall.ENOENT {
					return nil, asAwsRequestError(err, requestId)
				} else {
					return nil, asAwsError(err)
				}
			}
		}
	}

	cloud.kvPut(cloud.kvBlobKey(param.Destination), &name)
	return backend.CopyBlob(param)
}

func (cloud *MultiCloud) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	requestId := requestId()
	_, backend, err := cloud.getBlogBackend(param.Key)
	if err != nil {
		if err == syscall.ENOENT {
			return nil, asAwsRequestError(err, requestId)
		}
		return nil, asAwsError(err)
	}
	return backend.GetBlob(param)
}

func (cloud *MultiCloud) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	mlog.Debugf("PutBlob %+v", param)
	name, backend, err := cloud.selectBackend(param.Key)
	if err != nil {
		mlog.Debugf("error %+v", err)
		return nil, err
	}
	output, err := backend.PutBlob(param)
	if err != nil {
		mlog.Debugf("error %+v", err)
		return nil, err
	}
	blobInfo := BlobInfo{}
	blobInfo.Size = param.Size
	blobInfo.Etag = output.ETag
	blobInfo.StorageClass = output.StorageClass

	now := time.Now()
	blobInfo.LastModified = &now
	blobInfo.Node = name

	key := cloud.kvBlobKey(param.Key)
	err = cloud.kvPut(key, &blobInfo)
	if err != nil {
		mlog.Debugf("error %+v", err)
		return nil, err
	}
	return output, nil
}

func (cloud *MultiCloud) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	mlog.Debugf("MultipartBlobBegin %+v", param)
	name, backend, err := cloud.selectBackend(param.Key)
	if err != nil {
		return nil, err
	}

	output, err := backend.MultipartBlobBegin(param)
	err = cloud.writeMultiPartNodeName(*output.UploadId, name)
	if err != nil {
		backend.MultipartBlobAbort(output)
		return nil, err
	}

	return output, err
}

func (cloud *MultiCloud) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	name, err := cloud.getMultiPartNodeName(*param.Commit.UploadId)
	if err != nil {
		return nil, err
	}
	backend, _ := cloud.backends[name]
	return backend.MultipartBlobAdd(param)
}

func (cloud *MultiCloud) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	name, err := cloud.getMultiPartNodeName(*param.UploadId)
	if err != nil {
		return nil, err
	}
	backend, _ := cloud.backends[name]

	return backend.MultipartBlobAbort(param)
}

func (cloud *MultiCloud) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	name, err := cloud.getMultiPartNodeName(*param.UploadId)
	if err != nil {
		return nil, err
	}
	backend, _ := cloud.backends[name]

	output, err := backend.MultipartBlobCommit(param)
	if err != nil {
		return nil, err
	}

	blobInfo := &BlobInfo{}
	blobInfo.Etag = output.ETag
	blobInfo.Node = name

	key := cloud.kvBlobKey(*param.Key)
	cloud.kvPut(key, &blobInfo)

	return output, nil
}

func (cloud *MultiCloud) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	for _, backend := range cloud.backends {
		backend.MultipartExpire(param)
	}

	return &MultipartExpireOutput{}, nil
}

func (cloud *MultiCloud) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	mlog.Debugf("RemoveBucket %+v", param)
	requestId := requestId()
	err := cloud.writeIntentLog(requestId, "RemoveBucket", param)
	if err != nil {
		return nil, err
	}
	defer cloud.deleteIntentLog(requestId, "RemoveBucket")

	for _, backend := range cloud.backends {
		backend.RemoveBucket(param)
	}
	return &RemoveBucketOutput{RequestId: requestId}, nil
}

func (cloud *MultiCloud) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	mlog.Debugf("MakeBucket %+v", param)
	requestId := requestId()
	err := cloud.writeIntentLog(requestId, "CreateBucket", param)
	if err != nil {
		return nil, err
	}
	for _, backend := range cloud.backends {
		backend.MakeBucket(param)
	}
	return &MakeBucketOutput{RequestId: requestId}, nil
}
