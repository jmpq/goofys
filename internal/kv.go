package internal

import (
	"context"
	"strings"
	"syscall"
	"time"

	"go.etcd.io/etcd/clientv3"
)

type KVPair struct {
	Key   string
	Value []byte
}

type KVDB interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Scan(prefix *string, start *string, limit int) ([]KVPair, error)
}

type EtcdDB struct {
	client *clientv3.Client
}

func NewEtcdDB(endpoints string, secure bool) (*EtcdDB, error) {
	cfg := clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	db := &EtcdDB{
		client: c,
	}
	return db, err
}

func (db *EtcdDB) Set(key string, value []byte) error {
	_, err := db.client.Put(context.TODO(), key, string(value))
	return err
}

func (db *EtcdDB) Get(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := db.client.Get(ctx, key)
	defer cancel()
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, syscall.ENOENT
	}

	return resp.Kvs[0].Value, nil

}

func (db *EtcdDB) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := db.client.Delete(ctx, key)
	return err
}

func (db *EtcdDB) Scan(prefix *string, start *string, limit int) ([]KVPair, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var pair []KVPair
	var resp *clientv3.GetResponse
	var err error

	if start != nil {
		resp, err = db.client.Get(ctx, *start,
			clientv3.WithFromKey(),
			clientv3.WithLimit(int64(limit)),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	} else if prefix != nil {
		resp, err = db.client.Get(ctx, *prefix,
			clientv3.WithPrefix(),
			clientv3.WithLimit(int64(limit)),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	} else {
		resp, err = db.client.Get(ctx, "",
			clientv3.WithPrefix(),
			clientv3.WithLimit(int64(limit)),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	}

	if err != nil {
		return nil, err
	}

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if prefix != nil {
			if !strings.HasPrefix(key, *prefix) {
				continue
			}
		}
		pair = append(pair, KVPair{Key: key, Value: kv.Value})
	}

	return pair, nil
}
