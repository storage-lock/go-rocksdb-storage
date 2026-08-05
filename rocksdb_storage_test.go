package rocksdb_storage

import (
	"context"
	"os"
	"testing"

	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
)

func TestRocksDBStorage(t *testing.T) {
	path := os.Getenv("STORAGE_LOCK_ROCKSDB_PATH")
	if path == "" {
		path = "/tmp/storage-lock-rocksdb-test"
	}

	ctx := context.Background()
	cm := NewRocksDBConnectionManager(path)
	s, err := NewRocksDBStorage(ctx, &RocksDBStorageOptions{
		ConnectionManager: cm,
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)

	// 清理
	os.RemoveAll(path)
}
