package rocksdb_storage

import (
	"context"
	"sync"

	"github.com/linxGnu/grocksdb"
	"github.com/storage-lock/go-storage"
)

// RocksDBStorageOptions RocksDB 存储选项
type RocksDBStorageOptions struct {
	Path          string
	TimeProvider  storage.TimeProvider
	ConnectionManager storage.ConnectionManager[*grocksdb.DB]

	ReadOptions  *grocksdb.ReadOptions
	WriteOptions *grocksdb.WriteOptions
}

func NewRocksDBStorageOptions() *RocksDBStorageOptions {
	return &RocksDBStorageOptions{
		ReadOptions:  grocksdb.NewDefaultReadOptions(),
		WriteOptions: grocksdb.NewDefaultWriteOptions(),
	}
}

func (x *RocksDBStorageOptions) SetPath(path string) *RocksDBStorageOptions {
	x.Path = path
	return x
}

func (x *RocksDBStorageOptions) SetTimeProvider(timeProvider storage.TimeProvider) *RocksDBStorageOptions {
	x.TimeProvider = timeProvider
	return x
}

func (x *RocksDBStorageOptions) SetConnectionManager(cm storage.ConnectionManager[*grocksdb.DB]) *RocksDBStorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *RocksDBStorageOptions) Check() error {
	if x.ConnectionManager == nil && x.Path == "" {
		return ErrPathEmpty
	}
	return nil
}

// RocksDBConnectionManager RocksDB 连接管理器
type RocksDBConnectionManager struct {
	Path string

	mu         sync.Mutex
	db         *grocksdb.DB
	initialized bool
}

var _ storage.ConnectionManager[*grocksdb.DB] = &RocksDBConnectionManager{}

func NewRocksDBConnectionManager(path string) *RocksDBConnectionManager {
	return &RocksDBConnectionManager{
		Path: path,
	}
}

func (x *RocksDBConnectionManager) Name() string {
	return "rocksdb-connection-manager"
}

func (x *RocksDBConnectionManager) Take(ctx context.Context) (*grocksdb.DB, error) {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.initialized {
		return x.db, nil
	}

	// 创建 RocksDB 选项
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	// 打开数据库
	db, err := grocksdb.OpenDb(opts, x.Path)
	if err != nil {
		return nil, err
	}

	x.db = db
	x.initialized = true
	return x.db, nil
}

func (x *RocksDBConnectionManager) Return(ctx context.Context, conn *grocksdb.DB) error {
	return nil
}

func (x *RocksDBConnectionManager) Shutdown(ctx context.Context) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.db != nil {
		x.db.Close()
		x.db = nil
	}
	x.initialized = false
	return nil
}