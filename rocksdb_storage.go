package rocksdb_storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// RocksDBStorage 基于 RocksDB 的存储引擎实现
// RocksDB 是嵌入式键值存储，不支持服务端原子 CAS，
// 需要使用互斥锁在进程内实现 CAS 语义
type RocksDBStorage struct {
	options      *RocksDBStorageOptions
	db           *grocksdb.DB
	timeProvider storage.TimeProvider
	// mutex 用于在进程内实现 CAS 语义
	// RocksDB 本身不支持原子条件写，需要加锁保护
	mu sync.RWMutex
}

var _ storage.Storage = &RocksDBStorage{}

func NewRocksDBStorage(ctx context.Context, options *RocksDBStorageOptions) (*RocksDBStorage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &RocksDBStorage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const StorageName = "rocksdb-storage"

func (x *RocksDBStorage) GetName() string {
	return StorageName
}

// Capabilities 声明 RocksDB 存储支持的能力
// RocksDB 通过进程内 mutex 实现 CAS，不支持 AtomicDelete
func (x *RocksDBStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
	}
}

func (x *RocksDBStorage) Init(ctx context.Context) error {
	db, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.db = db

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// CreateWithVersion 原子创建锁记录
// RocksDB 不支持原子条件写，使用 mutex 保护
func (x *RocksDBStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	// 检查锁是否已存在
	key := []byte(lockId)
	value, err := x.db.GetBytes(x.options.ReadOptions, key)
	if err != nil {
		return err
	}

	// 如果锁已存在，返回 ErrLockAlreadyExists
	if value != nil {
		return storage_lock.ErrLockAlreadyExists
	}

	// 写入新锁记录
	data, err := json.Marshal(lockInformation)
	if err != nil {
		return err
	}

	return x.db.Put(x.options.WriteOptions, key, data)
}

// UpdateWithVersion 原子更新锁记录
// RocksDB 不支持原子条件写，使用 mutex 保护
func (x *RocksDBStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	key := []byte(lockId)
	value, err := x.db.GetBytes(x.options.ReadOptions, key)
	if err != nil {
		return err
	}

	// 如果锁不存在，返回 ErrLockNotFound
	if value == nil {
		return storage_lock.ErrLockNotFound
	}

	// 反序列化当前锁信息
	var currentInfo storage.LockInformation
	if err := json.Unmarshal(value, &currentInfo); err != nil {
		return err
	}

	// 检查版本号
	if currentInfo.Version != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 写入更新后的锁记录
	data, err := json.Marshal(lockInformation)
	if err != nil {
		return err
	}

	return x.db.Put(x.options.WriteOptions, key, data)
}

// DeleteWithVersion 原子删除锁记录
// RocksDB 不支持原子条件删除，使用 mutex 保护
func (x *RocksDBStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	key := []byte(lockId)
	value, err := x.db.GetBytes(x.options.ReadOptions, key)
	if err != nil {
		return err
	}

	// 如果锁不存在，返回 ErrLockNotFound
	if value == nil {
		return storage_lock.ErrLockNotFound
	}

	// 反序列化当前锁信息
	var currentInfo storage.LockInformation
	if err := json.Unmarshal(value, &currentInfo); err != nil {
		return err
	}

	// 检查版本号
	if currentInfo.Version != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 删除锁记录
	return x.db.Delete(x.options.WriteOptions, key)
}

// Get 获取锁记录
func (x *RocksDBStorage) Get(ctx context.Context, lockId string) (string, error) {
	x.mu.RLock()
	defer x.mu.RUnlock()

	key := []byte(lockId)
	value, err := x.db.GetBytes(x.options.ReadOptions, key)
	if err != nil {
		return "", err
	}

	if value == nil {
		return "", storage_lock.ErrLockNotFound
	}

	return string(value), nil
}

func (x *RocksDBStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

func (x *RocksDBStorage) Close(ctx context.Context) error {
	if x.db != nil {
		x.db.Close()
	}
	return nil
}

// List 列出所有锁记录
func (x *RocksDBStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	x.mu.RLock()
	defer x.mu.RUnlock()

	slice := make([]*storage.LockInformation, 0)

	iter := x.db.NewIterator(x.options.ReadOptions)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		value := iter.Value()
		var lockInfo storage.LockInformation
		if err := json.Unmarshal(value.Data(), &lockInfo); err != nil {
			continue
		}
		slice = append(slice, &lockInfo)
	}

	return go_iterator.FromSlice(slice), nil
}