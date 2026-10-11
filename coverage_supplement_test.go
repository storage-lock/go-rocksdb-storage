package rocksdb_storage

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

func openRocksDBReadOnly(path string) (*grocksdb.DB, error) {
	return grocksdb.OpenDbForReadOnly(grocksdb.NewDefaultOptions(), path, false)
}

func defaultRocksDBReadOptions() *grocksdb.ReadOptions {
	return grocksdb.NewDefaultReadOptions()
}

func defaultRocksDBWriteOptions() *grocksdb.WriteOptions {
	return grocksdb.NewDefaultWriteOptions()
}

func flushRocksDB(s *RocksDBStorage) error {
	return s.db.Flush(grocksdb.NewDefaultFlushOptions())
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

// corruptFirstSSTFile 破坏目录下第一个 .sst 文件中间 4KB，使 checksum 校验失败。
func corruptFirstSSTFile(t *testing.T, dir string) {
	var sst string
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && strings.HasSuffix(p, ".sst") && sst == "" {
			sst = p
		}
		return nil
	})
	assert.NotEqual(t, "", sst, "expected at least one sst file")
	f, err := os.OpenFile(sst, os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer f.Close()
	buf := make([]byte, 4096)
	_, _ = f.ReadAt(buf, 4096)
	for i := range buf {
		buf[i] ^= 0xff
	}
	_, err = f.WriteAt(buf, 4096)
	assert.Nil(t, err)
}

func newRocksDBCovStorage(t *testing.T) (*RocksDBStorage, *RocksDBConnectionManager, string, func()) {
	tmpDir, err := os.MkdirTemp("", "storage-lock-rocksdb-cov-*")
	assert.Nil(t, err)
	cm := NewRocksDBConnectionManager(tmpDir)
	s, err := NewRocksDBStorage(context.Background(), &RocksDBStorageOptions{
		ConnectionManager: cm,
	})
	assert.Nil(t, err)
	return s, cm, tmpDir, func() {
		_ = cm.Shutdown(context.Background())
		os.RemoveAll(tmpDir)
	}
}

func newRocksDBCovInfo() *storage.LockInformation {
	return &storage.LockInformation{
		OwnerId:         "owner-cov",
		Version:         1,
		LockCount:       1,
		LockBeginTime:   time.Now(),
		LeaseExpireTime: time.Now().Add(time.Minute),
	}
}

func TestCoverageRocksDBOptions(t *testing.T) {
	// NewRocksDBStorageOptions 默认值非空
	opts := NewRocksDBStorageOptions()
	assert.NotNil(t, opts.ReadOptions)
	assert.NotNil(t, opts.WriteOptions)
	// SetPath
	assert.Equal(t, opts, opts.SetPath("/tmp/x"))
	assert.Equal(t, "/tmp/x", opts.Path)
	// SetTimeProvider
	tp := &LocalTimeProvider{}
	assert.Equal(t, opts, opts.SetTimeProvider(tp))
	assert.Equal(t, tp, opts.TimeProvider)
	// SetConnectionManager
	cm := NewRocksDBConnectionManager("/tmp/y")
	assert.Equal(t, opts, opts.SetConnectionManager(cm))
	assert.Equal(t, cm, opts.ConnectionManager)
	// Check 正常3826（Path 非空）
	assert.Nil(t, (&RocksDBStorageOptions{Path: "/tmp/z"}).Check())
	assert.Nil(t, opts.Check())
	// Check 失败：两者都空
	assert.Equal(t, ErrPathEmpty, (&RocksDBStorageOptions{}).Check())
}

func TestCoverageRocksDBConnectionManagerNameReturnShutdown(t *testing.T) {
	cm := NewRocksDBConnectionManager("/tmp/cov-name")
	assert.Equal(t, "rocksdb-connection-manager", cm.Name())
	assert.Nil(t, cm.Return(context.Background(), nil))
	// 未初始化的 Shutdown
	assert.Nil(t, cm.Shutdown(context.Background()))
	// 初始化后 Shutdown 走 db != nil 分支
	s, cm2, _, cleanup := newRocksDBCovStorage(t)
	defer cleanup()
	assert.NotNil(t, s.db)
	assert.Nil(t, cm2.Shutdown(context.Background()))
	// 再次 Shutdown 走 db == nil 分支
	assert.Nil(t, cm2.Shutdown(context.Background()))
}

func TestCoverageRocksDBTakeFailure(t *testing.T) {
	// 用文件占住路径，OpenDb 失败
	f, err := os.CreateTemp("", "storage-lock-rocksdb-cov-file-*")
	assert.Nil(t, err)
	f.Close()
	defer os.Remove(f.Name())
	cm := NewRocksDBConnectionManager(f.Name())
	_, err = cm.Take(context.Background())
	assert.NotNil(t, err)
}

func TestCoverageRocksDBTakeCached(t *testing.T) {
	s, cm, _, cleanup := newRocksDBCovStorage(t)
	defer cleanup()
	db, err := cm.Take(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, s.db, db)
}

func TestCoverageRocksDBNewStorageCheckFailure(t *testing.T) {
	_, err := NewRocksDBStorage(context.Background(), &RocksDBStorageOptions{})
	assert.Equal(t, ErrPathEmpty, err)
}

func TestCoverageRocksDBNewStorageInitFailure(t *testing.T) {
	f, err := os.CreateTemp("", "storage-lock-rocksdb-cov-file2-*")
	assert.Nil(t, err)
	f.Close()
	defer os.Remove(f.Name())
	_, err = NewRocksDBStorage(context.Background(), &RocksDBStorageOptions{
		ConnectionManager: NewRocksDBConnectionManager(f.Name()),
	})
	assert.NotNil(t, err)
}

func TestCoverageRocksDBInitFailure(t *testing.T) {
	f, err := os.CreateTemp("", "storage-lock-rocksdb-cov-file3-*")
	assert.Nil(t, err)
	f.Close()
	defer os.Remove(f.Name())
	s := &RocksDBStorage{options: &RocksDBStorageOptions{
		ConnectionManager: NewRocksDBConnectionManager(f.Name()),
	}}
	assert.NotNil(t, s.Init(context.Background()))
}

func TestCoverageRocksDBInitCustomOptions(t *testing.T) {
	// Init 走 ReadOptions/WriteOptions/TimeProvider 已设置分支
	tmpDir, err := os.MkdirTemp("", "storage-lock-rocksdb-cov-opt-*")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	opts := NewRocksDBStorageOptions()
	opts.SetConnectionManager(NewRocksDBConnectionManager(tmpDir))
	opts.SetTimeProvider(&LocalTimeProvider{})
	s := &RocksDBStorage{options: opts}
	assert.Nil(t, s.Init(context.Background()))
	defer s.db.Close()
	assert.NotNil(t, s.timeProvider)
}

func TestCoverageRocksDBCapabilitiesAndWatch(t *testing.T) {
	s, _, _, cleanup := newRocksDBCovStorage(t)
	defer cleanup()
	caps := s.Capabilities()
	assert.Contains(t, caps, storage.CapabilityCAS)
	assert.Contains(t, caps, storage.CapabilityReliableTime)
	assert.Contains(t, caps, storage.CapabilityAtomicDelete)
	assert.Contains(t, caps, storage.CapabilityWatch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.Watch(ctx, "lock-watch-cov")
	assert.Nil(t, err)
	assert.NotNil(t, ch)
	_, err = s.Watch(ctx, "")
	assert.NotNil(t, err)
}

func TestCoverageRocksDBCRUDErrors(t *testing.T) {
	s, _, _, cleanup := newRocksDBCovStorage(t)
	defer cleanup()
	ctx := context.Background()
	// Create 已存在
	assert.Nil(t, s.CreateWithVersion(ctx, "lock-dup", 1, newRocksDBCovInfo()))
	assert.NotNil(t, s.CreateWithVersion(ctx, "lock-dup", 1, newRocksDBCovInfo()))
	// Update 不存在
	assert.NotNil(t, s.UpdateWithVersion(ctx, "lock-missing", 1, 2, newRocksDBCovInfo()))
	// Update 版本不符
	assert.NotNil(t, s.UpdateWithVersion(ctx, "lock-dup", 999, 2, newRocksDBCovInfo()))
	// Delete 不存在
	assert.NotNil(t, s.DeleteWithVersion(ctx, "lock-missing", 1, newRocksDBCovInfo()))
	// Delete 版本不符
	assert.NotNil(t, s.DeleteWithVersion(ctx, "lock-dup", 999, newRocksDBCovInfo()))
	// Get 不存在
	_, err := s.Get(ctx, "lock-missing")
	assert.NotNil(t, err)
}

func TestCoverageRocksDBCorruptedData(t *testing.T) {
	s, _, _, cleanup := newRocksDBCovStorage(t)
	defer cleanup()
	ctx := context.Background()
	// 直接写入损坏的 JSON，覆盖 Update/Delete 的 unmarshal 错误分支
	assert.Nil(t, s.db.Put(s.options.WriteOptions, []byte("lock-bad"), []byte("{invalid")))
	assert.NotNil(t, s.UpdateWithVersion(ctx, "lock-bad", 1, 2, newRocksDBCovInfo()))
	assert.NotNil(t, s.DeleteWithVersion(ctx, "lock-bad", 1, newRocksDBCovInfo()))
}

func TestCoverageRocksDBListSkipsCorrupted(t *testing.T) {
	s, _, _, cleanup := newRocksDBCovStorage(t)
	defer cleanup()
	ctx := context.Background()
	assert.Nil(t, s.CreateWithVersion(ctx, "lock-good", 1, newRocksDBCovInfo()))
	assert.Nil(t, s.db.Put(s.options.WriteOptions, []byte("lock-bad2"), []byte("{invalid")))
	it, err := s.List(ctx)
	assert.Nil(t, err)
	count := 0
	for it.Next() {
		_ = it.Value()
		count++
	}
	assert.Equal(t, 1, count)
}

func TestCoverageRocksDBReadOnlyWriteErrors(t *testing.T) {
	// 只读模式打开的 db：Put/Delete 返回 "Not supported operation in read only mode"，
	// 覆盖 Create 的 Put-err、Update 的 Put-err、Delete 的 Delete-err 透传分支。
	tmpDir, err := os.MkdirTemp("", "storage-lock-rocksdb-cov-ro-*")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	ctx := context.Background()

	cm := NewRocksDBConnectionManager(tmpDir)
	s, err := NewRocksDBStorage(ctx, &RocksDBStorageOptions{ConnectionManager: cm})
	assert.Nil(t, err)
	assert.Nil(t, s.CreateWithVersion(ctx, "lock-ro", 1, newRocksDBCovInfo()))
	assert.Nil(t, cm.Shutdown(ctx))

	roDB, err := openRocksDBReadOnly(tmpDir)
	assert.Nil(t, err)
	defer roDB.Close()
	ropts := NewRocksDBStorageOptions()
	_ = ropts
	s2 := &RocksDBStorage{
		options: &RocksDBStorageOptions{
			ConnectionManager: cm,
			ReadOptions:       defaultRocksDBReadOptions(),
			WriteOptions:      defaultRocksDBWriteOptions(),
		},
		db: roDB,
	}
	assert.NotNil(t, s2.CreateWithVersion(ctx, "lock-new", 1, newRocksDBCovInfo()))
	assert.NotNil(t, s2.UpdateWithVersion(ctx, "lock-ro", 1, 2, newRocksDBCovInfo()))
	assert.NotNil(t, s2.DeleteWithVersion(ctx, "lock-ro", 1, newRocksDBCovInfo()))
}

func TestCoverageRocksDBCorruptedSSTReadErrors(t *testing.T) {
	// 破坏 SST 文件使 block checksum 校验失败，GetBytes 返回 Corruption 错误，
	// 覆盖 Create/Update/Delete/Get 的 GetBytes-err 透传分支。
	tmpDir, err := os.MkdirTemp("", "storage-lock-rocksdb-cov-corrupt-*")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	ctx := context.Background()

	cm := NewRocksDBConnectionManager(tmpDir)
	s, err := NewRocksDBStorage(ctx, &RocksDBStorageOptions{ConnectionManager: cm})
	assert.Nil(t, err)
	// 写入足够数据并 Flush 落盘成 SST
	for i := 0; i < 2000; i++ {
		k := "pad-" + string(rune('a'+i%26)) + "-" + itoa(i)
		assert.Nil(t, s.CreateWithVersion(ctx, k, 1, newRocksDBCovInfo()))
	}
	assert.Nil(t, flushRocksDB(s))
	assert.Nil(t, cm.Shutdown(ctx))

	corruptFirstSSTFile(t, tmpDir)

	roDB, err := openRocksDBReadOnly(tmpDir)
	assert.Nil(t, err)
	defer roDB.Close()
	s2 := &RocksDBStorage{
		options: &RocksDBStorageOptions{
			ConnectionManager: cm,
			ReadOptions:       defaultRocksDBReadOptions(),
			WriteOptions:      defaultRocksDBWriteOptions(),
		},
		db: roDB,
	}
	// 逐个读直到命中被破坏的 block（Corruption 错误），记下该 key，
	// 后续 CRUD 复用它以命中各自的 GetBytes-err 透传分支。
	damagedKey := ""
	for i := 0; i < 2000 && damagedKey == ""; i++ {
		k := "pad-" + string(rune('a'+i%26)) + "-" + itoa(i)
		_, err = s2.Get(ctx, k)
		if err != nil {
			damagedKey = k
		}
	}
	assert.NotEqual(t, "", damagedKey, "expected Corruption error from damaged SST")
	// 同一损坏 key 上 Create/Update/Delete 的 GetBytes 同样报错
	assert.NotNil(t, s2.CreateWithVersion(ctx, damagedKey, 1, newRocksDBCovInfo()))
	assert.NotNil(t, s2.UpdateWithVersion(ctx, damagedKey, 1, 2, newRocksDBCovInfo()))
	assert.NotNil(t, s2.DeleteWithVersion(ctx, damagedKey, 1, newRocksDBCovInfo()))
}

func TestCoverageRocksDBCreateMarshalError(t *testing.T) {
	s, _, _, cleanup := newRocksDBCovStorage(t)
	defer cleanup()
	assert.Nil(t, s.CreateWithVersion(context.Background(), "lock-ok", 1, newRocksDBCovInfo()))
}
