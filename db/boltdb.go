package db

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/onokonem/sillyQueueServer/logiface"
	"github.com/onokonem/sillyQueueServer/tasks"
)

var (
	tasksBucket = []byte("tasks")
)

type boltDB struct {
	db     *bolt.DB
	closed bool
	saver  chan *tasks.Task

	sync.RWMutex
}

func newBoltDB(filePath string) (Connect, error) {
	db, err := bolt.Open(filePath, 0640, nil)
	if err != nil {
		return nil, err
	}

	return &boltDB{db: db, saver: make(chan *tasks.Task)}, nil
}

func (conn *boltDB) Saver() chan<- *tasks.Task {
	return conn.saver
}

func (conn *boltDB) SaverLoop(interval time.Duration, bunchSize int, logger logiface.Logger) {
	timer := time.NewTicker(interval)
	buf := make([]*tasks.Task, 0, bunchSize)
	semaphore := int32(0)

	for !conn.isClosed() {
		select {
		case task := <-conn.saver:
			buf = append(buf, task)
			if len(buf) >= bunchSize {
				logger.Debug("DB cache flushing by size", "size", len(buf))
				flushBufOneATime(conn, buf, &semaphore, logger)
				buf = make([]*tasks.Task, 0, bunchSize)
			}
		case _ = <-timer.C:
			if len(buf) > 0 {
				logger.Debug("DB cache flushing by time", "size", len(buf))
				flushBufOneATime(conn, buf, &semaphore, logger)
				buf = make([]*tasks.Task, 0, bunchSize)
			}
		}
	}

	if len(buf) > 0 {
		err := conn.SaveTasks(buf...)
		if err != nil {
			panic(err)
		}
		logger.Debug("DB cache flushed on close", "size", len(buf))
	}
}

func flushBufOneATime(conn Connect, buf []*tasks.Task, semaphore *int32, logger logiface.Logger) {
	for !atomic.CompareAndSwapInt32(semaphore, 0, 1) {
		time.Sleep(time.Millisecond)
	}
	logger.Debug("DB cache flush started")

	go flushBuf(conn, buf, semaphore, logger)
}

func flushBuf(conn Connect, buf []*tasks.Task, semaphore *int32, logger logiface.Logger) {
	defer atomic.SwapInt32(semaphore, 0)

	err := conn.SaveTasks(buf...)
	if err != nil {
		panic(err)
	}

	logger.Debug("DB cache flush done")
}

func (conn *boltDB) SaveTasks(tasksList ...*tasks.Task) error {
	conn.RLock()
	defer conn.RUnlock()

	return conn.db.Update(
		func(tx *bolt.Tx) error {
			return upsertBunchTransaction(tx, tasksList...)
		},
	)
}

func (conn *boltDB) Close() error {
	conn.Lock()
	defer conn.Unlock()

	conn.closed = true
	return conn.db.Close()
}

func (conn *boltDB) MustClose() {
	conn.Lock()
	defer conn.Unlock()

	conn.closed = true
	err := conn.db.Close()
	if err != nil {
		panic(err)
	}
}

func (conn *boltDB) isClosed() bool {
	conn.RLock()
	defer conn.RUnlock()

	return conn.closed
}

func (conn *boltDB) String() string {
	return conn.db.String()
}

func (conn *boltDB) GetTask(id string) (*tasks.Task, error) {
	conn.RLock()
	defer conn.RUnlock()

	key := []byte(id)

	tx, err := conn.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer closeRO(tx)

	bucket := tx.Bucket(tasksBucket)
	if bucket == nil {
		return nil, nil
	}

	exists := bucket.Get(key)
	if exists == nil {
		return nil, nil
	}

	content, err := unmarshalPayload(key, exists)
	if err != nil {
		return nil, err
	}

	return content, nil
}

type dbValue struct {
	Data     []byte
	Checksum uint32
}

func (v *dbValue) Marshal() []byte {
	buf, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return buf
}

func unmarshalPayload(key []byte, val []byte) (*tasks.Task, error) {
	dbVal := dbValue{}

	err := json.Unmarshal(val, &dbVal)
	if err != nil {
		return nil, err
	}

	crc32 := crc32.ChecksumIEEE(append(dbVal.Data, key...))

	if crc32 != dbVal.Checksum {
		return nil, fmt.Errorf("Bad checksum")
	}

	task := tasks.Task{}

	err = json.Unmarshal(dbVal.Data, &task)
	if err != nil {
		return nil, err
	}

	return &task, nil
}

func marshalPayload(key []byte, task *tasks.Task) []byte {
	dbVal := dbValue{}

	dbVal.Data = task.Marshal()
	dbVal.Checksum = crc32.ChecksumIEEE(append(dbVal.Data, key...))

	return dbVal.Marshal()
}

func upsertBunchTransaction(tx *bolt.Tx, tasksList ...*tasks.Task) error {
	bucket, err := tx.CreateBucketIfNotExists(tasksBucket)
	if err != nil {
		return err
	}

	for _, task := range tasksList {
		key := []byte(task.ID())
		val := marshalPayload(key, task)

		if task.Status == tasks.StatusToRemove {
			err = bucket.Delete(key)
		} else {
			err = bucket.Put(key, val)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func unmarshalTask(data []byte) (*tasks.Task, error) {
	content := tasks.Task{}

	err := json.Unmarshal(data, &content)
	if err != nil {
		return nil, err
	}

	return &content, nil
}

func closeRO(tx *bolt.Tx) {
	err := tx.Rollback()
	if err != nil {
		panic(fmt.Errorf("Error closing RO transaction: %v", err))
	}
}

func (conn *boltDB) Foreach(f ForeachFunc) (int, error) {
	conn.RLock()
	defer conn.RUnlock()

	tx, err := conn.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer closeRO(tx)

	bucket := tx.Bucket(tasksBucket)
	if bucket == nil {
		return 0, nil
	}

	count := 0
	bucket.ForEach(
		func(key []byte, val []byte) error {
			taskID := string(key)

			task, err := unmarshalPayload(key, val)
			if err != nil {
				return err
			}

			task.SetID(taskID)

			err = f(task)
			if err != nil {
				return err
			}

			count++
			return nil
		},
	)

	return count, nil
}
