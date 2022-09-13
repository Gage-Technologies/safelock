package safelock

import (
	"bytes"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRedisLock(t *testing.T) {
	filename := "/tmp/file1.txt"
	//lockfile := filename + DefaultSuffix

	// create redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
		DB:   12,
	})

	l := NewRedisLock(0, filename, rdb)

	errLock := l.Lock()
	assert.NoError(t, errLock)

	// Verify the contents of the lock file
	data, err := rdb.Get(context.TODO(), l.GetLockFilename()).Bytes()
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(
		bytes.Join(bytes.Split(l.GetLockBody(), []byte("__::__"))[:2], []byte("__::__")),
		bytes.Join(bytes.Split(data, []byte("__::__"))[:2], []byte("__::__")),
	))

	errUnlock := l.Unlock()
	assert.NoError(t, errUnlock)

	nodeCreation := time.Unix(0, int64(l.GetID()))
	fmt.Println(time.Since(nodeCreation))
	assert.True(t, time.Since(nodeCreation) < time.Second)

	lockState, errGetLockState := l.GetLockState()
	assert.NoError(t, errGetLockState)
	assert.Equal(t, LockStateUnlocked, lockState)

	// File Info
	assert.Equal(t, filename, l.GetFilename())

	// Wait
	errWaitForLock := l.WaitForLock(DefaultTimeout)
	assert.NoError(t, errWaitForLock)
}

func TestRedisLockLockErrors(t *testing.T) {
	// create redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
		DB:   12,
	})

	filename := "/tmp/file2.txt"

	l := NewRedisLock(0, filename, rdb)

	rdb.Set(context.TODO(), l.GetLockFilename(), uuid.New().String(), 0)

	errLock := l.Lock()
	assert.Error(t, errLock)
}

func TestRedisLockUnlockErrors(t *testing.T) {
	filename := "/tmp/file3.txt"

	// create redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
		DB:   12,
	})

	l := NewRedisLock(0, filename, rdb)

	errUnlock := l.Unlock()
	assert.Error(t, errUnlock)

	rdb.Set(context.TODO(), l.GetLockFilename(), uuid.New().String(), 0)

	errUnlock = l.Unlock()
	assert.Error(t, errUnlock)
}

func TestRedisLockWait(t *testing.T) {
	// create redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
		DB:   12,
	})

	filename := "/tmp/file4.txt"

	l := NewRedisLock(0, filename, rdb)

	errLock := l.Lock()
	assert.NoError(t, errLock)

	// Spin this off in a goroutine so that we can manipulate the lock
	go func() {
		errWaitForLock := l.WaitForLock(DefaultTimeout)
		assert.NoError(t, errWaitForLock)
	}()

	// Wait long enough for code to loop
	time.Sleep(500 * time.Millisecond)
	errUnlock := l.Unlock()
	assert.NoError(t, errUnlock)
}

func TestRedisLockWaitError(t *testing.T) {
	// create redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
		DB:   12,
	})

	filename := "/tmp/file5.txt"

	l := NewRedisLock(0, filename, rdb)

	// Timeout as fast as possible
	l.SetTimeout(1 * time.Nanosecond)

	errWaitForLock := l.WaitForLock(1 * time.Nanosecond)
	assert.Error(t, errWaitForLock)
	assert.Equal(t, "unable to obtain lock after 1ns: context deadline exceeded", errWaitForLock.Error())
}

func TestRedisLockDeadlockRepair(t *testing.T) {
	// create redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
		DB:   12,
	})

	filename := "/tmp/file6.txt"

	// create lock with node 0
	l := NewRedisLock(0, filename, rdb)

	err := l.Lock()
	assert.NoError(t, err)

	// create a new session with node 1 leaving the old lock on
	l = NewRedisLock(1, filename, rdb)

	// attempt to unlock the prior sessions lock
	err = l.Unlock()
	assert.Error(t, err)

	// create a new session with node 0 leaving the old lock on
	l = NewRedisLock(0, filename, rdb)

	// attempt to unlock the prior sessions lock
	err = l.Unlock()
	assert.NoError(t, err)

	err = l.Lock()
	assert.NoError(t, err)

	// create a new session with node 0 leaving the old lock on
	l = NewRedisLock(0, filename, rdb)

	// attempt to lock on top of the old session
	err = l.Lock()
	assert.NoError(t, err)

	// create a new session with node 1 leaving the old lock on
	l = NewRedisLock(1, filename, rdb)

	// attempt to lock on top of the old session
	err = l.Lock()
	assert.Error(t, err)

	// create a new session with node 0 leaving the old lock on
	l = NewRedisLock(0, filename, rdb)

	err = l.Unlock()
	assert.NoError(t, err)

	// create lock with node 0
	l0 := NewRedisLock(0, filename, rdb)

	err = l0.Lock()
	assert.NoError(t, err)

	l1 := NewRedisLock(1, filename, rdb)
	l1.SetTimeout(time.Second * 2)

	err = l1.Lock()
	assert.Error(t, err)

	err = l1.Unlock()
	assert.Error(t, err)

	time.Sleep(time.Second * 2)

	err = l1.Lock()
	assert.NoError(t, err)

	err = l1.Unlock()
	assert.NoError(t, err)
}

func TestRedisLock_ForceUnlock(t *testing.T) {
	// create redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
		DB:   12,
	})

	filename := "/tmp/file7.txt"

	// create lock with node 0
	l := NewRedisLock(0, filename, rdb)

	err := l.Lock()
	assert.NoError(t, err)

	// create a new session with node 1 leaving the old lock on
	l = NewRedisLock(1, filename, rdb)

	// attempt to unlock the prior sessions lock
	err = l.Unlock()
	assert.Error(t, err)

	// attempt to unlock the prior sessions lock
	err = l.ForceUnlock()
	assert.NoError(t, err)
}
