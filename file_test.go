package safelock

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestFileLock(t *testing.T) {
	filename := "/tmp/file1.txt"
	lockfile := filename + DefaultSuffix

	l := NewFileLock(0, filename)

	errLock := l.Lock()
	assert.NoError(t, errLock)

	// Verify the contents of the lock file
	aFile, errOpen := os.Open(lockfile)
	assert.NoError(t, errOpen)
	defer aFile.Close()

	data, errReadAll := ioutil.ReadAll(aFile)
	assert.NoError(t, errReadAll)
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

func TestFileLockLockErrors(t *testing.T) {
	filename := "/tmp/file2.txt"
	lockfile := filename + DefaultSuffix

	aFile, errOpen := os.OpenFile(lockfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.NoError(t, errOpen)
	defer aFile.Close()

	_, errWrite := aFile.Write([]byte(uuid.New().String()))
	assert.NoError(t, errWrite)

	l := NewFileLock(0, filename)

	errLock := l.Lock()
	assert.Error(t, errLock)
}

func TestFileLockUnlockErrors(t *testing.T) {
	filename := "/tmp/file3.txt"
	lockfile := filename + DefaultSuffix

	l := NewFileLock(0, filename)

	errUnlock := l.Unlock()
	assert.Error(t, errUnlock)

	// Indicate that the file exists
	aFile, errOpen := os.OpenFile(lockfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.NoError(t, errOpen)
	defer aFile.Close()

	// Set the contents of the file to a UUID that is not the same as the lock
	_, errWrite := aFile.Write([]byte(uuid.New().String()))
	assert.NoError(t, errWrite)

	errUnlock = l.Unlock()
	assert.Error(t, errUnlock)
}

func TestFileLockWait(t *testing.T) {
	filename := "/tmp/file4.txt"

	l := NewFileLock(0, filename)

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

func TestFileLockWaitError(t *testing.T) {
	filename := "/tmp/file5.txt"

	l := NewFileLock(0, filename)

	// Timeout as fast as possible
	l.SetTimeout(1 * time.Nanosecond)

	errWaitForLock := l.WaitForLock(1 * time.Nanosecond)
	assert.Error(t, errWaitForLock)
	assert.Equal(t, "unable to obtain lock after 1ns: context deadline exceeded", errWaitForLock.Error())
}

func TestFileLockDeadlockRepair(t *testing.T) {
	filename := "/tmp/file6.txt"

	// create lock with node 0
	l := NewFileLock(0, filename)

	err := l.Lock()
	assert.NoError(t, err)

	// create a new session with node 1 leaving the old lock on
	l = NewFileLock(1, filename)

	// attempt to unlock the prior sessions lock
	err = l.Unlock()
	assert.Error(t, err)

	// create a new session with node 0 leaving the old lock on
	l = NewFileLock(0, filename)

	// attempt to unlock the prior sessions lock
	err = l.Unlock()
	assert.NoError(t, err)

	err = l.Lock()
	assert.NoError(t, err)

	// create a new session with node 0 leaving the old lock on
	l = NewFileLock(0, filename)

	// attempt to lock on top of the old session
	err = l.Lock()
	assert.NoError(t, err)

	// create a new session with node 1 leaving the old lock on
	l = NewFileLock(1, filename)

	// attempt to lock on top of the old session
	err = l.Lock()
	assert.Error(t, err)

	// create a new session with node 0 leaving the old lock on
	l = NewFileLock(0, filename)

	err = l.Unlock()
	assert.NoError(t, err)

	// create lock with node 0
	l0 := NewFileLock(0, filename)

	err = l0.Lock()
	assert.NoError(t, err)

	l1 := NewFileLock(1, filename)
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

func TestFileLock_ForceUnlock(t *testing.T) {
	filename := "/tmp/file7.txt"

	// create lock with node 0
	l := NewFileLock(0, filename)

	err := l.Lock()
	assert.NoError(t, err)

	// create a new session with node 1 leaving the old lock on
	l = NewFileLock(1, filename)

	// attempt to unlock the prior sessions lock
	err = l.Unlock()
	assert.Error(t, err)

	// attempt to unlock the prior sessions lock
	err = l.ForceUnlock()
	assert.NoError(t, err)
}
