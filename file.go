package safelock

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/spf13/afero"
)

// FileLock will create a lock for a specific file
// As an example, if the URI is file:///filename.txt then the lock will be
// a file named file:///filename.txt.lock and the contents will be the lock's
// UUID.
type FileLock struct {
	*SafeLock

	filename string
	fs       afero.Fs
}

// NewFileLock creates a new instance of FileLock
func NewFileLock(node uint16, filename string, fs afero.Fs) *FileLock {
	return &FileLock{
		SafeLock: NewSafeLock(node),
		filename: filename,
		fs:       fs,
	}
}

// Lock will lock
func (l *FileLock) Lock() error {

	// Check first if the lock exists
	// For FileLock the error is never used and the state can only be locked/unlocked
	lockState, _ := l.GetLockState()
	if lockState == LockStateLocked {
		// conditionally handle deadlock if the lock exists and is owned by a prior session of the same node

		// check the ownership of the lock
		ownedNode, ownedSession, err := l.isSameLock()
		if err != nil {
			return fmt.Errorf("failed to check lock ownership: %v", err)
		}

		// release a deadlocked file lock
		if ownedNode && !ownedSession {
			l.mu.Lock()
			// remove file system lock
			err := l.fs.Remove(l.GetLockFilename())
			if err != nil {
				l.mu.Unlock()
				return err
			}
			l.mu.Unlock()
		} else {
			return fmt.Errorf("the object at %s is locked", l.GetFilename())
		}
	}

	// Lock after getting the lock state
	l.mu.Lock()
	defer l.mu.Unlock()

	// Write object to S3
	aFile, errOpen := l.fs.OpenFile(l.GetLockFilename(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if errOpen != nil {
		return fmt.Errorf("unable to open %q: %w", l.GetLockFilename(), errOpen)
	}
	defer aFile.Close()

	_, errWrite := aFile.Write(l.GetLockBody())
	if errWrite != nil {
		return fmt.Errorf("unable to write data to %q: %w", l.GetLockFilename(), errWrite)
	}
	return nil
}

// Unlock will unlock
func (l *FileLock) Unlock() error {

	// Check first if the lock exists
	// For FileLock the error is never used and the state can only be locked/unlocked
	lockState, _ := l.GetLockState()
	if lockState == LockStateUnlocked {
		return fmt.Errorf("the object at %s is not locked", l.GetFilename())
	}

	// Validate that the lock belongs to this code
	ownedNode, _, errIsSameLock := l.isSameLock()
	if errIsSameLock != nil {
		return fmt.Errorf("unable to determine if lock is the same lock: %w", errIsSameLock)
	}

	if !ownedNode {
		return fmt.Errorf("the existing lock is not managed by this process")
	}

	// Lock after verifying the state and lock contents
	l.mu.Lock()
	defer l.mu.Unlock()

	// Remove object from filesystem
	errRemove := l.fs.Remove(l.GetLockFilename())
	if errRemove != nil {
		return errRemove
	}
	return nil
}

// GetFilename will return the filename for the lock
func (l *FileLock) GetFilename() string {
	return l.filename
}

// GetLockFilename will return the filename for the lock object
func (l *FileLock) GetLockFilename() string {
	lockPath := l.GetFilename() + l.GetLockSuffix()
	return lockPath
}

// GetLockState returns the lock's state
func (l *FileLock) GetLockState() (LockState, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, errStat := l.fs.Stat(l.GetLockFilename())
	if errStat != nil {
		if os.IsNotExist(errStat) {
			// Throw away the error here because it means the file doesn't exist
			return LockStateUnlocked, nil
		} else {
			return LockStateUnknown, errStat
		}
	}
	return LockStateLocked, nil
}

// isSameLock will determine if the current lock belongs to this node and session
func (l *FileLock) isSameLock() (bool, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	aFile, errOpen := l.fs.Open(l.GetLockFilename())
	if errOpen != nil {
		return false, false, fmt.Errorf("unable to open %q: %w", l.GetLockFilename(), errOpen)
	}
	defer aFile.Close()

	body, errRead := ioutil.ReadAll(aFile)
	if errRead != nil {
		return false, false, errRead
	}

	// split the body into the node and id
	parts := bytes.Split(body, []byte("\n"))
	if len(parts) != 2 {
		return false, false, fmt.Errorf("incompatible lock file format")
	}

	return bytes.Equal(parts[0], l.GetNodeBytes()), bytes.Equal(parts[1], l.GetIDBytes()), nil
}

// WaitForLock waits until an object is no longer locked or cancels based on a timeout
func (l *FileLock) WaitForLock() error {
	// Do not lock/unlock the struct here or it will block getting the lock state

	// Pass a context with a timeout to tell a blocking function that it
	// should abandon its work after the timeout elapses.
	ctx, cancel := context.WithTimeout(context.Background(), l.GetTimeout())
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("unable to obtain lock after %s: %w", l.GetTimeout(), ctx.Err())
		default:
			lockState, errGetLockState := l.GetLockState()
			if errGetLockState != nil {
				return errGetLockState
			}
			switch lockState {
			case LockStateUnlocked:
				return nil
			case LockStateUnknown, LockStateLocked:
				// Add jitter to the sleep of 1 second
				r := rand.Intn(100)
				time.Sleep(1*time.Second + time.Duration(r)*time.Millisecond)
			}
		}
	}
}
