package safelock

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/go-redis/redis/v8"
	"golang.org/x/crypto/sha3"
	"math/rand"
	"time"
)

// RedisLock will create a lock for a specific file using redis as the lock storage
// As an example, if the URI is file:///filename.txt then the lock will be
// a file named safelock-<FILE PATH SHA3-256> and the contents will be the lock's
// UUID.
type RedisLock struct {
	*SafeLock
	rdb      redis.UniversalClient
	filename string
	hash     string
}

// NewRedisLock creates a new instance of RedisLock
func NewRedisLock(node uint16, filename string, rdb redis.UniversalClient) *RedisLock {
	// create SHA256 hasher
	hasher := sha3.New256()

	// add data to hasher
	hasher.Write([]byte(filename))

	// create output buffer
	buff := make([]byte, 32)
	// sum hash slices into buffer
	hasher.Sum(buff[:0])

	// hex encode hash and return
	hash := hex.EncodeToString(buff)

	return &RedisLock{
		SafeLock: NewSafeLock(node),
		rdb:      rdb,
		filename: filename,
		hash:     hash,
	}
}

// Lock will lock
func (l *RedisLock) Lock() error {

	// Check first if the lock exists
	// For RedisLock the error is never used and the state can only be locked/unlocked
	lockState, err := l.GetLockState()
	if err != nil {
		return err
	}
	if lockState == LockStateLocked {
		// conditionally handle deadlock if the lock exists and is owned by a prior session of the same node

		// check the ownership of the lock
		ownedNode, ownedSession, expired, err := l.lockStatus()
		if err != nil {
			return fmt.Errorf("failed to check lock ownership: %v", err)
		}

		// release a deadlocked file lock
		if (ownedNode && !ownedSession) || expired {
			l.mu.Lock()
			// remove redis lock
			err := l.rdb.Del(context.TODO(), l.GetLockFilename()).Err()
			if err != nil {
				l.mu.Unlock()
				return fmt.Errorf("failed to remove deadlocked file lock %q: %v", l.GetLockFilename(), err)
			}
			l.mu.Unlock()
		} else {
			return fmt.Errorf("the object at %s is locked", l.GetFilename())
		}
	}

	// Lock after getting the lock state
	l.mu.Lock()
	defer l.mu.Unlock()

	// create new lock in redis
	err = l.rdb.Set(context.TODO(), l.GetLockFilename(), l.GetLockBody(), l.timeout).Err()
	if err != nil {
		return fmt.Errorf("failed to create lock in redis with key %q: %v", l.GetLockFilename(), err)
	}

	return nil
}

// Unlock will unlock
func (l *RedisLock) Unlock() error {

	// Check first if the lock exists
	// For RedisLock the error is never used and the state can only be locked/unlocked
	lockState, err := l.GetLockState()
	if err != nil {
		return err
	}

	if lockState == LockStateUnlocked {
		return fmt.Errorf("the object at %s is not locked", l.GetFilename())
	}

	// Validate that the lock belongs to this code
	ownedNode, _, expired, errIsSameLock := l.lockStatus()
	if errIsSameLock != nil {
		return fmt.Errorf("unable to determine if lock is the same lock: %w", errIsSameLock)
	}

	if !ownedNode && !expired {
		return fmt.Errorf("the existing lock is not managed by this process")
	}

	// Lock after verifying the state and lock contents
	l.mu.Lock()
	defer l.mu.Unlock()

	// Remove object from redis
	err = l.rdb.Del(context.TODO(), l.GetLockFilename()).Err()
	if err != nil {
		return fmt.Errorf("failed to remove deadlocked file lock %q: %v", l.GetLockFilename(), err)
	}
	return nil
}

// ForceUnlock will unlock despite ownership
func (l *RedisLock) ForceUnlock() error {

	// Check first if the lock exists
	// For RedisLock the error is never used and the state can only be locked/unlocked
	lockState, _ := l.GetLockState()
	if lockState == LockStateUnlocked {
		return fmt.Errorf("the object at %s is not locked", l.GetFilename())
	}

	// Lock after verifying the state and lock contents
	l.mu.Lock()
	defer l.mu.Unlock()

	// Remove object from redis
	err := l.rdb.Del(context.TODO(), l.GetLockFilename()).Err()
	if err != nil {
		return fmt.Errorf("failed to remove locked file lock %q: %v", l.GetLockFilename(), err)
	}
	return nil
}

// GetFilename will return the filename for the lock
func (l *RedisLock) GetFilename() string {
	return l.filename
}

// GetLockFilename will return the filename for the lock object
func (l *RedisLock) GetLockFilename() string {
	return fmt.Sprintf("safelock-%s", l.hash)
}

// GetLockState returns the lock's state
func (l *RedisLock) GetLockState() (LockState, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	res := l.rdb.Get(context.TODO(), l.GetLockFilename())
	if res.Err() != nil {
		if res.Err() == redis.Nil {
			return LockStateUnlocked, nil
		}
		return LockStateUnknown, fmt.Errorf("failed to retrieve key from redis %q: %v", l.GetLockFilename(), res.Err())
	}

	return LockStateLocked, nil
}

// lockStatus load the current state of the lock
// Returns
// 		nodeOwned 			- bool, whether the lock is owned by this node
//		sessionOwned 		- bool, whether the lock is owned by this session
//		expired 			- bool, whether the lock has passed its expiration
func (l *RedisLock) lockStatus() (bool, bool, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// retrieve lock from redis
	body, err := l.rdb.Get(context.TODO(), l.GetLockFilename()).Bytes()
	if err != nil {
		// handle lock expiration
		if err == redis.Nil {
			return false, false, true, nil
		}
		return false, false, false, fmt.Errorf("failed to retrieve lock data from redis %q: %v", l.GetLockFilename(), err)
	}

	// split the body into the node and id
	parts := bytes.Split(body, []byte("__::__"))
	if len(parts) != 3 {
		return false, false, false, fmt.Errorf("incompatible lock file format")
	}

	// set the default value for expiration to false
	expired := false

	// handle timestamp if there is a configured timeout on the lock
	if l.timeout > 0 {
		// decode the timestamp from the third position in the lock file
		ts := time.Unix(0, int64(binary.LittleEndian.Uint64(parts[2])))

		// update expired with the expiration status
		expired = time.Since(ts) > l.timeout
	}

	return bytes.Equal(parts[0], l.GetNodeBytes()), bytes.Equal(parts[1], l.GetIDBytes()), expired, nil
}

// WaitForLock waits until an object is no longer locked or cancels based on a timeout
func (l *RedisLock) WaitForLock(timeout time.Duration) error {
	// Do not lock/unlock the struct here or it will block getting the lock state

	// create variable to hold the context
	var ctx context.Context
	var cancel context.CancelFunc

	// conditionally configure the context with a timeout
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}

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
