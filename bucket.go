package ratelimiter

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/dgraph-io/badger"
)

// Errors
var (
	ErrEmptyBucket = errors.New("buckets cannot have size 0")
)

// Bucket represents a ratelimit bucket
type Bucket struct {
	BucketInfo
	db           *badger.DB
	pendingKey   []byte
	lastTick     time.Time
	started      bool
	stopChan     chan struct{}
	errorHandler func(error)
}

// GetBucket attempts to fetch a bucket from disk; returns nil, nil if not found
func GetBucket(db *badger.DB, id []byte) (*Bucket, error) {
	info, err := FetchBucketInfo(db, id)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return NewBucket(db, info)
}

// NewBucket makes a new ratelimit bucket. Panics if size is 0.
func NewBucket(db *badger.DB, info BucketInfo) (*Bucket, error) {
	if info.Size == 0 {
		return nil, ErrEmptyBucket
	}

	return &Bucket{
		BucketInfo: info,
		db:         db,
		pendingKey: KeyBucketPending.Make(info.ID),
		stopChan:   make(chan struct{}, 1),
	}, nil
}

// Txn checks out a badger transaction
func (b *Bucket) Txn(update bool) *badger.Txn {
	return b.db.NewTransaction(update)
}

// Commit discards and commits the given badger transaction
func (b *Bucket) Commit(txn *badger.Txn) error {
	txn.Discard()
	return txn.Commit(nil)
}

// Start starts the bucket
func (b *Bucket) Start() {
	b.Stop()
	b.tick(b.errorHandler)
}

// Stop closes this bucket
func (b *Bucket) Stop() {
	if b.started {
		b.stopChan <- struct{}{}
	}
}

// SetErrorHandler sets the error handler for this bucket
func (b *Bucket) SetErrorHandler(fn func(error)) {
	b.errorHandler = fn
}

// TimeoutAndIncr gets the timeout for the next request and increments the number of pending requests by 1
func (b *Bucket) TimeoutAndIncr(txn *badger.Txn) (d time.Duration, err error) {
	pending, err := b.Incr(txn, 1)
	if err != nil {
		return
	}

	// since "pending" is already incremented, this is pending-1
	d = b.UnlocksIn(pending - 1)
	return
}

// Timeout gets the wait time for the next request
func (b *Bucket) Timeout(txn *badger.Txn) (d time.Duration, err error) {
	pending, err := b.GetPending(txn)
	if err != nil {
		return
	}

	d = b.UnlocksIn(pending)
	return
}

// UnlocksIn determines how long until this bucket unlocks
func (b *Bucket) UnlocksIn(pending uint32) time.Duration {
	if !b.started {
		return time.Duration(0)
	}

	// pending/size represents how many times the bucket has been filled
	return time.Now().Sub(b.lastTick) + b.Interval*time.Duration(pending/b.Size)
}

// GetPending returns the number of pending requests
func (b *Bucket) GetPending(txn *badger.Txn) (pending uint32, err error) {
	item, err := txn.Get(b.pendingKey)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return
	}

	v, err := item.ValueCopy(make([]byte, 4))
	if err != nil {
		return
	}

	pending = binary.LittleEndian.Uint32(v)
	return
}

// Incr increments the pending count by the specified amount
func (b *Bucket) Incr(txn *badger.Txn, count int) (pending uint32, err error) {
	pending, err = b.GetPending(txn)
	if err != nil {
		return
	}

	var newCount uint32
	if count < 0 {
		// prevent integer underflows
		uCount := uint32(-count)
		if pending > uCount {
			newCount = pending - uCount
		} else {
			newCount = 0
		}
	} else {
		newCount = pending + uint32(count)
		if newCount < pending {
			panic("integer overflow: pending requests exceeded max uint32")
		}
	}
	pending = newCount

	if pending == 0 {
		b.Stop()
		err = txn.Delete(b.pendingKey)
	} else {
		if !b.started {
			go b.Start()
		}

		v := make([]byte, 4)
		binary.LittleEndian.PutUint32(v, pending)
		err = txn.Set(b.pendingKey, v)
	}

	return
}

func (b *Bucket) tick(errFn func(error)) {
	ticker := time.NewTicker(b.Interval)
	defer ticker.Stop()

	b.started = true
	b.lastTick = time.Now()

loop:
	for {
		select {
		case <-b.stopChan:
			break loop
		case t := <-ticker.C:
			b.lastTick = t
			err := b.db.Update(func(txn *badger.Txn) error {
				_, err := b.Incr(txn, -int(b.Size))
				return err
			})

			if err != nil {
				errFn(err)
			}
		}
	}

	b.started = false
}
