package limit

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/dgraph-io/badger"
)

// Bucket represents a ratelimit bucket
type Bucket struct {
	BucketInfo
	db         *badger.DB
	pendingKey []byte
	stopped    bool
	stopChan   chan struct{}
}

// NewBucket makes a new ratelimit bucket. Panics if size is 0.
func NewBucket(db *badger.DB, info BucketInfo) (*Bucket, error) {
	if info.Size == 0 {
		panic("buckets cannot be created with size 0")
	}

	return &Bucket{
		BucketInfo: info,
		db:         db,
		pendingKey: []byte("pending." + info.ID),
		stopped:    true,
		stopChan:   make(chan struct{}, 1),
	}, info.Save(db)
}

// IsStarted returns whether this bucket is currently ticking
func (b *Bucket) IsStarted() bool {
	return !b.stopped
}

// Start starts the bucket
func (b *Bucket) Start() {
	b.Stop()

	b.stopped = false
	errChan := make(chan error)
	defer close(errChan)

	go func() {
		for err := range errChan {
			b.ErrorHandler(err)
		}
	}()
	b.tick(errChan)
}

// Stop closes this bucket
func (b *Bucket) Stop() {
	if b.IsStarted() {
		b.stopChan <- struct{}{}
		b.stopped = true
	}
}

// TimeoutAndIncr gets the timeout for the next request and increments the number of pending requests by 1
func (b *Bucket) TimeoutAndIncr(txn *badger.Txn) (d time.Duration, err error) {
	pending, err := b.IncrPending(txn, 1)
	if err != nil {
		return
	}

	// since "pending" is already incremented, this is pending-1
	d = b.TimeoutWithPending(pending - 1)
	return
}

// Timeout gets the wait time for the next request
func (b *Bucket) Timeout(txn *badger.Txn) (d time.Duration, err error) {
	pending, err := b.GetPending(txn)
	if err != nil {
		return
	}

	d = b.TimeoutWithPending(pending)
	return
}

// TimeoutWithPending gets the timeout for the next request, given a number of pending requests
func (b *Bucket) TimeoutWithPending(pending uint32) time.Duration {
	bucketCount := time.Duration(pending / b.Size)
	return b.Interval * bucketCount
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

// IncrPending increments the pending count by the specified amount
func (b *Bucket) IncrPending(txn *badger.Txn, count int) (pending uint32, err error) {
	pending, err = b.GetPending(txn)
	if err != nil {
		return
	}

	var newCount uint32
	uCount := uint32(count)
	if count < 0 {
		// prevent integer underflows
		if pending > uCount {
			newCount = pending - uCount
		} else {
			newCount = 0
		}
	} else {
		newCount = pending + uCount
		if newCount < pending {
			panic("integer overflow: pending requests exceeded max uint32")
		}
	}
	pending = newCount

	if pending == 0 {
		b.Stop()
		err = txn.Delete(b.pendingKey)
	} else {
		if !b.IsStarted() {
			go b.Start()
		}

		v := make([]byte, 4)
		binary.LittleEndian.PutUint32(v, pending)
		err = txn.Set(b.pendingKey, v)
	}

	return
}

func (b *Bucket) tick(errChan chan error) {
	var err error
	ticker := time.NewTicker(b.Interval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-b.stopChan:
			break loop
		case <-ticker.C:
			err = b.db.Update(func(txn *badger.Txn) error {
				_, err := b.IncrPending(txn, -int(b.Size))
				return err
			})

			if err != nil {
				errChan <- err
			}
		}
	}

	err = b.db.Update(func(txn *badger.Txn) error {
		_, err := b.IncrPending(txn, math.MinInt32)
		return err
	})

	if err != nil {
		errChan <- err
	}
}
