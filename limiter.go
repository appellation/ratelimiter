package ratelimiter

import (
	"context"
	"log"

	"github.com/dgraph-io/badger"
)

//go:generate protoc ratelimiter.proto --go_out=plugins=grpc:.

// Limiter represents a collection of ratelimit buckets
type Limiter struct {
	db *badger.DB
}

// New makes a new limiter
func New(db *badger.DB) *Limiter {
	return &Limiter{db}
}

// Get gets a bucket by ID
func (l *Limiter) Get(id []byte) (b *Bucket, err error) {
	return GetBucket(l.db, id)
}

// GetAndSave gets a bucket by ID and saves its properties to disk (if changed or not previously existant)
func (l *Limiter) GetAndSave(info BucketInfo) (b *Bucket, err error) {
	b, err = l.Get(info.ID)
	if err != nil {
		return
	}

	if b == nil {
		b, err = NewBucket(l.db, info)
		if err != nil {
			return
		}
	} else if (b.Interval != info.Interval && info.Interval != 0) ||
		(b.Size != info.Size && info.Size != 0) {
		err = b.Save(l.db)
	}

	return
}

// Fetch fetches the gateway for a given request
func (l *Limiter) Fetch(ctx context.Context, req *Request) (res *Response, err error) {
	b, err := l.GetAndSave(BucketInfo{
		ID:       req.GetId(),
		Size:     req.GetSize(),
		Interval: durationToNative(req.GetInterval()),
	})
	if err != nil {
		return
	}

	i := req.GetIncr()
	willIncr := i != 0

	log.Printf("bucket %v", b)

	txn := b.Txn(willIncr)
	defer b.Commit(txn)

	timeout, err := b.Timeout(txn)
	if err != nil {
		return
	}

	if willIncr {
		_, err = b.Incr(txn, int(i))
		if err != nil {
			return
		}
	}

	res = &Response{
		Id:      b.ID,
		WaitFor: durationToPB(timeout),
	}
	return
}
