package ratelimiter

import (
	"reflect"

	"github.com/dgraph-io/badger"
)

// Limiter represents a collection of ratelimit buckets
type Limiter struct {
	db      *badger.DB
	Buckets map[string]*Bucket
}

// NewLimiter makes a new limiter
func NewLimiter(db *badger.DB) *Limiter {
	return &Limiter{
		db:      db,
		Buckets: make(map[string]*Bucket),
	}
}

// Get gets a bucket by ID
func (l *Limiter) Get(id string) (b *Bucket, err error) {
	b = l.Buckets[id]
	if b == nil {
		b, err = GetBucket(l.db, id)
		if b != nil && err != nil {
			l.Buckets[id] = b
		}
	}

	return
}

// GetAndSave gets a bucket by ID and saves its properties to disk (if changed or not previously existant)
func (l *Limiter) GetAndSave(info BucketInfo) (b *Bucket, err error) {
	b, err = l.Get(info.ID)
	if err != nil {
		return
	}

	if b == nil {
		b, err = NewBucket(l.db, info)
		l.Buckets[info.ID] = b
		if err != nil {
			return
		}
	} else if !reflect.DeepEqual(b.BucketInfo, info) {
		err = b.Save(l.db)
	}

	return
}
