package ratelimiter

import (
	"reflect"

	"github.com/dgraph-io/badger"
	lru "github.com/hashicorp/golang-lru"
)

// Limiter represents a collection of ratelimit buckets
type Limiter struct {
	db      *badger.DB
	Buckets *lru.Cache
}

// NewLimiter makes a new limiter
func NewLimiter(db *badger.DB, size int) *Limiter {
	cache, err := lru.New(size)
	if err != nil {
		panic(err)
	}

	return &Limiter{
		db:      db,
		Buckets: cache,
	}
}

// Get gets a bucket by ID
func (l *Limiter) Get(id []byte) (b *Bucket, err error) {
	v, ok := l.Buckets.Get(id)
	if ok {
		b = v.(*Bucket)
	} else {
		b, err = GetBucket(l.db, id)
		if b != nil && err != nil {
			l.Buckets.Add(id, b)
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
		l.Buckets.Add(info.ID, b)
		if err != nil {
			return
		}
	} else if !reflect.DeepEqual(b.BucketInfo, info) {
		err = b.Save(l.db)
	}

	return
}
