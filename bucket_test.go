package ratelimiter

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
)

const badgerDir = "./badger"

var (
	db            *badger.DB
	bucket        *Bucket
	emptyUint32   uint32
	emptyDuration time.Duration
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func clearDB(t testing.TB, db *badger.DB) {
	if db != nil {
		err := db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	err := os.RemoveAll(badgerDir)
	if err != nil {
		t.Fatal(err)
	}
}

func createDB(t testing.TB) *badger.DB {
	opts := badger.DefaultOptions
	opts.Dir = badgerDir
	opts.ValueDir = badgerDir

	clearDB(t, nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func createBucket(t testing.TB, db *badger.DB) *Bucket {
	bucket, err := NewBucket(db, BucketInfo{
		ID:       "testing",
		Interval: 5 * time.Second,
		Size:     5,
		ErrorHandler: func(err error) {
			t.Fatal(err)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func get(t testing.TB, fn func(txn *badger.Txn) error) {
	err := db.View(fn)
	if err != nil {
		t.Fatal(err)
	}
}

func set(t testing.TB, fn func(txn *badger.Txn) error) {
	err := db.Update(fn)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucket(t *testing.T) {
	db = createDB(t)
	defer clearDB(t, db)
	bucket = createBucket(t, db)

	get(t, func(txn *badger.Txn) error {
		pending, err := bucket.GetPending(txn)
		assert.NoError(t, err)
		assert.Equal(t, uint32(0), pending, "pending requests should be 0")
		return nil
	})

	get(t, func(txn *badger.Txn) error {
		timeout, err := bucket.Timeout(txn)
		assert.NoError(t, err)
		assert.Equal(t, time.Duration(0), timeout, "timeout should be 0")
		return nil
	})

	set(t, func(txn *badger.Txn) error {
		pending, err := bucket.Incr(txn, int(bucket.Size))
		assert.NoError(t, err)
		assert.Equal(t, bucket.Size, pending, "pending should be equal to bucket size")
		return nil
	})

	get(t, func(txn *badger.Txn) error {
		timeout, err := bucket.Timeout(txn)
		assert.NoError(t, err)
		assert.Equal(t, bucket.Interval, timeout, "bucket timeout should be equal to the interval")
		return nil
	})

	time.Sleep(bucket.Interval + 1*time.Second)
	get(t, func(txn *badger.Txn) error {
		pending, err := bucket.GetPending(txn)
		assert.NoError(t, err)
		assert.Equal(t, uint32(0), pending, "pending should be 0 after waiting")
		return nil
	})

	count := rand.Intn(1000)
	for count < int(bucket.Size) {
		count = rand.Intn(1000)
	}
	set(t, func(txn *badger.Txn) error {
		pending, err := bucket.Incr(txn, count)
		assert.NoError(t, err)
		assert.Equal(t, count, int(pending), "pending should equal random count")
		return nil
	})

	get(t, func(txn *badger.Txn) error {
		pending, err := bucket.GetPending(txn)
		assert.NoError(t, err)
		assert.Equal(t, count, int(pending), "pending should equal random count on separate fetch")
		return nil
	})

	time.Sleep(bucket.Interval + 1*time.Second)
	get(t, func(txn *badger.Txn) error {
		pending, err := bucket.GetPending(txn)
		assert.NoError(t, err)
		assert.Equal(t, uint32(count)-bucket.Size, pending, "pending should be equal to count less the bucket size after waiting the interval")
		return nil
	})
}

func BenchmarkBucket(b *testing.B) {
	db = createDB(b)
	defer clearDB(b, db)
	bucket = createBucket(b, db)

	b.Run("GetPending", func(b *testing.B) {
		var err error
		for n := 0; n < b.N; n++ {
			get(b, func(txn *badger.Txn) error {
				emptyUint32, err = bucket.GetPending(txn)
				return err
			})
		}
	})

	b.Run("GetTimeout", func(b *testing.B) {
		var err error
		for n := 0; n < b.N; n++ {
			get(b, func(txn *badger.Txn) error {
				emptyDuration, err = bucket.Timeout(txn)
				return err
			})
		}
	})

	set(b, func(txn *badger.Txn) error {
		_, err := bucket.Incr(txn, 500)
		return err
	})

	b.Run("GetTimeoutWithIncr", func(b *testing.B) {
		var err error
		for n := 0; n < b.N; n++ {
			get(b, func(txn *badger.Txn) error {
				emptyDuration, err = bucket.Timeout(txn)
				return err
			})
		}
	})
}
