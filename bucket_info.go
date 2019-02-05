package ratelimiter

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/dgraph-io/badger"
)

// BucketInfo represents the ratelimit information of a bucket
type BucketInfo struct {
	ID           string
	Interval     time.Duration
	Size         uint32
	ErrorHandler func(error)
}

// Save saves this bucket to disk
func (i BucketInfo) Save(db *badger.DB) error {
	return db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(i.Key())
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		buf := bytes.NewBuffer([]byte{})
		gob.NewEncoder(buf).Encode(i)
		return txn.Set(i.Key(), buf.Bytes())
	})
}

// Fetch loads this bucket from disk
func (i BucketInfo) Fetch(db *badger.DB) (err error) {
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(i.Key())
		if err != nil {
			return err
		}

		v, err := item.ValueCopy([]byte{})
		if err != nil {
			return err
		}

		buf := bytes.NewReader(v)
		gob.NewDecoder(buf).Decode(i)
		return nil
	})

	return
}

// Key gets the key of this bucket
func (i *BucketInfo) Key() []byte {
	return []byte("info." + i.ID)
}
