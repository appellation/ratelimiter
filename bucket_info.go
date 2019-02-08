package ratelimiter

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/dgraph-io/badger"
)

// BucketInfo represents the ratelimit information of a bucket
type BucketInfo struct {
	ID           []byte
	Interval     time.Duration
	Size         uint32
	ErrorHandler func(error)
}

// FetchBucketInfo fetches bucket information from the database
func FetchBucketInfo(db *badger.DB, id []byte) (i BucketInfo, err error) {
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(KeyBucketInfo.Make(id))
		if err != nil {
			return err
		}

		v, err := item.ValueCopy([]byte{})
		if err != nil {
			return err
		}

		buf := bytes.NewReader(v)
		gob.NewDecoder(buf).Decode(&i)
		return nil
	})

	return
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

// Key gets the key of this bucket
func (i *BucketInfo) Key() []byte {
	return KeyBucketInfo.Make(i.ID)
}
