package ratelimiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimiter(t *testing.T) {
	db := createDB(t)
	defer clearDB(t, db)

	l := NewLimiter(db, 500)
	assert.Equal(t, 0, l.Buckets.Len(), "buckets should be empty")

	b, err := l.Get([]byte("testing"))
	assert.NoError(t, err)
	assert.Nil(t, b, "bucket should be nil")

	b, err = l.GetAndSave(BucketInfo{
		ID:       []byte("testing"),
		Interval: 5 * time.Second,
		Size:     5,
	})
	assert.NoError(t, err)
	assert.IsType(t, &Bucket{}, b, "bucket should be a bucket")
	assert.Equal(t, 1, l.Buckets.Len(), "buckets should have 1 element")
}
