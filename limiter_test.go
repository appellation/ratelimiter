package ratelimiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimiter(t *testing.T) {
	db := createDB(t)
	defer clearDB(t, db)

	l := NewLimiter(db)
	assert.Len(t, l.Buckets, 0, "buckets should be an empty map")

	b, err := l.Get("testing")
	assert.NoError(t, err)
	assert.Nil(t, b, "bucket should be nil")

	b, err = l.GetAndSave(BucketInfo{
		ID:       "testing",
		Interval: 5 * time.Second,
		Size:     5,
	})
	assert.NoError(t, err)
	assert.IsType(t, &Bucket{}, b, "bucket should be a bucket")
	assert.Len(t, l.Buckets, 1, "buckets should have 1 element")
}
