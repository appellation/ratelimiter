package ratelimiter

// KeyType represents a key type in the database
type KeyType byte

// Available key types
const (
	KeyBucketInfo KeyType = iota
	KeyBucketPending
)

// Make a key type into a key given an ID
func (k KeyType) Make(id []byte) []byte {
	return append([]byte{byte(k)}, id...)
}
