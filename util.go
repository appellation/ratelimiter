package ratelimiter

import (
	"time"

	"github.com/golang/protobuf/ptypes/duration"
)

// durationToNative converts a protobuf duration to a Golang duration
func durationToNative(d *duration.Duration) time.Duration {
	return time.Duration(d.GetNanos())*time.Nanosecond + time.Duration(d.GetSeconds())*time.Second
}

// durationToPB converts a Golang duration to a protobuf duration
func durationToPB(d time.Duration) *duration.Duration {
	return &duration.Duration{
		Seconds: int64(d.Seconds()),
		Nanos:   int32(d.Nanoseconds()),
	}
}
