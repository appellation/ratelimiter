package main

import (
	"context"
	"testing"

	"github.com/appellation/ratelimiter"
	"github.com/golang/protobuf/ptypes/duration"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	go main()

	conn, err := grpc.Dial(":3000", grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := ratelimiter.NewRatelimiterClient(conn)
	res, err := client.Fetch(context.Background(), &ratelimiter.Request{
		Id:   []byte("test"),
		Size: 5,
		Interval: &duration.Duration{
			Seconds: 5,
			Nanos:   0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log(res)
}
