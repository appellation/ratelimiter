package main

import (
	"log"
	"net"

	"github.com/appellation/ratelimiter"
	"github.com/dgraph-io/badger"
	"google.golang.org/grpc"
)

func main() {
	opts := badger.DefaultOptions
	opts.Dir = "./badger"
	opts.ValueDir = "./badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	lis, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatal(err)
	}

	l := ratelimiter.New(db)
	server := grpc.NewServer()
	ratelimiter.RegisterRatelimiterServer(server, l)
	server.Serve(lis)
}
