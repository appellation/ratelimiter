package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/appellation/ratelimiter"
	"github.com/dgraph-io/badger"
	"google.golang.org/grpc"
)

var dir = flag.String("dir", "./badger", "the directory in which to store ratelimits")
var addr = flag.String("addr", ":3000", "the network address to listen on")

func main() {
	flag.Parse()

	opts := badger.DefaultOptions
	opts.Dir = *dir
	opts.ValueDir = *dir

	log.Println("opening database...")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("attempting to gracefully stop server...")
		server.GracefulStop()
	}()

	l := ratelimiter.New(db)
	ratelimiter.RegisterRatelimiterServer(server, l)

	log.Printf("server ready at \"%s\"", *addr)
	err = server.Serve(lis)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("exiting")
}
