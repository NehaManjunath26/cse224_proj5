package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

const EX_USAGE int = 64

func main() {
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	args := flag.Args()
	blockStoreAddr := ""
	if len(args) == 1 {
		blockStoreAddr = args[0]
	}

	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddr))
}

func startServer(hostAddr string, serviceType string, blockStoreAddr string) error {
	grpcServer := grpc.NewServer()

	if serviceType == "block" || serviceType == "both" {
		blockstore := surfstore.NewBlockStore()
		surfstore.RegisterBlockStoreServer(grpcServer, blockstore)
	}

	if serviceType == "meta" || serviceType == "both" {
		metastore := surfstore.NewMetaStore(blockStoreAddr)
		surfstore.RegisterMetaStoreServer(grpcServer, metastore)
	}

	listen, err := net.Listen("tcp", hostAddr)

	if err != nil {
		return fmt.Errorf("listen failed: %v", err)
	}

	if err := grpcServer.Serve(listen); err != nil {
		return fmt.Errorf("serve failed: %v", err)
	}

	return nil
}
