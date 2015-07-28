package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
)

var cdnlog *log.Logger

var (
	mirror   = flag.String("mirror", "", "Mirror Web Base URL")
	logfile  = flag.String("log", "-", "Set log file, default STDOUT")
	upstream = flag.String("upstream", "", "Server base URL, conflict with -mirror")
	address  = flag.String("addr", ":5000", "Listen address")
	token    = flag.String("token", "1234567890ABCDEFG", "peer and master token should be same")
)

func InitSignal() {
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for {
			s := <-sig
			fmt.Println("Got signal:", s)
			if state.IsClosed() {
				fmt.Println("Cold close !!!")
				os.Exit(1)
			}
			fmt.Println("Warm close, waiting ...")
			go func() {
				state.Close()
				os.Exit(0)
			}()
		}
	}()
}

func main() {
	flag.Parse()

	if *mirror != "" && *upstream != "" {
		log.Fatal("Can't set both -mirror and -upstream")
	}
	if *mirror == "" && *upstream == "" {
		log.Fatal("Must set one of -mirror and -upstream")
	}

	if *logfile == "-" || *logfile == "" {
		cdnlog = log.New(os.Stderr, "CDNLOG: ", 0)
	} else {
		fd, err := os.OpenFile(*logfile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			log.Fatal(err)
		}
		cdnlog = log.New(fd, "", 0)
	}
	if *upstream != "" {
		if err := InitPeer(); err != nil {
			log.Fatal(err)
		}
	}
	if *mirror != "" {
		if _, err := url.Parse(*mirror); err != nil {
			log.Fatal(err)
		}
		if err := InitMaster(); err != nil {
			log.Fatal(err)
		}
	}

	InitSignal()
	log.Printf("Listening on %s", *address)
	log.Fatal(http.ListenAndServe(*address, nil))
}
