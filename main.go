package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/codeskyblue/groupcache"
)

var (
	cdnlog     *log.Logger
	thumbNails = groupcache.NewGroup("thumbnail", 512<<20, groupcache.GetterFunc(
		func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
			fileName := key
			bytes, err := generateThumbnail(fileName)
			if err != nil {
				return err
			}
			dest.SetBytes(bytes)
			return nil
		}))
)

func generateThumbnail(key string) ([]byte, error) {
	u, _ := url.Parse(*mirror)
	u.Path = key
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func FileHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path

	state.addActiveDownload(1)
	defer state.addActiveDownload(-1)

	if *upstream == "" { // Master
		if peerAddr, err := peerGroup.PeekPeer(); err == nil {
			u, _ := url.Parse(peerAddr)
			u.Path = r.URL.Path
			u.RawQuery = r.URL.RawQuery
			http.Redirect(w, r, u.String(), 302)
			return
		}
	}
	//fmt.Println("KEY:", key)
	var data []byte
	var ctx groupcache.Context
	err := thumbNails.Get(ctx, key, groupcache.AllocatingByteSliceSink(&data))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	sendData := map[string]interface{}{
		"remote_addr": r.RemoteAddr,
		"key":         key,
		"success":     err == nil,
		"user_agent":  r.Header.Get("User-Agent"),
	}
	headerData := r.Header.Get("X-Minicdn-Data")
	headerType := r.Header.Get("X-Minicdn-Type")
	if headerType == "json" {
		var data interface{}
		if err := json.Unmarshal([]byte(headerData), &data); err == nil {
			sendData["header_data"] = data
		}
	} else {
		sendData["header_data"] = headerData
		sendData["header_type"] = headerType
	}

	sendc <- sendData
	var modTime time.Time = time.Now()

	rd := bytes.NewReader(data)
	http.ServeContent(w, r, filepath.Base(key), modTime, rd)
}

func LogHandler(w http.ResponseWriter, r *http.Request) {
	if *logfile == "" || *logfile == "-" {
		http.Error(w, "Log file not found", 404)
		return
	}
	http.ServeFile(w, r, *logfile)
}

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
	//fmt.Println("Hello CDN")
	http.HandleFunc("/", FileHandler)
	http.HandleFunc("/_log", LogHandler)
	log.Printf("Listening on %s", *address)
	log.Fatal(http.ListenAndServe(*address, nil))
}
