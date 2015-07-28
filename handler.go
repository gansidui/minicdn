package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/codeskyblue/groupcache"
	"github.com/ugorji/go/codec"
)

var (
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

type HttpResponse struct {
	Header http.Header
	Body   []byte
}

func generateThumbnail(key string) ([]byte, error) {
	u, _ := url.Parse(*mirror)
	u.Path = key
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf := bytes.NewBuffer(nil)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	mpenc := codec.NewEncoder(buf, &codec.MsgpackHandle{})
	err = mpenc.Encode(HttpResponse{resp.Header, body})
	return buf.Bytes(), err
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
	fmt.Println("KEY:", key)
	var data []byte
	var ctx groupcache.Context
	err := thumbNails.Get(ctx, key, groupcache.AllocatingByteSliceSink(&data))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var hr HttpResponse
	mpdec := codec.NewDecoder(bytes.NewReader(data), &codec.MsgpackHandle{})
	err = mpdec.Decode(&hr)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// FIXME(ssx): should have some better way
	for key, _ := range hr.Header {
		w.Header().Set(key, hr.Header.Get(key))
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
		err := json.Unmarshal([]byte(headerData), &data)
		if err == nil {
			sendData["header_data"] = data
			sendData["header_type"] = headerType
		} else {
			log.Println("header data decode:", err)
		}
	} else {
		sendData["header_data"] = headerData
		sendData["header_type"] = headerType
	}

	if *upstream != "" { // Slave
		sendc <- sendData
	}
	// FIXME(ssx): ModTime should from header
	var modTime time.Time = time.Now()

	rd := bytes.NewReader(hr.Body)
	http.ServeContent(w, r, filepath.Base(key), modTime, rd)
}

func LogHandler(w http.ResponseWriter, r *http.Request) {
	if *logfile == "" || *logfile == "-" {
		http.Error(w, "Log file not found", 404)
		return
	}
	http.ServeFile(w, r, *logfile)
}

func init() {
	http.HandleFunc("/", FileHandler)
	http.HandleFunc("/_log", LogHandler)
}
