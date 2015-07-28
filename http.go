package main

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/codeskyblue/groupcache"
	"github.com/gorilla/websocket"
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	wsclient *websocket.Conn
	sendc    = make(chan map[string]interface{}, 10)
)

func InitMaster() (err error) {
	http.HandleFunc(defaultWSURL, WSHandler)
	return nil
}

func WSHandler(w http.ResponseWriter, r *http.Request) {
	/*
		defer func() {
			if e := recover(); e != nil {
				log.Println(e)
			}
		}()
	*/
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(conn.RemoteAddr())
	defer conn.Close()

	var name string
	remoteHost, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	for {
		var err error
		var msg = make(map[string]interface{})
		if err = conn.ReadJSON(&msg); err != nil {
			break
		}

		action, _ := msg["action"].(string)
		switch action {
		case "LOGIN":
			name = "http://" + remoteHost + ":" + msg["port"].(string)
			currKeys := peerGroup.Keys()
			peerGroup.AddPeer(name, conn)
			err = conn.WriteJSON(map[string]string{
				"self":   name,
				"peers":  strings.Join(peerGroup.Keys(), ","),
				"mirror": *mirror,
			})

			peerGroup.RLock()
			for _, key := range currKeys {
				if s, exists := peerGroup.m[key]; exists {
					s.Connection.WriteJSON(map[string]string{
						"action": "PEER_UPDATE",
						"peers":  strings.Join(peerGroup.Keys(), ","),
					})
				}
			}
			peerGroup.RUnlock()
			log.Printf("Peer: %s JOIN", name)
		case "LOG":
			delete(msg, "action")
			msgb, _ := json.Marshal(map[string]interface{}{
				"timestamp": time.Now().Unix(),
				"data":      msg,
				"peer":      name,
			})
			cdnlog.Println(string(msgb))
		default:
			log.Println("UNKNOWN:", msg)
		}
		if err != nil {
			break
		}
	}

	peerGroup.Delete(name)
	peerGroup.RLock()
	for _, key := range peerGroup.Keys() {
		if s, exists := peerGroup.m[key]; exists {
			s.Connection.WriteJSON(map[string]string{
				"action": "PEER_UPDATE",
				"peers":  strings.Join(peerGroup.Keys(), ","),
			})
		}
	}
	peerGroup.RUnlock()
	log.Printf("Peer: %s QUIT", name)
}

func InitPeer() (err error) {
	u, err := url.Parse(*upstream)
	if err != nil {
		return
	}
	u.Path = defaultWSURL
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		return
	}
	wsclient, _, err = websocket.NewClient(conn, u, nil, 1024, 1024)
	if err != nil {
		return
	}

	// Get slave name from master
	_, port, _ := net.SplitHostPort(*address)
	wsclient.WriteJSON(map[string]string{
		"action": "LOGIN",
		"token":  *token,
		"port":   port,
	})
	var msg = make(map[string]string)
	if err = wsclient.ReadJSON(&msg); err != nil {
		return err
	}
	if me, ok := msg["self"]; ok {
		if pool == nil {
			pool = groupcache.NewHTTPPool(me)
		}
		peers := strings.Split(msg["peers"], ",")
		m := msg["mirror"]
		mirror = &m
		log.Println("Self name:", me)
		log.Println("Peer list:", peers)
		log.Println("Mirror site:", *mirror)
		pool.Set(peers...)
	} else {
		return errors.New("'peer_name' not found in master response")
	}

	// Listen peers update
	go func() {
		for {
			err := wsclient.ReadJSON(&msg)
			if err != nil {
				if state.IsClosed() {
					break
				}
				log.Println("Connection to master closed !!!")
				for {
					log.Println("> retry in 5 seconds")
					time.Sleep(time.Second * 5)
					if err := InitPeer(); err == nil {
						break
					}
				}
				break
			}
			action := msg["action"]
			switch action {
			case "PEER_UPDATE":
				peers := strings.Split(msg["peers"], ",")
				log.Println("Update peer list:", peers)
				pool.Set(peers...)
			}
		}
	}()

	// send queue
	go func() {
		for msg := range sendc {
			//log.Println("Send msg:", msg)
			if msg["action"] == nil {
				msg["action"] = "LOG"
			}
			err := wsclient.WriteJSON(msg)
			if err != nil {
				log.Println("Send queue err:", err)
				break
			}
		}
	}()

	return nil
}
