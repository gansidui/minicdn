package main

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/codeskyblue/groupcache"
	"github.com/gorilla/websocket"
)

const defaultWSURL = "/_ws/"

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	state = ServerState{
		ActiveDownload: 0,
		Closed:         false,
	}
	peerGroup = PeerGroup{
		m: make(map[string]Peer, 10),
	}

	pool     *groupcache.HTTPPool
	wsclient *websocket.Conn
)

type Peer struct {
	Name           string
	Connection     *websocket.Conn
	ActiveDownload int
}

type PeerGroup struct {
	sync.RWMutex
	m map[string]Peer
}

func (sm *PeerGroup) AddPeer(name string, conn *websocket.Conn) {
	sm.Lock()
	defer sm.Unlock()
	sm.m[name] = Peer{
		Name:       name,
		Connection: conn,
	}
}

func (sm *PeerGroup) Delete(name string) {
	sm.Lock()
	delete(sm.m, name)
	sm.Unlock()
}

func (sm *PeerGroup) Keys() []string {
	sm.RLock()
	defer sm.RUnlock()
	keys := []string{}
	for key, _ := range sm.m {
		keys = append(keys, key)
	}
	return keys
}

func (sm *PeerGroup) PeekPeer() (string, error) {
	// FIXME(ssx): need to order by active download count
	sm.RLock()
	defer sm.RUnlock()
	ridx := rand.Int()
	keys := []string{}
	for key, _ := range sm.m {
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return "", errors.New("Peer count zero")
	}
	return keys[ridx%len(keys)], nil
}

func (sm *PeerGroup) BroadcastJSON(v interface{}) error {
	var err error
	for _, s := range sm.m {
		if err = s.Connection.WriteJSON(v); err != nil {
			return err
		}
	}
	return nil
}

type ServerState struct {
	sync.Mutex
	ActiveDownload int
	Closed         bool
}

func (s *ServerState) addActiveDownload(n int) {
	s.Lock()
	defer s.Unlock()
	s.ActiveDownload += n
}

func (s *ServerState) Close() error {
	s.Closed = true
	time.Sleep(time.Millisecond * 500) // 0.5s
	for {
		if s.ActiveDownload == 0 { // Wait until all download finished
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	return nil
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
				log.Println("Connection to master closed, retry in 10 seconds")
				time.Sleep(time.Second * 10)
				InitPeer()
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

	return nil
}

func InitMaster() (err error) {
	http.HandleFunc(defaultWSURL, WSHandler)
	return nil
}

func WSHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(conn.RemoteAddr())
	defer conn.Close()

	var name string
	remoteHost, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	var msg = make(map[string]string)
	for {
		var err error
		if err = conn.ReadJSON(&msg); err != nil {
			break
		}

		log.Println(msg)
		switch msg["action"] {
		case "LOGIN":
			name = "http://" + remoteHost + ":" + msg["port"]
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
