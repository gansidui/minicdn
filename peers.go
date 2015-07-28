package main

import (
	"errors"
	"math/rand"
	"sync"

	"github.com/codeskyblue/groupcache"
	"github.com/gorilla/websocket"
)

const defaultWSURL = "/_ws/"

var (
	state = ServerState{
		ActiveDownload: 0,
		closed:         false,
	}
	peerGroup = PeerGroup{
		m: make(map[string]Peer, 10),
	}

	pool *groupcache.HTTPPool
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
