package main

import (
	"sync"
	"time"
)

type ServerState struct {
	sync.Mutex
	closed         bool
	ActiveDownload int
}

func (s *ServerState) addActiveDownload(n int) {
	s.Lock()
	defer s.Unlock()
	s.ActiveDownload += n
}

func (s *ServerState) Close() error {
	s.closed = true
	if wsclient != nil {
		wsclient.Close()
	}
	time.Sleep(time.Millisecond * 500) // 0.5s
	for {
		if s.ActiveDownload == 0 { // Wait until all download finished
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	return nil
}

func (s *ServerState) IsClosed() bool {
	return s.closed
}
