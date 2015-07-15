package main

import "testing"

func TestServerState(t *testing.T) {
	ss := ServerState{}
	ss.addActiveDownload(1)
	if ss.ActiveDownload != 1 {
		t.Errorf("expect 1 but got %d", ss.ActiveDownload)
	}
}
