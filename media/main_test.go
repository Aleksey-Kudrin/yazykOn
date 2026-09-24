package main

import (
	"testing"

	"github.com/pion/webrtc/v4"
)

func newTestPeer(t *testing.T, id string) *Peer {
	t.Helper()
	pc, err := newPeerConnection()
	if err != nil {
		t.Fatalf("new peer connection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return &Peer{
		id:           id,
		pc:           pc,
		published:    make(map[string]*webrtc.TrackLocalStaticRTP),
		subscriptions: make(map[string]*webrtc.RTPSender),
	}
}

func TestRemoveSubscriptionRemovesSender(t *testing.T) {
	p := newTestPeer(t, "subscriber")
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio-1",
		"publisher",
	)
	if err != nil {
		t.Fatalf("new local track: %v", err)
	}

	sender, err := p.pc.AddTrack(track)
	if err != nil {
		t.Fatalf("add track: %v", err)
	}
	p.subscriptions["publisher:audio-1"] = sender

	if !removeSubscription(p, "publisher:audio-1") {
		t.Fatal("removeSubscription returned false")
	}
	if _, ok := p.subscriptions["publisher:audio-1"]; ok {
		t.Fatal("subscription was not deleted")
	}
	if err := p.pc.RemoveTrack(sender); err == nil {
		t.Fatal("sender was still attached after removeSubscription")
	}
}

func TestRemoveSubscriptionIsIdempotent(t *testing.T) {
	p := newTestPeer(t, "subscriber")
	if removeSubscription(p, "missing") {
		t.Fatal("missing subscription should return false")
	}

	p.closed = true
	if removeSubscription(p, "missing") {
		t.Fatal("closed peer should return false")
	}
}

func TestRoomTracksAndPeersLifecycle(t *testing.T) {
	room := &Room{id: "room-1", peers: make(map[string]*Peer), tracks: make(map[string]*PublishedTrack)}
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"video-1",
		"publisher",
	)
	if err != nil {
		t.Fatalf("new video track: %v", err)
	}
	room.peers["publisher"] = &Peer{id: "publisher", room: room}
	room.tracks["publisher:video-1"] = &PublishedTrack{
		ownerID: "publisher",
		trackID: "video-1",
		local:   track,
	}

	if got := len(othersSnapshot(room, "publisher")); got != 0 {
		t.Fatalf("othersSnapshot returned %d peers, want 0", got)
	}
	if got := len(room.tracks); got != 1 {
		t.Fatalf("room has %d tracks, want 1", got)
	}
}

func TestNewID(t *testing.T) {
	a, b := newID(), newID()
	if len(a) != 18 || len(b) != 18 {
		t.Fatalf("unexpected peer id lengths: %q %q", a, b)
	}
	if a == b {
		t.Fatal("generated peer IDs are identical")
	}
}
