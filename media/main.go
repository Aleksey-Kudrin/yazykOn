package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const (
	mediaUDPMin = 50000
	mediaUDPMax = 50100
)

type Signal struct {
	Type   string          `json:"type"`
	RoomID string          `json:"roomId,omitempty"`
	PeerID string          `json:"peerId,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

type Peer struct {
	id        string
	room      *Room
	conn      *websocket.Conn
	pc        *webrtc.PeerConnection
	mu        sync.Mutex
	closed    bool
	negotiating bool
	negotiationPending bool
	published      map[string]*webrtc.TrackLocalStaticRTP
	subscriptions  map[string]*webrtc.RTPSender
}

type Room struct {
	id     string
	mu     sync.RWMutex
	peers  map[string]*Peer
	tracks map[string]*PublishedTrack
}

type PublishedTrack struct {
	ownerID string
	trackID string
	local   *webrtc.TrackLocalStaticRTP
}

var (
	upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	roomsMu  sync.Mutex
	rooms    = map[string]*Room{}
)

func newPeerConnection() (*webrtc.PeerConnection, error) {
	settings := webrtc.SettingEngine{}
	if err := settings.SetEphemeralUDPPortRange(mediaUDPMin, mediaUDPMax); err != nil {
		return nil, err
	}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))
	return api.NewPeerConnection(webrtc.Configuration{})
}

func getRoom(id string) *Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	if r := rooms[id]; r != nil {
		return r
	}
	r := &Room{id: id, peers: make(map[string]*Peer), tracks: make(map[string]*PublishedTrack)}
	rooms[id] = r
	return r
}

func send(p *Peer, msg Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	return p.conn.WriteJSON(msg)
}

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"service":"yazykOn-sfu","version":"0.5.0","mediaUdpRange":"50000-50100"}`))
	})
	http.HandleFunc("/ws", handleWS)

	log.Println("языкOn custom SFU listening on :4000, media UDP :50000-50100")
	log.Fatal(http.ListenAndServe(":4000", nil))
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	var first Signal
	if err := conn.ReadJSON(&first); err != nil || first.Type != "join" || first.RoomID == "" {
		_ = conn.WriteJSON(Signal{Type: "error", Data: json.RawMessage(`{"code":"INVALID_JOIN"}`)})
		return
	}

	id := first.PeerID
	if id == "" {
		id = newID()
	}

	pc, err := newPeerConnection()
	if err != nil {
		_ = conn.WriteJSON(Signal{Type: "error", Data: json.RawMessage(`{"code":"MEDIA_PORT_CONFIG"}`)})
		return
	}

	room := getRoom(first.RoomID)
	me := &Peer{id: id, room: room, conn: conn, pc: pc, published: make(map[string]*webrtc.TrackLocalStaticRTP), subscriptions: make(map[string]*webrtc.RTPSender)}

	room.mu.Lock()
	room.peers[id] = me
	others := make([]*Peer, 0, len(room.peers)-1)
	for pid, p := range room.peers {
		if pid != id {
			others = append(others, p)
		}
	}
	tracks := make([]*PublishedTrack, 0, len(room.tracks))
	for _, t := range room.tracks {
		tracks = append(tracks, t)
	}
	room.mu.Unlock()

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		b, _ := json.Marshal(c.ToJSON())
		_ = send(me, Signal{Type: "ice", Data: b})
	})

	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		codec := track.Codec()
		local, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
			MimeType: codec.MimeType, ClockRate: codec.ClockRate, Channels: codec.Channels, SDPFmtpLine: codec.SDPFmtpLine,
		}, track.ID(), id)
		if err != nil {
			log.Printf("track %s: %v", track.ID(), err)
			return
		}

		publishedID := id + ":" + track.ID()
		room.mu.Lock()
		room.tracks[publishedID] = &PublishedTrack{ownerID: id, trackID: track.ID(), local: local}
		room.mu.Unlock()

		for _, target := range othersSnapshot(room, id) {
			if sender, err := target.pc.AddTrack(local); err == nil {
				target.mu.Lock()
				if !target.closed {
					target.subscriptions[publishedID] = sender
				}
				target.mu.Unlock()
				renegotiate(target)
			}
		}

		buf := make([]byte, 1500)
		for {
			n, _, readErr := track.Read(buf)
			if readErr != nil {
				break
			}
			var pkt rtp.Packet
			if pktErr := pkt.Unmarshal(buf[:n]); pktErr == nil {
				_ = local.WriteRTP(&pkt)
			}
		}

		removePublication(room, publishedID)
	})

	for _, t := range tracks {
		if t.ownerID == id {
			continue
		}
		if sender, err := pc.AddTrack(t.local); err != nil {
			log.Printf("initial track %s: %v", t.trackID, err)
		} else {
			me.mu.Lock()
			me.subscriptions[t.ownerID+":"+t.trackID] = sender
			me.mu.Unlock()
		}
	}

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			removePeer(me)
		}
	})

	b, _ := json.Marshal(map[string]any{"peers": peerIDs(others), "tracks": len(tracks)})
	_ = send(me, Signal{Type: "joined", RoomID: room.id, PeerID: id, Data: b})
	for _, other := range others {
		_ = send(other, Signal{Type: "peer-joined", PeerID: id})
	}
	// The browser's initial offer/answer establishes the connection. Existing
	// remote tracks are already attached above and are included in that answer.
	// Server-initiated renegotiation starts only after the connection is stable.
	for {
		var msg Signal
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}
		switch msg.Type {
		case "offer", "answer":
			var desc webrtc.SessionDescription
			if json.Unmarshal(msg.Data, &desc) != nil {
				continue
			}
			if msg.Type == "offer" {
				// Block server-initiated renegotiation while answering the
				// browser's offer. Any track changes observed by OnTrack are
				// queued and renegotiated after this exchange becomes stable.
				me.mu.Lock()
				if me.closed || me.negotiating {
					me.mu.Unlock()
					continue
				}
				me.negotiating = true
				me.mu.Unlock()

				if pc.SetRemoteDescription(desc) != nil {
					negotiationFinished(me)
					continue
				}
				answer, e := pc.CreateAnswer(nil)
				if e != nil {
					negotiationFinished(me)
					continue
				}
				if pc.SetLocalDescription(answer) != nil {
					negotiationFinished(me)
					continue
				}
				b, _ := json.Marshal(pc.LocalDescription())
				_ = send(me, Signal{Type: "answer", Data: b})
				negotiationFinished(me)
			} else {
				if pc.SetRemoteDescription(desc) == nil {
					negotiationFinished(me)
				}
			}
		case "ice":
			var candidate webrtc.ICECandidateInit
			if json.Unmarshal(msg.Data, &candidate) == nil {
				_ = pc.AddICECandidate(candidate)
			}
		}
	}
	removePeer(me)
}

func othersSnapshot(room *Room, self string) []*Peer {
	room.mu.RLock()
	defer room.mu.RUnlock()
	out := make([]*Peer, 0, len(room.peers))
	for id, p := range room.peers {
		if id != self {
			out = append(out, p)
		}
	}
	return out
}

func renegotiate(p *Peer) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if p.negotiating {
		p.negotiationPending = true
		p.mu.Unlock()
		return
	}
	p.negotiating = true
	p.mu.Unlock()

	go func() {
		offer, err := p.pc.CreateOffer(nil)
		if err != nil {
			negotiationFinished(p)
			return
		}
		if err = p.pc.SetLocalDescription(offer); err != nil {
			negotiationFinished(p)
			return
		}
		b, err := json.Marshal(p.pc.LocalDescription())
		if err != nil {
			negotiationFinished(p)
			return
		}
		if err = send(p, Signal{Type: "offer", Data: b}); err != nil {
			negotiationFinished(p)
		}
	}()
}

func negotiationFinished(p *Peer) {
	p.mu.Lock()
	p.negotiating = false
	pending := p.negotiationPending && !p.closed
	p.negotiationPending = false
	p.mu.Unlock()
	if pending {
		renegotiate(p)
	}
}

func removePeer(p *Peer) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.negotiating = false
	p.negotiationPending = false
	p.mu.Unlock()
	if p.room == nil {
		return
	}
	p.room.mu.Lock()
	delete(p.room.peers, p.id)
	removedTracks := make([]string, 0)
	for id, t := range p.room.tracks {
		if t.ownerID == p.id {
			removedTracks = append(removedTracks, id)
			delete(p.room.tracks, id)
		}
	}
	remaining := make([]*Peer, 0, len(p.room.peers))
	for _, other := range p.room.peers {
		remaining = append(remaining, other)
	}
	empty := len(p.room.peers) == 0
	p.room.mu.Unlock()

	for _, other := range remaining {
		for _, publishedID := range removedTracks {
			if removeSubscription(other, publishedID) {
				renegotiate(other)
			}
		}
		_ = send(other, Signal{Type: "peer-left", PeerID: p.id})
	}
	_ = p.pc.Close()
	if empty {
		roomsMu.Lock()
		delete(rooms, p.room.id)
		roomsMu.Unlock()
	}
}

func removePublication(room *Room, publishedID string) {
	room.mu.Lock()
	if _, ok := room.tracks[publishedID]; !ok {
		room.mu.Unlock()
		return
	}
	delete(room.tracks, publishedID)
	remaining := make([]*Peer, 0, len(room.peers))
	for _, p := range room.peers {
		remaining = append(remaining, p)
	}
	room.mu.Unlock()

	for _, target := range remaining {
		if removeSubscription(target, publishedID) {
			renegotiate(target)
		}
	}
}

func removeSubscription(p *Peer, publishedID string) bool {
	p.mu.Lock()
	sender, ok := p.subscriptions[publishedID]
	if ok {
		delete(p.subscriptions, publishedID)
	}
	closed := p.closed
	p.mu.Unlock()
	if !ok || closed {
		return false
	}
	if err := p.pc.RemoveTrack(sender); err != nil {
		log.Printf("remove track %s from peer %s: %v", publishedID, p.id, err)
		return false
	}
	return true
}

func peerIDs(peers []*Peer) []string {
	ids := make([]string, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, p.id)
	}
	return ids
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "p-unknown"
	}
	return "p-" + hex.EncodeToString(b)
}
