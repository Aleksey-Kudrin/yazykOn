package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type Signal struct {
	Type   string                     `json:"type"`
	RoomID string                     `json:"roomId,omitempty"`
	PeerID string                     `json:"peerId,omitempty"`
	Data   json.RawMessage            `json:"data,omitempty"`
}

type Peer struct {
	id      string
	room    *Room
	conn    *websocket.Conn
	pc      *webrtc.PeerConnection
	mu      sync.Mutex
	pending bool
}

type Room struct {
	id    string
	mu    sync.RWMutex
	peers map[string]*Peer
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	roomsMu sync.Mutex
	rooms   = map[string]*Room{}
)

func getRoom(id string) *Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	r := rooms[id]
	if r == nil {
		r = &Room{id: id, peers: make(map[string]*Peer)}
		rooms[id] = r
	}
	return r
}

func send(p *Peer, msg Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn.WriteJSON(msg)
}

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"service":"yazykOn-sfu"}`))
	})
	http.HandleFunc("/ws", handleWS)

	addr := ":4000"
	log.Printf("языкOn custom SFU listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
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

	api := webrtc.SettingEngine{}
	api.SetICEMulticastDNSMode(webrtc.ICEMulticastDNSModeDisabled)

	me := &Peer{id: id, conn: conn}
	config := webrtc.Configuration{}
	pc, err := webrtc.NewAPI(webrtc.WithSettingEngine(api)).NewPeerConnection(config)
	if err != nil {
		return
	}
	me.pc = pc

	room := getRoom(first.RoomID)
	me.room = room

	room.mu.Lock()
	room.peers[id] = me
	others := make([]*Peer, 0, len(room.peers)-1)
	for pid, p := range room.peers {
		if pid != id {
			others = append(others, p)
		}
	}
	room.mu.Unlock()

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		b, _ := json.Marshal(c.ToJSON())
		_ = send(me, Signal{Type: "ice", PeerID: "", Data: b})
	})

	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		log.Printf("peer=%s published kind=%s id=%s", id, track.Kind(), track.ID())

		local, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{
				MimeType:  track.Codec().MimeType,
				ClockRate: track.Codec().ClockRate,
				Channels:  track.Codec().Channels,
				SDPFmtpLine: track.Codec().SDPFmtpLine,
			},
			track.ID(),
			id,
		)
		if err != nil {
			return
		}

		room.mu.RLock()
		targets := make([]*Peer, 0, len(room.peers))
		for _, p := range room.peers {
			if p.id != id {
				targets = append(targets, p)
			}
		}
		room.mu.RUnlock()

		for _, target := range targets {
			if _, err := target.pc.AddTrack(local); err != nil {
				log.Printf("add track to %s: %v", target.id, err)
				continue
			}
			renegotiate(target)
		}

		buf := make([]byte, 1500)
		for {
			n, _, err := track.Read(buf)
			if err != nil {
				return
			}
			var pkt rtp.Packet
			if err := pkt.Unmarshal(buf[:n]); err != nil {
				continue
			}
			_, _ = local.WriteRTP(&pkt)
		}
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateClosed {
			removePeer(me)
		}
	})

	b, _ := json.Marshal(map[string]any{"peers": peerIDs(others)})
	_ = send(me, Signal{Type: "joined", RoomID: room.id, PeerID: id, Data: b})

	for _, other := range others {
		_ = send(other, Signal{Type: "peer-joined", PeerID: id})
	}

	for {
		var msg Signal
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}
		switch msg.Type {
		case "offer", "answer":
			var desc webrtc.SessionDescription
			if err := json.Unmarshal(msg.Data, &desc); err != nil {
				continue
			}
			if msg.Type == "offer" {
				if err := pc.SetRemoteDescription(desc); err != nil {
					continue
				}
				answer, err := pc.CreateAnswer(nil)
				if err != nil {
					continue
				}
				if err = pc.SetLocalDescription(answer); err != nil {
					continue
				}
				b, _ := json.Marshal(pc.LocalDescription())
				_ = send(me, Signal{Type: "answer", Data: b})
			} else {
				_ = pc.SetRemoteDescription(desc)
			}
		case "ice":
			var candidate webrtc.ICECandidateInit
			if err := json.Unmarshal(msg.Data, &candidate); err == nil {
				_ = pc.AddICECandidate(candidate)
			}
		}
	}

	removePeer(me)
}

func renegotiate(p *Peer) {
	p.mu.Lock()
	if p.pending {
		p.mu.Unlock()
		return
	}
	p.pending = true
	p.mu.Unlock()

	go func() {
		defer func() {
			p.mu.Lock()
			p.pending = false
			p.mu.Unlock()
		}()

		offer, err := p.pc.CreateOffer(nil)
		if err != nil {
			return
		}
		if err = p.pc.SetLocalDescription(offer); err != nil {
			return
		}
		b, _ := json.Marshal(p.pc.LocalDescription())
		_ = send(p, Signal{Type: "offer", Data: b})
	}()
}

func removePeer(p *Peer) {
	if p.room == nil {
		return
	}
	p.room.mu.Lock()
	delete(p.room.peers, p.id)
	remaining := make([]*Peer, 0, len(p.room.peers))
	for _, other := range p.room.peers {
		remaining = append(remaining, other)
	}
	p.room.mu.Unlock()

	for _, other := range remaining {
		_ = send(other, Signal{Type: "peer-left", PeerID: p.id})
	}
	_ = p.pc.Close()
}

func peerIDs(peers []*Peer) []string {
	ids := make([]string, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, p.id)
	}
	return ids
}

func newID() string {
	return "p-" + randomToken()
}

func randomToken() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 10)
	for i := range b {
		b[i] = chars[i%len(chars)]
	}
	return string(b)
}
