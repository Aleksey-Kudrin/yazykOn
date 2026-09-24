package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/ice/v4"
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

type ChatMessage struct {
	Text string `json:"text"`
}

type ModerationCommand struct {
	Action string `json:"action"`
	PeerID string `json:"peerId"`
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
	pendingICE      []webrtc.ICECandidateInit
}

type Room struct {
	id     string
	hostID string
	locked bool
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
	if publicIPs := envList("WEBRTC_PUBLIC_IP"); len(publicIPs) > 0 {
		settings.SetNAT1To1IPs(publicIPs, webrtc.ICECandidateTypeHost)
		settings.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	}
	if err := settings.SetEphemeralUDPPortRange(mediaUDPMin, mediaUDPMax); err != nil {
		return nil, err
	}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))
	return api.NewPeerConnection(webrtc.Configuration{})
}

func envList(name string) []string {
	var values []string
	for _, value := range strings.Split(os.Getenv(name), ",") {
		value = strings.TrimSpace(value)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
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

	log.Printf("языкOn custom SFU listening on :4000, media UDP :50000-50100, public ICE IPs: %v", envList("WEBRTC_PUBLIC_IP"))
	log.Fatal(http.ListenAndServe(":4000", nil))
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(16 << 10)

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
	room.mu.Lock()
	if room.hostID == "" {
		room.hostID = id
	}
	hostID := room.hostID
	if room.locked && id != hostID {
		room.mu.Unlock()
		_ = conn.WriteJSON(Signal{Type: "error", Data: mustJSON(map[string]string{"code": "ROOM_LOCKED"})})
		_ = pc.Close()
		return
	}
	room.mu.Unlock()
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
				_ = send(target, Signal{Type: "track-published", PeerID: id, Data: mustJSON(map[string]string{"trackId": track.ID()})})
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
			_ = send(me, Signal{Type: "track-published", PeerID: t.ownerID, Data: mustJSON(map[string]string{"trackId": t.trackID})})
		}
	}

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			removePeer(me)
		}
	})

	b, _ := json.Marshal(map[string]any{"peers": peerIDs(others), "tracks": len(tracks), "hostId": hostID})
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
				flushPendingICE(me)
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
					flushPendingICE(me)
					negotiationFinished(me)
				}
			}
		case "moderate":
			var cmd ModerationCommand
			if json.Unmarshal(msg.Data, &cmd) != nil || cmd.Action == "" {
				continue
			}
			if me.id != roomHostID(room) {
				_ = send(me, Signal{Type: "error", Data: mustJSON(map[string]string{"code": "MODERATION_DENIED"})})
				continue
			}
			if cmd.Action == "lock" || cmd.Action == "unlock" {
				room.mu.Lock()
				room.locked = cmd.Action == "lock"
				locked := room.locked
				room.mu.Unlock()
				kind := "room-unlocked"
				if locked {
					kind = "room-locked"
				}
				room.mu.RLock()
				roomPeers := make([]*Peer, 0, len(room.peers))
				for _, p := range room.peers {
					roomPeers = append(roomPeers, p)
				}
				room.mu.RUnlock()
				for _, target := range roomPeers {
					_ = send(target, Signal{Type: kind})
				}
				continue
			}
			if (cmd.Action != "remove" && cmd.Action != "mute") || cmd.PeerID == "" || cmd.PeerID == me.id {
				_ = send(me, Signal{Type: "error", Data: mustJSON(map[string]string{"code": "MODERATION_DENIED"})})
				continue
			}
			target := findPeer(room, cmd.PeerID)
			if target != nil {
				if cmd.Action == "mute" {
					_ = send(target, Signal{Type: "muted", Data: mustJSON(map[string]string{"by": me.id})})
				} else {
					_ = send(target, Signal{Type: "removed", Data: mustJSON(map[string]string{"reason": "removed_by_host"})})
					removePeer(target)
				}
			}
		case "chat":
			var chat ChatMessage
			if err := json.Unmarshal(msg.Data, &chat); err != nil {
				continue
			}
			chat.Text = strings.TrimSpace(chat.Text)
			if chat.Text == "" {
				continue
			}
			if len([]rune(chat.Text)) > 2000 {
				_ = send(me, Signal{Type: "error", Data: mustJSON(map[string]string{"code": "CHAT_TOO_LARGE"})})
				continue
			}
			payload := mustJSON(map[string]any{"text": chat.Text, "timestamp": time.Now().UnixMilli()})
			room.mu.RLock()
			chatPeers := make([]*Peer, 0, len(room.peers))
			for _, p := range room.peers {
				chatPeers = append(chatPeers, p)
			}
			room.mu.RUnlock()
			for _, target := range chatPeers {
				_ = send(target, Signal{Type: "chat", PeerID: me.id, Data: payload})
			}
		case "ice":
			var candidate webrtc.ICECandidateInit
			if json.Unmarshal(msg.Data, &candidate) == nil {
				me.mu.Lock()
				if me.closed {
					me.mu.Unlock()
					continue
				}
				if pc.RemoteDescription() == nil {
					me.pendingICE = append(me.pendingICE, candidate)
					me.mu.Unlock()
				} else {
					me.mu.Unlock()
					_ = pc.AddICECandidate(candidate)
				}
			}
		}
	}
	removePeer(me)
}

func roomHostID(room *Room) string {
	room.mu.RLock()
	defer room.mu.RUnlock()
	return room.hostID
}

func findPeer(room *Room, id string) *Peer {
	room.mu.RLock()
	defer room.mu.RUnlock()
	return room.peers[id]
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
	newHost := ""
	if p.room.hostID == p.id && len(remaining) > 0 {
		newHost = remaining[0].id
		p.room.hostID = newHost
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
		if newHost != "" {
			_ = send(other, Signal{Type: "host-changed", PeerID: newHost})
		}
	}
	_ = p.pc.Close()
	if empty {
		roomsMu.Lock()
		delete(rooms, p.room.id)
		roomsMu.Unlock()
	}
}

func flushPendingICE(p *Peer) {
	p.mu.Lock()
	pending := append([]webrtc.ICECandidateInit(nil), p.pendingICE...)
	p.pendingICE = nil
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return
	}
	for _, candidate := range pending {
		if err := p.pc.AddICECandidate(candidate); err != nil {
			log.Printf("add buffered ICE candidate for peer %s: %v", p.id, err)
		}
	}
}

func removePublication(room *Room, publishedID string) {
	room.mu.Lock()
	published, ok := room.tracks[publishedID]
	if !ok {
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
			_ = send(target, Signal{Type: "track-removed", PeerID: published.ownerID, Data: mustJSON(map[string]string{"trackId": published.trackID})})
			renegotiate(target)
		}
	}
}

func mustJSON(value any) json.RawMessage {
	b, _ := json.Marshal(value)
	return b
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
