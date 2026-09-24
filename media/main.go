package main

import (
	"crypto/hmac"
	"crypto/subtle"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"context"
	"sync/atomic"
	"net"
	"os"
	"strconv"
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

type JoinData struct {
	AccessToken string `json:"accessToken"`
}

type AccessClaims struct {
	RoomID string `json:"roomId"`
	UserID string `json:"userId"`
	Role string `json:"role"`
	Exp int64 `json:"exp"`
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
	userID    string
	role      string
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
	waiting         bool
	pendingOffer    *webrtc.SessionDescription
}

type Room struct {
	id     string
	hostID string
	locked bool
	lobby  bool
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
	mediaConnMu sync.Mutex
	mediaConnByIP = map[string]int{}
	activePeers atomic.Int64
	activeRooms atomic.Int64
	upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" { return true }
		for _, allowed := range envList("WEB_ORIGINS") {
			if origin == allowed { return true }
		}
		return false
	}}
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

func maxRoomPeers() int {
	value := 32
	if raw := os.Getenv("MAX_ROOM_PEERS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 2 && parsed <= 500 {
			value = parsed
		}
	}
	return value
}

func getRoom(id string) *Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	if r := rooms[id]; r != nil {
		return r
	}
	r := &Room{id: id, peers: make(map[string]*Peer), tracks: make(map[string]*PublishedTrack)}
	rooms[id] = r
	activeRooms.Add(1)
	return r
}

func verifyRoomAccessToken(roomID, token string) (AccessClaims, bool) {
  var claims AccessClaims
  secret := os.Getenv("ROOM_ACCESS_SECRET")
  if secret == "" || token == "" { return claims, false }
  parts := strings.Split(token, ".")
  if len(parts) != 2 { return claims, false }
  mac := hmac.New(sha256.New, []byte(secret))
  _, _ = mac.Write([]byte(parts[0]))
  expected, err := base64.RawURLEncoding.DecodeString(parts[1])
  if err != nil || !hmac.Equal(mac.Sum(nil), expected) { return claims, false }
  payload, err := base64.RawURLEncoding.DecodeString(parts[0])
  if err != nil || json.Unmarshal(payload, &claims) != nil { return claims, false }
  if strings.ToUpper(claims.RoomID) != strings.ToUpper(roomID) || claims.UserID == "" || (claims.Role != "host" && claims.Role != "cohost" && claims.Role != "member") || claims.Exp < time.Now().Unix() { return claims, false }
  return claims, true
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
	http.HandleFunc("/control/role", handleRoleControl)
	http.HandleFunc("/control/chat", handleChatControl)
  http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
    w.Header().Set("Content-Type", "text/plain; version=0.0.4")
    _, _ = w.Write([]byte("yazykon_media_active_peers " + strconv.FormatInt(activePeers.Load(), 10) + "\n" +
      "yazykon_media_active_rooms " + strconv.FormatInt(activeRooms.Load(), 10) + "\n"))
  })
  srv := &http.Server{Addr: ":4000", Handler: nil}
  go func() {
    log.Printf("языкOn custom SFU listening on :4000, media UDP :50000-50100, public ICE IPs: %v", envList("WEBRTC_PUBLIC_IP"))
    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed { log.Fatal(err) }
  }()
  signals := make(chan os.Signal, 1)
  signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
  <-signals
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second); defer cancel()
  _ = srv.Shutdown(ctx)
  roomsMu.Lock()
  snapshot := make([]*Room, 0, len(rooms)); for _, room := range rooms { snapshot = append(snapshot, room) }
  rooms = map[string]*Room{}
  roomsMu.Unlock()
  for _, room := range snapshot {
    room.mu.RLock(); peers := make([]*Peer, 0, len(room.peers)); for _, p := range room.peers { peers = append(peers, p) }; room.mu.RUnlock()
    for _, p := range peers { removePeer(p) }
  }
}

func mediaControlAuthorized(r *http.Request) bool {
  secret := os.Getenv("MEDIA_CONTROL_SECRET")
  if secret == "" { return false }
  expected := []byte("Bearer " + secret)
  actual := []byte(r.Header.Get("Authorization"))
  return len(actual) == len(expected) && subtle.ConstantTimeCompare(actual, expected) == 1
}

func handleRoleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !mediaControlAuthorized(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var update struct {
		RoomID string `json:"roomId"`
		UserID string `json:"userId"`
		Role string `json:"role"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&update); err != nil || update.RoomID == "" || update.UserID == "" || (update.Role != "cohost" && update.Role != "member") {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	room := getRoom(update.RoomID)
	room.mu.RLock()
	var target *Peer
	peers := make([]*Peer, 0, len(room.peers))
	for _, p := range room.peers {
		if p.userID == update.UserID { target = p }
		if !p.waiting { peers = append(peers, p) }
	}
	room.mu.RUnlock()
	if target == nil { w.WriteHeader(http.StatusNotFound); return }
	target.mu.Lock()
	if target.role != "host" { target.role = update.Role }
	newRole := target.role
	target.mu.Unlock()
	payload := mustJSON(map[string]string{"userId": target.userID, "role": newRole})
	for _, p := range peers { _ = send(p, Signal{Type: "role-updated", PeerID: target.id, Data: payload}) }
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func handleChatControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !mediaControlAuthorized(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var message map[string]json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&message); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
	var roomID, userID, username, messageID, textValue string
	var timestamp int64
	if json.Unmarshal(message["roomId"], &roomID) != nil || json.Unmarshal(message["userId"], &userID) != nil || json.Unmarshal(message["username"], &username) != nil || json.Unmarshal(message["messageId"], &messageID) != nil || json.Unmarshal(message["text"], &textValue) != nil || json.Unmarshal(message["timestamp"], &timestamp) != nil || roomID == "" || userID == "" || messageID == "" || strings.TrimSpace(textValue) == "" || len([]rune(textValue)) > 2000 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	room := getRoom(roomID)
	room.mu.RLock()
	peers := make([]*Peer, 0, len(room.peers))
	for _, p := range room.peers { if !p.waiting { peers = append(peers, p) } }
	room.mu.RUnlock()
	payload := mustJSON(map[string]any{"id": messageID, "userId": userID, "username": username, "text": strings.TrimSpace(textValue), "timestamp": timestamp})
	for _, p := range peers { _ = send(p, Signal{Type: "chat", Data: payload}) }
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true}`))
}
func handleWS(w http.ResponseWriter, r *http.Request) {
  ip := r.RemoteAddr
  if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil { ip = host }
  maxPerIP := 8
  if raw := os.Getenv("MAX_WS_PER_IP"); raw != "" { if n, err := strconv.Atoi(raw); err == nil && n >= 1 && n <= 100 { maxPerIP = n } }
  mediaConnMu.Lock()
  if mediaConnByIP[ip] >= maxPerIP { mediaConnMu.Unlock(); http.Error(w, "too many connections", http.StatusTooManyRequests); return }
  mediaConnByIP[ip]++
  mediaConnMu.Unlock()
  defer func() { mediaConnMu.Lock(); mediaConnByIP[ip]--; if mediaConnByIP[ip] <= 0 { delete(mediaConnByIP, ip) }; mediaConnMu.Unlock() }()
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

	var join JoinData
	if len(first.Data) > 0 && json.Unmarshal(first.Data, &join) != nil {
		_ = conn.WriteJSON(Signal{Type: "error", Data: mustJSON(map[string]string{"code": "INVALID_JOIN"})})
		return
	}
	claims, accessOK := verifyRoomAccessToken(first.RoomID, join.AccessToken)
	if !accessOK {
		_ = conn.WriteJSON(Signal{Type: "error", Data: mustJSON(map[string]string{"code": "ROOM_ACCESS_DENIED"})})
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

	roomID := strings.ToUpper(strings.TrimSpace(first.RoomID))
	if len(roomID) < 1 || len(roomID) > 64 {
		_ = pc.Close()
		_ = conn.WriteJSON(Signal{Type: "error", Data: mustJSON(map[string]string{"code": "INVALID_ROOM_ID"})})
		return
	}
	room := getRoom(roomID)
	room.mu.Lock()
	if _, exists := room.peers[id]; exists {
		room.mu.Unlock()
		_ = pc.Close()
		_ = conn.WriteJSON(Signal{Type: "error", Data: mustJSON(map[string]string{"code": "DUPLICATE_PEER_ID"})})
		return
	}
	if len(room.peers) >= maxRoomPeers() {
		room.mu.Unlock()
		_ = pc.Close()
		_ = conn.WriteJSON(Signal{Type: "error", Data: mustJSON(map[string]string{"code": "ROOM_FULL"})})
		return
	}
	if room.hostID == "" && claims.Role == "host" {
		room.hostID = id
	}
	if room.hostID == "" {
		room.hostID = id
	}
	hostID := room.hostID
	if room.locked && id != hostID && claims.Role != "host" && claims.Role != "cohost" {
		room.mu.Unlock()
		_ = conn.WriteJSON(Signal{Type: "error", Data: mustJSON(map[string]string{"code": "ROOM_LOCKED"})})
		_ = pc.Close()
		return
	}
	room.mu.Unlock()
	me := &Peer{id: id, userID: claims.UserID, role: claims.Role, room: room, conn: conn, pc: pc, published: make(map[string]*webrtc.TrackLocalStaticRTP), subscriptions: make(map[string]*webrtc.RTPSender)}

	room.mu.Lock()
	me.waiting = room.lobby && claims.Role != "host" && claims.Role != "cohost"
	room.peers[id] = me
	activePeers.Add(1)
	others := make([]*Peer, 0, len(room.peers)-1)
	for pid, p := range room.peers {
		if pid != id && !p.waiting {
			others = append(others, p)
		}
	}
	isWaiting := me.waiting
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

	if !isWaiting {
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
	}

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			removePeer(me)
		}
	})

	room.mu.RLock()
	locked := room.locked
	lobby := room.lobby
	room.mu.RUnlock()
	if isWaiting {
		_ = send(me, Signal{Type: "lobby-waiting", RoomID: room.id, PeerID: id})
		if host := findPeer(room, hostID); host != nil { _ = send(host, Signal{Type: "lobby-join", PeerID: id}) }
	} else {
		peerMeta := make(map[string]map[string]string, len(others)+1)
		peerMeta[id] = map[string]string{"userId": me.userID, "role": me.role}
		for _, p := range others { peerMeta[p.id] = map[string]string{"userId": p.userID, "role": p.role} }
		b, _ := json.Marshal(map[string]any{"peers": peerIDs(others), "peerMeta": peerMeta, "tracks": len(tracks), "hostId": hostID, "locked": locked, "lobby": lobby})
		_ = send(me, Signal{Type: "joined", RoomID: room.id, PeerID: id, Data: b})
		for _, other := range others { _ = send(other, Signal{Type: "peer-joined", PeerID: id, Data: mustJSON(map[string]string{"userId": me.userID, "role": me.role})}) }
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
				me.mu.Lock()
				if me.waiting {
					me.pendingOffer = &desc
					me.mu.Unlock()
					continue
				}
				me.mu.Unlock()
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
			if json.Unmarshal(msg.Data, &cmd) != nil || cmd.Action == "" { continue }
			if !canModerate(me, nil, cmd.Action) {
				_ = send(me, Signal{Type: "error", Data: mustJSON(map[string]string{"code": "MODERATION_DENIED"})})
				continue
			}
			if cmd.Action == "lobby-on" || cmd.Action == "lobby-off" {
				room.mu.Lock()
				room.lobby = cmd.Action == "lobby-on"
				lobby := room.lobby
				room.mu.Unlock()
				kind := "lobby-off"
				if lobby { kind = "lobby-on" }
				room.mu.RLock()
				for _, p := range room.peers { if !p.waiting { _ = send(p, Signal{Type: kind}) } }
				room.mu.RUnlock()
				continue
			}
			if cmd.Action == "approve" || cmd.Action == "deny" {
				target := findPeer(room, cmd.PeerID)
				if target == nil { continue }
				target.mu.Lock()
				waiting := target.waiting
				target.mu.Unlock()
				if !waiting { continue }
				if cmd.Action == "deny" {
					_ = send(target, Signal{Type: "lobby-denied"})
					removePeer(target)
					continue
				}
				target.mu.Lock()
				target.waiting = false
				pendingOffer := target.pendingOffer
				target.pendingOffer = nil
				target.mu.Unlock()
				room.mu.RLock()
				active := make([]*Peer, 0)
				approvedTracks := make([]*PublishedTrack, 0, len(room.tracks))
				for id, p := range room.peers { if id != target.id && !p.waiting { active = append(active, p) } }
				for _, t := range room.tracks { approvedTracks = append(approvedTracks, t) }
				locked, lobby := room.locked, room.lobby
				room.mu.RUnlock()
				for _, t := range approvedTracks {
					if sender, err := target.pc.AddTrack(t.local); err == nil {
						target.mu.Lock(); target.subscriptions[t.ownerID+":"+t.trackID] = sender; target.mu.Unlock()
						_ = send(target, Signal{Type: "track-published", PeerID: t.ownerID, Data: mustJSON(map[string]string{"trackId": t.trackID})})
					}
				}
				b, _ := json.Marshal(map[string]any{"peers": peerIDs(active), "tracks": len(approvedTracks), "hostId": roomHostID(room), "locked": locked, "lobby": lobby})
				_ = send(target, Signal{Type: "joined", RoomID: room.id, PeerID: target.id, Data: b})
				for _, other := range active { _ = send(other, Signal{Type: "peer-joined", PeerID: target.id}) }
				if pendingOffer != nil {
					target.mu.Lock(); target.negotiating = true; target.mu.Unlock()
					if target.pc.SetRemoteDescription(*pendingOffer) == nil {
						flushPendingICE(target)
						if answer, err := target.pc.CreateAnswer(nil); err == nil && target.pc.SetLocalDescription(answer) == nil {
							data, _ := json.Marshal(target.pc.LocalDescription()); _ = send(target, Signal{Type: "answer", Data: data})
						}
					}
					negotiationFinished(target)
				}
				continue
			}
			if cmd.Action == "lock" || cmd.Action == "unlock" {
				room.mu.Lock(); room.locked = cmd.Action == "lock"; locked := room.locked; room.mu.Unlock()
				kind := "room-unlocked"; if locked { kind = "room-locked" }
				room.mu.RLock(); for _, target := range room.peers { _ = send(target, Signal{Type: kind}) }; room.mu.RUnlock()
				continue
			}
			if (cmd.Action != "remove" && cmd.Action != "mute") || cmd.PeerID == "" || cmd.PeerID == me.id {
				_ = send(me, Signal{Type: "error", Data: mustJSON(map[string]string{"code": "MODERATION_DENIED"})}); continue
			}
			target := findPeer(room, cmd.PeerID)
			if target != nil {
				if !canModerate(me, target, cmd.Action) { _ = send(me, Signal{Type: "error", Data: mustJSON(map[string]string{"code": "MODERATION_DENIED"}) }); continue }
				if cmd.Action == "mute" { _ = send(target, Signal{Type: "muted", Data: mustJSON(map[string]string{"by": me.id}) })
				} else { _ = send(target, Signal{Type: "removed", Data: mustJSON(map[string]string{"reason": "removed_by_host"})}); removePeer(target) }
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

func canModerate(actor, target *Peer, action string) bool {
  if actor == nil || (actor.role != "host" && actor.role != "cohost") { return false }
  if target != nil && target.role == "host" && actor.role != "host" { return false }
  return action == "remove" || action == "mute" || action == "lobby-on" || action == "lobby-off" || action == "approve" || action == "deny" || action == "lock" || action == "unlock"
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
		if id != self && !p.waiting {
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
	activePeers.Add(-1)
	removedTracks := make([]string, 0)
	for id, t := range p.room.tracks {
		if t.ownerID == p.id {
			removedTracks = append(removedTracks, id)
			delete(p.room.tracks, id)
		}
	}
	remaining := make([]*Peer, 0, len(p.room.peers))
	for _, other := range p.room.peers {
		if !other.waiting {
			remaining = append(remaining, other)
		}
	}
	newHost := ""
	if p.room.hostID == p.id {
		for _, candidate := range remaining { if candidate.role == "host" { newHost = candidate.id; break } }
		if newHost == "" { for _, candidate := range remaining { if candidate.role == "cohost" { newHost = candidate.id; break } } }
		if newHost == "" && len(remaining) > 0 { newHost = remaining[0].id }
		if newHost != "" {
			p.room.hostID = newHost
		} else {
			p.room.hostID = ""
		}
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
		activeRooms.Add(-1)
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
