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
	"runtime"
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
	"github.com/redis/go-redis/v9"
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
	Reconnect   bool   `json:"reconnect,omitempty"`
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
	sessionID string
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
	ownerID   string
	trackID   string
	sessionID string
	local     *webrtc.TrackLocalStaticRTP
}

var (
	clusterMu sync.RWMutex
	clusterRedis *redis.Client
	clusterNodeID string
	clusterReady atomic.Bool
	clusterLastSync atomic.Int64
	mediaConnMu sync.Mutex
	mediaConnByIP = map[string]int{}
	activePeers atomic.Int64
	activeRooms atomic.Int64
	failoverClaims atomic.Int64
	failoverOwnerRedirects atomic.Int64
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


func clusterPeerKey(roomID, peerID string) string {
	return "yazykon:sfu:peer:" + roomID + ":" + peerID
}

func clusterNodeKey(nodeID string) string {
	return "yazykon:sfu:node:" + nodeID
}

func clusterChannel(roomID string) string {
	return "yazykon:sfu:room:" + roomID
}

func clusterOwnerKey(roomID string) string {
	return "yazykon:sfu:owner:" + roomID
}

func clusterRoomStateKey(roomID string) string {
	return "yazykon:sfu:state:" + roomID
}
func clusterRoomTracksKey(roomID string) string {
	return "yazykon:sfu:tracks:" + roomID
}

type clusterTrackState struct {
	PeerID    string `json:"peerId"`
	TrackID   string `json:"trackId"`
	SessionID string `json:"sessionId"`
	Kind      string `json:"kind"`
	UpdatedAt int64 `json:"updatedAt"`
}

func clusterSaveTrackState(roomID, peerID, trackID, sessionID, kind string) {
	clusterMu.RLock()
	client := clusterRedis
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() { return }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := clusterRoomTracksKey(roomID)
	state := clusterTrackState{PeerID: peerID, TrackID: trackID, SessionID: sessionID, Kind: kind, UpdatedAt: time.Now().Unix()}
	const saveScript = `redis.call("HSET", KEYS[1], ARGV[1], ARGV[2]); return redis.call("PEXPIRE", KEYS[1], ARGV[3])`
	_, _ = client.Eval(ctx, saveScript, []string{key}, peerID+":"+trackID, mustJSON(state), "90000").Result()
}

func clusterRemoveTrackState(roomID, peerID, trackID, sessionID string) {
	clusterMu.RLock()
	client := clusterRedis
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() { return }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	const script = `local current = redis.call("HGET", KEYS[1], ARGV[1])
if not current then return 0 end
local ok, obj = pcall(cjson.decode, current)
if not ok or obj.sessionId ~= ARGV[2] then return 0 end
return redis.call("HDEL", KEYS[1], ARGV[1])`
	_ = client.Eval(ctx, script, []string{clusterRoomTracksKey(roomID)}, peerID+":"+trackID, sessionID).Err()
}

func clusterDeleteTrackState(roomID string) {
	clusterMu.RLock()
	client := clusterRedis
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() { return }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.Del(ctx, clusterRoomTracksKey(roomID)).Err()
}

func clusterLoadTrackStates(roomID string) []clusterTrackState {
	clusterMu.RLock()
	client := clusterRedis
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	values, err := client.HGetAll(ctx, clusterRoomTracksKey(roomID)).Result()
	if err != nil { return nil }
	states := make([]clusterTrackState, 0, len(values))
	for _, raw := range values {
		var state clusterTrackState
		if json.Unmarshal([]byte(raw), &state) == nil && state.PeerID != "" && state.TrackID != "" {
			states = append(states, state)
		}
	}
	return states
}


type clusterRoomState struct {
	RoomID string `json:"roomId"`
	HostID string `json:"hostId"`
	Locked bool `json:"locked"`
	Lobby bool `json:"lobby"`
	UpdatedAt int64 `json:"updatedAt"`
}

func clusterSaveRoomState(room *Room) {
	clusterMu.RLock()
	client := clusterRedis
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() { return }
	room.mu.RLock()
	state := clusterRoomState{RoomID: room.id, HostID: room.hostID, Locked: room.locked, Lobby: room.lobby, UpdatedAt: time.Now().Unix()}
	room.mu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.Set(ctx, clusterRoomStateKey(room.id), mustJSON(state), 90*time.Second).Err()
}

func clusterLoadRoomState(roomID string) (clusterRoomState, bool) {
	clusterMu.RLock()
	client := clusterRedis
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() { return clusterRoomState{}, false }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	raw, err := client.Get(ctx, clusterRoomStateKey(roomID)).Result()
	if err != nil { return clusterRoomState{}, false }
	var state clusterRoomState
	if json.Unmarshal([]byte(raw), &state) != nil { return clusterRoomState{}, false }
	return state, true
}

func clusterDeleteRoomState(roomID string) {
	clusterMu.RLock()
	client := clusterRedis
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() { return }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.Del(ctx, clusterRoomStateKey(roomID)).Err()
}

func clusterNodeEndpoint() string {
	return strings.TrimSpace(os.Getenv("SFU_WS_URL"))
}

func clusterClaimRoom(roomID string) (bool, string) {
	clusterMu.RLock()
	client := clusterRedis
	nodeID := clusterNodeID
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() {
		return true, ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := clusterOwnerKey(roomID)
	payload := mustJSON(map[string]any{"nodeId": nodeID, "endpoint": clusterNodeEndpoint()})
	claimed, err := client.SetNX(ctx, key, payload, sfuRoomOwnerTTL()).Result()
	if err != nil {
		log.Printf("SFU Redis room claim failed: %v", err)
		return false, ""
	}
	if claimed {
		failoverClaims.Add(1)
		return true, ""
	}
	current, err := client.Get(ctx, key).Result()
	if err != nil {
		return false, ""
	}
	var owner struct { NodeID string `json:"nodeId"`; Endpoint string `json:"endpoint"` }
	if json.Unmarshal([]byte(current), &owner) != nil {
		return false, ""
	}
	if owner.NodeID != nodeID {
		failoverOwnerRedirects.Add(1)
	}
	return owner.NodeID == nodeID, owner.Endpoint
}

func clusterRenewOwnedRooms() {
	clusterMu.RLock()
	client := clusterRedis
	nodeID := clusterNodeID
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() {
		return
	}
	roomsMu.Lock()
	roomSnapshot := make([]*Room, 0, len(rooms))
	for _, room := range rooms { roomSnapshot = append(roomSnapshot, room) }
	roomsMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, room := range roomSnapshot {
		key := clusterOwnerKey(room.id)
		payload := mustJSON(map[string]any{"nodeId": nodeID, "endpoint": clusterNodeEndpoint()})
		const renewScript = `local current = redis.call("GET", KEYS[1])
if not current then return 0 end
local ok, owner = pcall(cjson.decode, current)
if not ok or owner.nodeId ~= ARGV[1] then return 0 end
redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
return 1`
		_, _ = client.Eval(ctx, renewScript, []string{key}, nodeID, payload, strconv.FormatInt(sfuRoomOwnerTTL().Milliseconds(), 10)).Result()
	}
}

func clusterReleaseRoom(roomID string) {
	clusterMu.RLock()
	client := clusterRedis
	nodeID := clusterNodeID
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() { return }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := clusterOwnerKey(roomID)
	const releaseScript = `local current = redis.call("GET", KEYS[1])
if not current then return 0 end
local ok, owner = pcall(cjson.decode, current)
if not ok or owner.nodeId ~= ARGV[1] then return 0 end
return redis.call("DEL", KEYS[1])`
	_, _ = client.Eval(ctx, releaseScript, []string{key}, nodeID).Result()
}

func clusterInit() func() {
	url := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if url == "" {
		return func() {}
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		log.Printf("SFU Redis disabled: invalid REDIS_URL: %v", err)
		return func() {}
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		_ = client.Close()
		log.Printf("SFU Redis disabled: ping failed: %v", err)
		return func() {}
	}
	cancel()
	nodeID := strings.TrimSpace(os.Getenv("INSTANCE_ID"))
	if nodeID == "" {
		host, _ := os.Hostname()
		nodeID = host
	}
	if nodeID == "" {
		nodeID = "sfu-" + newID()
	}
	clusterMu.Lock()
	clusterRedis = client
	clusterNodeID = nodeID
	clusterMu.Unlock()
	clusterReady.Store(true)
	clusterLastSync.Store(time.Now().UnixNano())

	go func() {
		ticker := time.NewTicker(sfuClusterHeartbeat())
		defer ticker.Stop()
		for range ticker.C {
			clusterSync()
			clusterRenewOwnedRooms()
		}
	}()
	clusterSync()
	log.Printf("SFU shared control plane enabled: node=%s", nodeID)

	return func() {
		clusterReady.Store(false)
		clusterMu.Lock()
		client := clusterRedis
		clusterRedis = nil
		clusterMu.Unlock()
		if client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = client.Del(ctx, clusterNodeKey(nodeID)).Err()
			cancel()
			_ = client.Close()
		}
	}
}

func clusterSync() {
	clusterMu.RLock()
	client := clusterRedis
	nodeID := clusterNodeID
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	nodePayload := mustJSON(map[string]any{
		"nodeId": nodeID,
		"activePeers": activePeers.Load(),
		"activeRooms": activeRooms.Load(),
		"timestamp": time.Now().Unix(),
	})
	if err := client.Set(ctx, clusterNodeKey(nodeID), nodePayload, 25*time.Second).Err(); err != nil {
		log.Printf("SFU Redis node heartbeat failed: %v", err)
		return
	}
	clusterLastSync.Store(time.Now().UnixNano())

	roomsMu.Lock()
	roomSnapshot := make([]*Room, 0, len(rooms))
	for _, room := range rooms {
		roomSnapshot = append(roomSnapshot, room)
	}
	roomsMu.Unlock()

	for _, room := range roomSnapshot {
		clusterSaveRoomState(room)
		room.mu.RLock()
		peers := make([]*Peer, 0, len(room.peers))
		for _, peer := range room.peers {
			peers = append(peers, peer)
		}
		room.mu.RUnlock()
		for _, peer := range peers {
			peer.mu.Lock()
			closed := peer.closed
			sessionID := peer.sessionID
			userID := peer.userID
			role := peer.role
			peer.mu.Unlock()
			if closed {
				continue
			}
			payload := mustJSON(map[string]any{
				"nodeId": nodeID,
				"roomId": room.id,
				"peerId": peer.id,
				"sessionId": sessionID,
				"userId": userID,
				"role": role,
				"updatedAt": time.Now().Unix(),
			})
			_ = client.Set(ctx, clusterPeerKey(room.id, peer.id), payload, 75*time.Second).Err()
		}
	}
}

func clusterRegisterPeer(peer *Peer) {
	clusterMu.RLock()
	client := clusterRedis
	nodeID := clusterNodeID
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() {
		return
	}
	payload := mustJSON(map[string]any{
		"nodeId": nodeID,
		"roomId": peer.room.id,
		"peerId": peer.id,
		"sessionId": peer.sessionID,
		"userId": peer.userID,
		"role": peer.role,
		"updatedAt": time.Now().Unix(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Set(ctx, clusterPeerKey(peer.room.id, peer.id), payload, 75*time.Second).Err(); err != nil {
		log.Printf("SFU Redis peer register failed: %v", err)
	}
}

func clusterRemovePeer(peer *Peer) {
	clusterMu.RLock()
	client := clusterRedis
	nodeID := clusterNodeID
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() || peer.room == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	const removeScript = `local current = redis.call("GET", KEYS[1])
if not current then return 0 end
local ok, obj = pcall(cjson.decode, current)
if not ok or obj.nodeId ~= ARGV[1] or obj.sessionId ~= ARGV[2] then return 0 end
return redis.call("DEL", KEYS[1])`
	_, _ = client.Eval(ctx, removeScript, []string{clusterPeerKey(peer.room.id, peer.id)}, nodeID, peer.sessionID).Result()
}

func clusterPublish(roomID, eventType, peerID, sessionID string) {
	clusterMu.RLock()
	client := clusterRedis
	nodeID := clusterNodeID
	clusterMu.RUnlock()
	if client == nil || !clusterReady.Load() {
		return
	}
	payload := mustJSON(map[string]any{
		"nodeId": nodeID,
		"type": eventType,
		"roomId": roomID,
		"peerId": peerID,
		"sessionId": sessionID,
		"timestamp": time.Now().Unix(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Publish(ctx, clusterChannel(roomID), payload).Err(); err != nil {
		log.Printf("SFU Redis event publish failed: %v", err)
	}
}

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

func sfuRoomOwnerTTL() time.Duration {
	if raw := os.Getenv("SFU_ROOM_OWNER_TTL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d >= time.Second && d <= 10*time.Minute { return d }
	}
	return 45 * time.Second
}

func sfuClusterHeartbeat() time.Duration {
	if raw := os.Getenv("SFU_CLUSTER_HEARTBEAT"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d >= 500*time.Millisecond && d <= 1*time.Minute { return d }
	}
	return 10 * time.Second
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
	_ = p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := p.conn.WriteJSON(msg)
	_ = p.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		log.Printf("SFU websocket send failed: peer=%s session=%s type=%s err=%v", p.id, p.sessionID, msg.Type, err)
	}
	return err
}

func main() {
  clusterShutdown := clusterInit()
  defer clusterShutdown()
  http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		clusterMu.RLock()
		nodeID := clusterNodeID
		clusterMu.RUnlock()
		lastSync := clusterLastSync.Load()
		clusterHealthy := clusterReady.Load() && lastSync > 0 && time.Since(time.Unix(0, lastSync)) <= 3*sfuClusterHeartbeat()
		payload := map[string]any{"ok": true, "service": "yazykOn-sfu", "version": "0.6.0", "mediaUdpRange": "50000-50100", "nodeId": nodeID, "cluster": clusterHealthy}
		_ = json.NewEncoder(w).Encode(payload)
	})
	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/control/role", handleRoleControl)
	http.HandleFunc("/control/chat", handleChatControl)
  http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
    var mem runtime.MemStats
    runtime.ReadMemStats(&mem)
    w.Header().Set("Content-Type", "text/plain; version=0.0.4")
    _, _ = w.Write([]byte(
      "yazykon_media_active_peers " + strconv.FormatInt(activePeers.Load(), 10) + "\n" +
      "yazykon_media_active_rooms " + strconv.FormatInt(activeRooms.Load(), 10) + "\n" +
      "yazykon_media_failover_claims_total " + strconv.FormatInt(failoverClaims.Load(), 10) + "\n" +
      "yazykon_media_owner_redirects_total " + strconv.FormatInt(failoverOwnerRedirects.Load(), 10) + "\n" +
      "yazykon_media_goroutines " + strconv.Itoa(runtime.NumGoroutine()) + "\n" +
      "yazykon_media_heap_bytes " + strconv.FormatUint(mem.HeapAlloc, 10) + "\n" +
      "yazykon_media_alloc_bytes_total " + strconv.FormatUint(mem.TotalAlloc, 10) + "\n" +
      "yazykon_media_gc_cycles_total " + strconv.FormatUint(uint64(mem.NumGC), 10) + "\n"))
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
func replacePeerSession(old *Peer, room *Room) {
	old.mu.Lock()
	if old.closed {
		old.mu.Unlock()
		return
	}
	old.closed = true
	old.negotiating = false
	old.negotiationPending = false
	old.mu.Unlock()

	room.mu.Lock()
	if current := room.peers[old.id]; current != old {
		room.mu.Unlock()
		return
	}
	delete(room.peers, old.id)
	activePeers.Add(-1)
	removedTracks := make([]string, 0)
	removedTrackStates := make([]PublishedTrack, 0)
	for id, track := range room.tracks {
		if track.ownerID == old.id {
			removedTracks = append(removedTracks, id)
			removedTrackStates = append(removedTrackStates, *track)
			delete(room.tracks, id)
		}
	}
	remaining := make([]*Peer, 0, len(room.peers))
	for _, other := range room.peers {
		if !other.waiting {
			remaining = append(remaining, other)
		}
	}
	room.mu.Unlock()

	clusterRemovePeer(old)

	for _, track := range removedTrackStates {
		clusterRemoveTrackState(room.id, track.ownerID, track.trackID, track.sessionID)
	}
	for _, other := range remaining {
		for _, publishedID := range removedTracks {
			if removeSubscription(other, publishedID) {
				trackID := publishedID
				if index := strings.LastIndex(publishedID, ":"); index >= 0 { trackID = publishedID[index+1:] }
				_ = send(other, Signal{Type: "track-removed", PeerID: old.id, Data: mustJSON(map[string]string{"trackId": trackID})})
				renegotiate(other)
			}
		}
	}
	_ = old.pc.Close()
	_ = old.conn.Close()
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