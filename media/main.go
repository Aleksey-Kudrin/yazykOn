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
	stopCh := make(chan struct{})
	var stopOnce sync.Once

	go func() {
		ticker := time.NewTicker(sfuClusterHeartbeat())
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				clusterSync()
				clusterRenewOwnedRooms()
			case <-stopCh:
				return
			}
		}
	}()
	clusterSync()
	log.Printf("SFU shared control plane enabled: node=%s", nodeID)

	return func() {
		stopOnce.Do(func() { close(stopCh) })
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
	if client == nil || !clusterReady.Load() || peer == nil || peer.room == nil {
		return
	}
	peer.mu.Lock()
	closed := peer.closed
	roomID := peer.room.id
	peerID := peer.id
	sessionID := peer.sessionID
	userID := peer.userID
	role := peer.role
	peer.mu.Unlock()
	if closed {
		return
	}
	payload := mustJSON(map[string]any{
		"nodeId": nodeID,
		"roomId": roomID,
		"peerId": peerID,
		"sessionId": sessionID,
		"userId": userID,
		"role": role,
		"updatedAt": time.Now().Unix(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Set(ctx, clusterPeerKey(roomID, peerID), payload, 75*time.Second).Err(); err != nil {
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

func findRoom(id string) *Room {
	roomsMu.RLock()
	defer roomsMu.RUnlock()
	return rooms[id]
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
	room := findRoom(update.RoomID)
	if room == nil { w.WriteHeader(http.StatusNotFound); return }
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
	clusterRegisterPeer(target)
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
	room := findRoom(roomID)
	if room == nil { w.WriteHeader(http.StatusNotFound); return }
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
	claimed, ownerEndpoint := clusterClaimRoom(roomID)
	if !claimed {
		_ = pc.Close()
		data := mustJSON(map[string]string{"code": "SFU_ROOM_OWNER", "endpoint": ownerEndpoint})
		_ = conn.WriteJSON(Signal{Type: "error", Data: data})
		return
	}
	room := getRoom(roomID)
	if state, ok := clusterLoadRoomState(roomID); ok {
		room.mu.Lock()
		if len(room.peers) == 0 {
			room.hostID = state.HostID
			room.locked = state.Locked
			room.lobby = state.Lobby
		}
		room.mu.Unlock()
	}
	room.mu.RLock()
	existing := room.peers[id]
	room.mu.RUnlock()
	if existing != nil {
		replacePeerSession(existing, room)
	}
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
	me := &Peer{id: id, sessionID: newID(), userID: claims.UserID, role: claims.Role, room: room, conn: conn, pc: pc, published: make(map[string]*webrtc.TrackLocalStaticRTP), subscriptions: make(map[string]*webrtc.RTPSender)}

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
	clusterRegisterPeer(me)
	clusterSaveRoomState(room)

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
		room.tracks[publishedID] = &PublishedTrack{ownerID: id, trackID: track.ID(), sessionID: me.sessionID, local: local}
		room.mu.Unlock()
		clusterSaveTrackState(room.id, id, track.ID(), me.sessionID, strings.ToLower(codec.MimeType))

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
	removedTrackStates := make([]PublishedTrack, 0)
	for id, t := range p.room.tracks {
		if t.ownerID == p.id {
			removedTracks = append(removedTracks, id)
			removedTrackStates = append(removedTrackStates, *t)
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
	clusterRemovePeer(p)
	for _, track := range removedTrackStates {
		clusterRemoveTrackState(p.room.id, track.ownerID, track.trackID, track.sessionID)
	}
	if empty {
		clusterReleaseRoom(p.room.id)
		clusterDeleteRoomState(p.room.id)
		clusterDeleteTrackState(p.room.id)
	} else {
		clusterSaveRoomState(p.room)
	}

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
	clusterRemoveTrackState(room.id, published.ownerID, published.trackID, published.sessionID)
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
