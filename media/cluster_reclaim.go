package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"
)

// The owner key is deliberately short-lived so a crashed SFU can stop being
authoritative quickly. A node that is alive must also be able to take over
rooms that were owned by a node that disappeared without a graceful close.
// Reclamation is done atomically with the room-state owner update so a
// recovered room cannot keep advertising the previous node as its owner.
func init() {
	go func() {
		// clusterInit() runs from main, so wait until that control plane exists.
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			clusterReclaimExpiredRooms()
		}
	}()
}

func clusterReclaimExpiredRooms() {
	clusterMu.RLock()
	client := clusterRedis
	nodeID := clusterNodeID
	ready := clusterReady.Load()
	clusterMu.RUnlock()
	if client == nil || !ready || strings.TrimSpace(nodeID) == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	iter := client.Scan(ctx, 0, "yazykon:sfu:state:*", 128).Iterator()
	for iter.Next(ctx) {
		stateKey := iter.Val()
		roomID := strings.TrimPrefix(stateKey, "yazykon:sfu:state:")
		if roomID == "" {
			continue
		}
		ownerKey := clusterOwnerKey(roomID)
		payload := redisJSON(map[string]any{
			"nodeId":   nodeID,
			"endpoint": clusterNodeEndpoint(),
		})

		// Redis executes the script atomically. A live previous owner blocks
		// reclamation; a successful claim also changes the persisted room owner
		// in the same transaction, preventing stale ownerNodeId after recovery.
		claimedRaw, err := client.Eval(ctx, `
local ownerRaw = redis.call("GET", KEYS[1])
if ownerRaw then return 0 end
local stateRaw = redis.call("GET", KEYS[2])
if stateRaw then
  local ok, state = pcall(cjson.decode, stateRaw)
  if ok and state.ownerNodeId and state.ownerNodeId ~= "" and state.ownerNodeId ~= ARGV[1] then
    local heartbeatKey = "yazykon:sfu:node:" .. state.ownerNodeId
    if redis.call("EXISTS", heartbeatKey) == 1 then return 0 end
  end
end
local claimed = redis.call("SET", KEYS[1], ARGV[2], "EX", ARGV[3], "NX")
if not claimed then return 0 end
if stateRaw then
  local ok, state = pcall(cjson.decode, stateRaw)
  if ok then
    state.ownerNodeId = ARGV[1]
    local ttl = redis.call("PTTL", KEYS[2])
    redis.call("SET", KEYS[2], cjson.encode(state))
    if ttl > 0 then redis.call("PEXPIRE", KEYS[2], ttl) end
  end
end
return 1
`, []string{ownerKey, stateKey}, nodeID, string(payload), strconv.Itoa(int(sfuRoomOwnerTTL().Seconds()))).Result()
		if err != nil {
			continue
		}
		if claimedRaw == int64(1) {
			failoverClaims.Add(1)
			log.Printf("SFU room ownership reclaimed: room=%s node=%s", roomID, nodeID)
		}
	}
	if err := iter.Err(); err != nil {
		return
	}
}
