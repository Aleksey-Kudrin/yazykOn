package main

import (
	"context"
	"log"
	"strings"\n\t"strconv"
	"time"
)

// The owner key is deliberately short-lived so a crashed SFU can stop being
// authoritative quickly. A node that is alive must also be able to take over
// rooms that were owned by a node that disappeared without a graceful close.
// Reclamation is done with SETNX, so two healthy nodes cannot both win the
// same expired owner key.
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

		// A Redis outage can make the owner lease disappear even though the
		// original SFU is still alive and reconnecting. Do not let another node
		// reclaim the room in that window. The room snapshot records who last
		// owned it, while the node heartbeat proves that owner is alive.
		// Check both and claim atomically so recovery cannot race with failover.
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
return redis.call("SET", KEYS[1], ARGV[2], "EX", ARGV[3], "NX") and 1 or 0
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
