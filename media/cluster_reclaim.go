package main

import (
	"context"
	"log"
	"strings"
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
		claimed, err := client.SetNX(ctx, ownerKey, payload, sfuRoomOwnerTTL()).Result()
		if err != nil {
			continue
		}
		if claimed {
			failoverClaims.Add(1)
			log.Printf("SFU room ownership reclaimed: room=%s node=%s", roomID, nodeID)
		}
	}
	if err := iter.Err(); err != nil {
		return
	}
}
