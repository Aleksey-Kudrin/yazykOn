package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// The main cluster initializer intentionally fails fast when Redis is not ready
// at process startup. In Compose/chaos runs Redis can legitimately become ready
// a little later, so keep a small supervisor that restores the control plane
// without requiring an SFU restart.
func init() {
	if strings.TrimSpace(os.Getenv("REDIS_URL")) == "" {
		return
	}
	go func() {
		time.Sleep(2 * time.Second)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if clusterRedisAvailable() {
				continue
			}
			if clusterReconnectOnce() {
				clusterSync()
			}
		}
	}()
}

func clusterRedisAvailable() bool {
	clusterMu.RLock()
	client := clusterRedis
	clusterMu.RUnlock()
	return client != nil
}

func clusterReconnectOnce() bool {
	url := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if url == "" {
		return false
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return false
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = client.Ping(ctx).Err()
	cancel()
	if err != nil {
		_ = client.Close()
		return false
	}

	nodeID := strings.TrimSpace(os.Getenv("INSTANCE_ID"))
	if nodeID == "" {
		nodeID, _ = os.Hostname()
	}
	if nodeID == "" {
		nodeID = "sfu-" + newID()
	}

	clusterMu.Lock()
	if clusterRedis != nil {
		clusterMu.Unlock()
		_ = client.Close()
		return true
	}
	clusterRedis = client
	clusterNodeID = nodeID
	clusterMu.Unlock()
	clusterReady.Store(true)
	clusterLastSync.Store(time.Now().UnixNano())
	log.Printf("SFU Redis control plane reconnected: node=%s", nodeID)
	return true
}
