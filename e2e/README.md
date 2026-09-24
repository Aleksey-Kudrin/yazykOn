# SFU failover E2E

Set `SFU_PRIMARY_URL` and `SFU_SECONDARY_URL` to run the live two-node failover checks.

The harness intentionally skips live failover tests when endpoints are not configured. It does not fake Redis ownership expiry or RTP continuity.

For a real takeover test, run two SFU instances against the same Redis with distinct `INSTANCE_ID` and `SFU_WS_URL` values. Stop the primary, wait for its room-owner lease to expire, and reconnect the browser to the secondary. The expected result is restored room control state followed by client media republish.


## Browser/media SFU failover

The live failover workflow can run the real browser-media probe with:

```bash
SFU_FAILOVER_LIVE=1 SFU_ROOM_OWNER_TTL_MS=3000 ROOM_ACCESS_SECRET=integration-secret REDIS_URL=redis://127.0.0.1:6379 SFU_PRIMARY_URL=http://127.0.0.1:4100 SFU_SECONDARY_URL=http://127.0.0.1:4200 npx playwright test tests/sfu-failover.spec.ts tests/sfu-browser-media-failover.spec.ts
```

The browser probe publishes a fake camera track to SFU A, stops A, waits for the Redis ownership lease to expire, reconnects with the stable peer ID to SFU B, republishes media, and verifies the Redis publication has a new session ID.


### SFU failover diagnostics

The failover workflow archives Docker Compose state, SFU logs, and final `/metrics` snapshots as a GitHub Actions artifact. Failed or slow runs can therefore be inspected without immediately rerunning the scenario.

Latency SLA is controlled by `SFU_FAILOVER_SLA_MS`; repeated samples are summarized as min/avg/p95/max by `sfu-failover-sla.spec.ts`.
