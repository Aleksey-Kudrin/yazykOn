# SFU failover E2E

Set `SFU_PRIMARY_URL` and `SFU_SECONDARY_URL` to run the live two-node failover checks.

The harness intentionally skips live failover tests when endpoints are not configured. It does not fake Redis ownership expiry or RTP continuity.

For a real takeover test, run two SFU instances against the same Redis with distinct `INSTANCE_ID` and `SFU_WS_URL` values. Stop the primary, wait for its room-owner lease to expire, and reconnect the browser to the secondary. The expected result is restored room control state followed by client media republish.
