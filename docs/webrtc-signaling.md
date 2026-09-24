# языкOn WebRTC signaling protocol

The signaling server is intentionally transport-only. It does not process audio or video.

## WebSocket

Endpoint:

`ws://HOST:3000/ws`

Production will use:

`wss://HOST/ws`

## Join

Client sends:

```json
{
  "type": "join",
  "roomId": "ABC123"
}
```

Server responds:

```json
{
  "type": "joined",
  "roomId": "ABC123",
  "peerId": "server-generated-id",
  "peers": []
}
```

For a two-party room, the joining client creates the WebRTC offer when an existing peer is returned.

## Offer

```json
{
  "type": "offer",
  "roomId": "ABC123",
  "peerId": "target-peer-id",
  "data": {}
}
```

## Answer

```json
{
  "type": "answer",
  "roomId": "ABC123",
  "peerId": "target-peer-id",
  "data": {}
}
```

## ICE

```json
{
  "type": "ice",
  "roomId": "ABC123",
  "peerId": "target-peer-id",
  "data": {}
}
```

## Leave

Closing the WebSocket removes the peer from the room and broadcasts `peer-left`.

## Media roadmap

Current:

```
Peer A <---- WebRTC ----> Peer B
              ^
              |
          signaling
```

Target:

```
Peer A ----\
Peer B -----+----> языкOn SFU ----> peers
Peer C ----/
```

The signaling protocol will be extended with SFU session identifiers and publication/subscription messages when the custom SFU is introduced.
