# языкOn

Self-hosted video conferencing platform for Proxmox, built around native WebRTC and a custom Go/Pion SFU.

## Architecture

языкOn does not depend on Jitsi. The browser connects to the custom SFU over WebSocket signaling and WebRTC media.

```
Browser / Desktop / Android
          |
       WebRTC
          |
   языкOn signaling
          |
    Custom Go/Pion SFU
          |
       RTP/RTCP
```

## Stack

- Web: TypeScript + React + Vite
- API/control plane: Node.js + TypeScript + Express
- Media plane: Go + Pion WebRTC
- Signaling: WebSocket
- Reverse proxy: Nginx
- Deployment: Docker Compose on Proxmox LXC
- PostgreSQL, Redis and coturn: planned
- Desktop: Electron, planned
- Android: Kotlin, planned

## Current milestone

The project has a working custom SFU prototype:

- room creation through the API;
- optional room passwords with HMAC-signed access tokens;
- browser WebRTC connection to the SFU;
- camera and microphone;
- local video;
- multi-participant remote media fan-out;
- peer lifecycle cleanup;
- serialized SFU renegotiation;
- deterministic SFU UDP media range: `50000-50100`;
- Nginx WebSocket/media proxying;
- Docker Compose deployment;
- GitHub Actions builds for server, web and media.

The media server exposes `/health` and `/ws`. Room metadata and password hashes are currently kept in memory. Password-protected rooms use an HMAC-signed access token shared between the API and media plane; PostgreSQL-backed persistence and full user authentication are still planned.

## Development

### Server

```bash
cd server
npm install
npm run dev
```

### Web

```bash
cd web
npm install
npm run dev
```

### Media SFU

```bash
cd media
go mod tidy
go run .
```

For the Docker deployment, the web application uses `/media` as the SFU WebSocket path and exposes UDP media ports `50000-50100`.

## Planned roadmap

1. SFU lifecycle and track-removal correctness
2. ICE/STUN/TURN configuration and NAT traversal
3. Participant list, screen sharing and chat
4. Host/co-host controls, lobby and room locking
5. Authentication and PostgreSQL persistence
6. Redis presence/pub-sub
7. Recording and optional captions
8. Breakout rooms
9. Security hardening and rate limits
10. Playwright/browser, Go and load testing
11. Windows desktop client
12. Android client

## Repository structure

```
server/       API + control/signaling layer
media/        Custom Go/Pion SFU
web/          Browser client
desktop/      Windows client
android/      Android client
deploy/       Docker Compose + Nginx
docs/         Architecture and API documentation
.github/      CI
```

## License

TBD.
