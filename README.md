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
- PostgreSQL-backed users, sessions, rooms, memberships, persistent roles and room chat history
- HMAC-signed room access tokens shared by API and SFU
- Redis 8: presence/pub-sub foundation and deployment-ready connection
- `/health` and `/ready` service/readiness endpoints
- coturn
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

The media server exposes `/health`, `/ws` and an authenticated internal role-control endpoint. User accounts, sessions, rooms, room passwords, memberships and roles are persisted in PostgreSQL; the SFU keeps only live room/media state in memory. Room access uses an HMAC-signed token shared between the API and media plane. Registration, login, room creation, room passwords and persistent host/co-host roles are available through the web client.

Production deployment should set `WEB_ORIGINS` to the exact HTTPS origin(s) used by the web client. CORS and both WebSocket endpoints reject origins outside that allowlist. Session cookies are HttpOnly and use `Secure` in production. API JSON bodies and authentication/room endpoints have bounded request sizes and rate limits.

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

## Definition of done

языкOn is considered release-ready when the following are implemented and verified:

1. SFU/WebRTC negotiation, track lifecycle, ICE/TURN, room capacity and disconnect recovery are covered by automated tests.
2. Authentication, persistent rooms/memberships/roles, authorization, rate limits, origin restrictions and audit-safe error handling are production-hardened.
3. Meeting UX includes participant management, chat, screen sharing, lobby, room locking and reliable reconnect behavior.
4. Persistent chat/history, presence and multi-instance coordination are implemented.
5. Recording, captions and breakout rooms are implemented behind explicit feature controls.
6. Browser E2E, Go unit/integration, API and load tests run in CI.
7. Docker/Proxmox deployment has health checks, TLS, secrets, firewall/port documentation and backup/restore procedures.
8. Windows desktop and Android clients use the stable meeting protocol and pass the same interoperability tests.

## Roadmap

1. SFU lifecycle and negotiation hardening
2. Security, authorization and automated API/media tests
3. Redis presence/pub-sub and persistent chat
4. Recording and optional captions
5. Breakout rooms
6. Browser E2E and synthetic SFU load testing
7. Production Docker/Proxmox installer, backup/restore and release documentation
8. Windows desktop client
9. Android client

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
