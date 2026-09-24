# языкOn

Self-hosted video conferencing platform for Proxmox.

## Architecture

языкOn is built without Jitsi. The project uses native WebRTC and a custom signaling layer. A custom SFU is the next media milestone.

```
Browser / Desktop / Android
          |
       WebRTC
          |
   языкOn Signaling
          |
      Custom SFU
          |
       RTP/RTCP
```

## Stack

- Web: TypeScript + React
- Signaling/API: Node.js + TypeScript + Express + WebSocket
- Media: native WebRTC
- SFU: custom, planned
- Desktop: Electron + TypeScript
- Android: Kotlin
- Database: PostgreSQL, planned
- Cache/presence: Redis, planned
- TURN: coturn, planned
- Reverse proxy: Nginx, planned
- Deployment: Docker Compose on Proxmox LXC

## Current milestone

The web client now has a native WebRTC 1-to-1 call:

- room creation;
- WebSocket signaling;
- camera and microphone;
- local and remote video;
- mute/unmute;
- camera on/off;
- ICE candidate exchange;
- WebRTC offer/answer.

There is no Jitsi dependency.

Rooms and signaling are currently in memory. The next milestone is a custom SFU for multi-user conferences.

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

Set `VITE_API_URL` when the API is hosted separately.

## Planned features

- Custom SFU
- Multi-user conferences
- Screen sharing
- Participant list
- Text chat
- Room passwords
- Lobby
- Authentication
- PostgreSQL persistence
- Redis presence
- TURN
- Windows desktop client
- Android client
- Russian UI with internationalization

## Repository structure

```
server/       API + WebSocket signaling
web/          Browser client
desktop/      Windows client
android/      Android client
deploy/       Proxmox/Docker deployment
docs/         Architecture and API documentation
.github/      CI/CD
```

## License

TBD.
