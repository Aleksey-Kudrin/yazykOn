# языкOn

Self-hosted video conferencing platform — a Zoom-like system for Proxmox.

## Project

**Name:** языкOn  
**Repository:** `Aleksey-Kudrin/yazykOn`

## Stack

- Web: TypeScript + React
- Backend: TypeScript + Node.js
- Desktop: Electron + TypeScript
- Android: Kotlin
- Media: Jitsi Videobridge / WebRTC
- TURN: coturn
- Database: PostgreSQL
- Cache/presence: Redis
- Reverse proxy: Nginx
- Deployment: Docker Compose on Proxmox LXC

## Planned clients

- Browser
- Windows
- Android

## Initial goals

- Video and audio calls
- Multi-user conference rooms
- Screen sharing
- Participant list
- Text chat
- Room passwords
- Lobby
- Authentication
- Russian UI with internationalization support
- Self-hosted deployment

## Repository structure

```text
server/       Backend API
web/          Browser client
desktop/      Windows client
android/      Android client
deploy/       Proxmox/Docker deployment
docs/         Architecture and API documentation
.github/      CI/CD
```

## Development

The project is being developed incrementally, starting with the server foundation and web client.

## License

TBD.
