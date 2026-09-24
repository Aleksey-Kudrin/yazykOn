# языкOn deployment

This deployment runs the current custom WebRTC stack:

- web client;
- Node.js API/signaling;
- custom Go SFU;
- Nginx reverse proxy.

## Start

From the repository root:

```bash
cd deploy
docker compose up -d --build
```

Open:

`http://SERVER_IP:8080`

Health checks:

`http://SERVER_IP:8080/api/health`

SFU health:

`http://SERVER_IP:8080/media/health`

## Media ports

The current SFU is exposed on TCP/UDP port 4000 for the initial development deployment.

Production deployment will use a dedicated media port range and TURN when NAT traversal is required.

## Important

This is the first custom-SFU milestone. Authentication, PostgreSQL persistence, Redis presence, TURN, TLS, observability, rate limiting and production hardening are still separate milestones.
