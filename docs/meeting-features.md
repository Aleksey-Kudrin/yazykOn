# Meeting features

## Browser E2E
Playwright smoke coverage is provided in `e2e/`. Set `BASE_URL` to an already running deployment or let Playwright start Vite.

## Load testing
`media/loadtest` creates concurrent authenticated WebSocket SFU clients and verifies join responses. Example:
`go run ./loadtest -clients 50 -url ws://127.0.0.1:4000/ws -room LOAD01 -secret "$ROOM_ACCESS_SECRET"`

## Recording
The web client now has a MediaRecorder-based local recording control. It records the local media stream and downloads a WebM file; server-side recording is a separate production phase.

## Captions
The web client has an optional browser SpeechRecognition captions control. Browser support varies; a self-hosted Whisper pipeline remains the production server-side option.

## Breakout rooms
`server/src/breakout` provides a concurrency-safe assignment manager for breakout-room state. It is the state-management foundation; SFU room routing and moderator UI are the next integration layer.
