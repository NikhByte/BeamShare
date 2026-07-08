# Beamshare — Beam + Gaze

> **Unstoppable P2P file transfer that works even on restricted college Wi-Fi.**

---

## Architecture: Three-Legged System

```
┌─────────────────────────────────────────────────────────┐
│  Tier A: beam CLI (Go)        → Sender                  │
│  Tier B: Gaze Web UI (HTML)   → Receiver (Zero-install) │
│  Tier C: Signaling (WS)       → Handshake Coordinator   │
└─────────────────────────────────────────────────────────┘
```

## Development Phases

| Phase | Status | Description |
|:------|:-------|:------------|
| **Phase 1** | Active | CLI Core — local HTTP server + static page embedding |
| **Phase 2** | Pending | mDNS Discovery — `beam.local` broadcasting |
| **Phase 3** | Pending | WebRTC Transfer — `RTCDataChannel` streaming |
| **Phase 4** | Pending | Optical Signaling — QR code SDP handshake |
| **Phase 5** | Pending | Live Pipe — `cat logs.txt | beam` stdin streaming |

## Project Structure

```
Beamshare/
├── cmd/
│   └── beam/
│       └── main.go           <- CLI entry point
├── internal/
│   ├── server/
│   │   └── server.go         <- HTTP server logic
│   ├── assets/
│   │   └── assets.go         <- Embedded static files
│   └── mdns/
│       └── mdns.go           <- mDNS broadcaster (Phase 2)
├── web/                      <- Gaze receiver UI
│   ├── index.html
│   ├── style.css
│   └── app.js
├── go.mod
├── go.sum
└── README.md
```

## Quick Start (Phase 1)

```bash
# Build
cd Beamshare
go build -o beam ./cmd/beam

# Run -- serves the Gaze receiver UI on a random local port
./beam send myfile.pdf
```

## Design Principles

- **Privacy First:** STUN/TURN only for handshake. File data is Peer-to-Peer.
- **Security:** Self-signed certs with fingerprint embedded in QR code.
- **UX:** Beautiful QR code printed in terminal via UTF-8 blocks.
- **Portability:** Single binary. No install on the receiver side.
- **Zero Limits:** Direct-to-Disk chunked streaming bypasses browser memory and file size limits.

