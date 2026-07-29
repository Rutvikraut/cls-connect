# CLS-Connect

Browser-based, view-only screen sharing for phone support — built as a feature inside CLS (Call Logging System).

A support agent needs to see a caller's screen to diagnose an issue over the phone. CLS-Connect does that without installing any software on the caller's device and without giving the agent any remote-control capability — no mouse/keyboard injection, no input channel at all. It exists because AnyDesk/TeamViewer-class remote-access tools are banned under corporate security policy.

## How it works

1. Agent opens the dashboard and clicks **New Session** — the server generates a random 6-digit code.
2. Agent reads the code aloud over the phone (no links sent digitally).
3. Caller opens the fixed URL, types the code, clicks Connect.
4. Caller's browser shares their screen via `getDisplayMedia()`; a WebRTC connection is negotiated through the Go server (which only relays signaling messages, never the video itself).
5. Agent's browser displays the caller's screen live, view-only, in a `<video>` element.
6. A countdown timer runs on both sides; on expiry or manual stop, the caller's screen capture is stopped at the OS level and the session is torn down on both ends.

```
Caller's Browser (shares screen)  ⇄  Go WebSocket Signaling Server  ⇄  Agent's Browser (views only)
                                              │
                                    In-memory session map
                                    (code → session, with expiry)
```

Full technical documentation — code walkthrough, function reference, and design rationale — lives in `docs/`.

## Tech stack

| Layer | Choice |
|---|---|
| Backend / signaling | Go (`net/http` + `gorilla/websocket`) |
| Real-time media | WebRTC (native browser API, P2P, encrypted via DTLS-SRTP) |
| NAT traversal fallback | TURN via Metered (managed relay service) |
| Frontend | Plain HTML/CSS/vanilla JS — no framework, no build step |
| Session storage | In-memory map, `sync.Mutex`-protected, no database |
| Hosting (current) | Render.com |

## Requirements

- Go 1.21+
- A [Metered](https://www.metered.ca/tools/openrelay/) account (or any TURN provider that exposes a compatible REST API) for TURN credentials — optional for local testing, since the app falls back to public STUN if unset

## Getting started

```bash
git clone <repo-url>
cd cls-connect
go mod download
```

Create a `.env` file in the project root (this is git-ignored):

```
METERED_SECRET_KEY=your_metered_secret_key
METERED_APP_DOMAIN=your_app.metered.live
PORT=8080
```

Run the server:

```bash
go run main.go
```

- Agent dashboard: `http://localhost:8080/agent.html`
- Caller page: `http://localhost:8080/`

> WebRTC's `getDisplayMedia()` requires a secure context. `localhost` is treated as secure by browsers, so local HTTP works for development. Any other host needs HTTPS — see Deployment below.

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `PORT` | No (defaults to `8080`) | Port the server listens on. Render sets this automatically. |
| `METERED_SECRET_KEY` | No | Secret key for minting short-lived TURN credentials via Metered's API. Without it, `/api/turn-credentials` returns a 500 and the client falls back to STUN-only. |
| `METERED_APP_DOMAIN` | No | Your Metered app domain (e.g. `yourapp.metered.live`). Required alongside the secret key. |

## Project structure

```
.
├── main.go              # Signaling server: sessions, WebSocket relay, rate limiting, TURN credentials
├── go.mod / go.sum
├── public/
│   ├── index.html        # Caller page — enter code, share screen
│   ├── agent.html         # Agent dashboard — generate code, view stream
│   └── styles.css         # Shared theme, matches CLS UI
└── docs/
    └── cls-connect-technical-documentation.pdf   # Full technical writeup
```

## API endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/create-session` | Generates a new 6-digit session code. Rate-limited per IP (5/min). |
| `GET` | `/api/turn-credentials` | Returns short-lived TURN ICE server credentials. |
| `GET` | `/ws?code=XXXXXX&role=agent\|user` | WebSocket endpoint for signaling. Rate-limited per IP (10/min). |
| `GET` | `/health` | Health check, returns service status and timestamp as JSON. |
| `GET` | `/` | Serves the static frontend from `public/`. |

## Deployment

Currently deployed on Render's free tier, which builds the Go binary directly from this repo and provides a public HTTPS URL out of the box (required — `getDisplayMedia()` and WebSocket-over-TLS both need a secure context outside of `localhost`). Set the environment variables above in Render's dashboard; `PORT` is provided automatically by the platform.

## Known limitations

- **Mobile callers can't share their screen** — `getDisplayMedia()` isn't supported on Android Chrome or iOS Safari for web pages. This is a browser platform limitation, not something fixable in this codebase. The agent side works fine on mobile.
- **No tab-only capture yet** — restricting the caller to sharing a single browser tab (`preferCurrentTab`) instead of the whole screen has been discussed but isn't implemented.
- **No persistent audit log** — session history exists only in server stdout during runtime.
- **No authentication on the agent dashboard** — anyone with the dashboard URL can currently generate sessions. Acceptable for a single-agent MVP.
- **Render free tier** — fine for MVP testing; not intended as the long-term production host (cold starts, not built for real traffic at scale).

## Security notes

- The signaling server only ever handles small JSON messages (SDP offers/answers, ICE candidates) — it never has access to the actual video stream, which flows peer-to-peer (or via TURN) and is encrypted end-to-end by WebRTC (DTLS-SRTP).
- Session codes expire after 30 minutes, or after 5 minutes if never joined by a second party, and are deleted immediately on manual stop — codes can never be reused.
- A session is capped at exactly two connected clients (one agent, one caller); a third party cannot piggyback on a code already in use.
- Per-IP rate limiting on both session creation and WebSocket connection attempts makes brute-forcing a 6-digit code impractical.

## License

Internal tool — not for external distribution.
