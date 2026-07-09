# Twitch Watch-Streak & Presence Bot

A Docker-based hybrid bot that maintains Twitch watch streaks, channel points, and chat presence for your subscribed streamers while you're away from your PC.

## Features

- **Go-live detection** via Twitch EventSub WebSocket + Helix polling fallback
- **Email notifications** when streamers go live (SMTP)
- **Watch streaks / channel points** via minute-watched events (API layer)
- **Chat presence** via IRC for all live monitored streamers
- **Raid following** — detected via IRC while in chat; switches watch slots automatically
- **Browser safety net** — headless Playwright tab on the top-priority stream
- **10-minute watchdog** — refreshes watch sessions and browser player
- **Health endpoint** at `http://localhost:8080/health` and `/status`

## Quick Start

### 1. Register a Twitch App

1. Go to [dev.twitch.tv/console/apps](https://dev.twitch.tv/console/apps)
2. Create an application, set OAuth redirect to `http://localhost`
3. Copy **Client ID** and **Client Secret**

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your Twitch and SMTP credentials

# Edit config/streamers.yaml with your streamer logins
```

### 3. Authenticate (one-time)

```bash
docker compose run --rm bot python -m src.main auth
```

Visit the URL shown and enter the device code in your browser.

### 4. Run

```bash
docker compose up -d
docker compose logs -f
```

Check health: `curl http://localhost:8080/status`

## Configuration

### `config/streamers.yaml`

```yaml
streamers:
  - login: your_streamer
    priority: streak    # streak | subscribed | order
    watch_streak: true
    subscribed: true
```

### Priority

Twitch only credits **2 streams at a time** for channel points. The scheduler picks the two highest-priority live streamers.

| Priority | Weight | Use when |
|---|---|---|
| `streak` | 100 | You care most about watch streak |
| `subscribed` | 50 | You're a sub (gets extra weight) |
| `order` | 10 | Default / lower priority |

### Environment Variables

See [`.env.example`](.env.example). Key settings:

| Variable | Default | Description |
|---|---|---|
| `POLL_INTERVAL_SECONDS` | 60 | Helix poll fallback interval |
| `WATCHDOG_INTERVAL_SECONDS` | 600 | Session refresh interval |
| `BROWSER_ENABLED` | true | Headless browser safety net |
| `HEALTH_PORT` | 8080 | Health check HTTP port |
| `LOG_LEVEL` | INFO | Set to `DEBUG` for verbose reconnect traces |

## Architecture

```
EventSub WS + Poll  →  Scheduler (2 slots)  →  minute-watched API
                      ↓                        IRC chat (all live)
                      ↓                        Playwright (top slot)
                      SMTP email on go-live
```

## Important Notes

- **EventSub WebSocket limit**: Twitch allows only 10 subscription points per WebSocket session. With 5 streamers, `stream.online` + `stream.offline` uses the full budget. Raids are detected via IRC instead.
- **Two-stream limit**: Don't watch other Twitch streams on your phone/PC while the bot runs, or streak credit may conflict. The bot logs `watch_conflict_detected` when minute-watched requests fail repeatedly.
- **PC must stay on**: Docker container runs on your PC. Use `restart: unless-stopped` so it recovers after reboots.
- **Personal use**: This is for maintaining your own watch streaks, equivalent to leaving tabs open.

## Commands

```bash
# Authenticate
docker compose run --rm bot python -m src.main auth

# Send a test email (verify SMTP config)
docker compose run --rm bot python -m src.main test-email

# Run in foreground (debug)
docker compose up

# Run detached
docker compose up -d

# View logs
docker compose logs -f

# Stop
docker compose down
```

## Local Development (without Docker)

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
playwright install chromium
python -m src.main auth
python -m src.main run
```
