# How to use this app

Commands assume the **repository root** (the folder that contains `venv/` and `ahacatcher/`).

A browser version of this guide is served at **`/how-to-use.html`** when [uvicorn](#2-start-the-web-server-uvicorn) is running.

---

## 1. Start the Python virtual environment (venv)

In Terminal:

```bash
cd /path/to/cursor-project
source venv/bin/activate
```

Your prompt should show `(venv)`. To leave: `deactivate`.

You can skip `activate` if you always call tools via `./venv/bin/python3` or `./venv/bin/uvicorn` (full path).

---

## 2. Start the web server (uvicorn)

Run from the **repository root** so imports and `.env` load correctly.

### Foreground (typical development)

```bash
cd /path/to/cursor-project
./venv/bin/uvicorn ahacatcher.server:app --reload --port 8765
```

LAN / phone access on the same network:

```bash
./venv/bin/uvicorn ahacatcher.server:app --host 0.0.0.0 --reload --port 8765
```

Leave this terminal window open. **Server log** (requests, errors) prints in this window.

### Keep running after you close the terminal (macOS)

- **Quick:** `nohup ./venv/bin/uvicorn ahacatcher.server:app --host 0.0.0.0 --port 8765 >> ~/Library/Logs/ahacatcher-uvicorn.log 2>&1 &`, or use **`tmux`** / **`screen`** and detach.
- **Persistent:** install a **launchd** LaunchAgent (see **`ahacatcher/scripts/uvicorn_launchd.plist.example`** and the **Run the web app** section in **`README.md`**).

Summary for **launchd**:

1. Stop any uvicorn already on port **8765** (avoid *address already in use*).
2. Copy the example plist to **`~/Library/LaunchAgents/com.YOURNAME.ahacatcher-uvicorn.plist`**, set **`WorkingDirectory`** to the repo root and **`ProgramArguments`** to your **`venv/bin/uvicorn`** + `ahacatcher.server:app` + `--host` / `--port` (example plist has **no `--reload`**).
3. Load: `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.YOURNAME.ahacatcher-uvicorn.plist`
4. Stop: `launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.YOURNAME.ahacatcher-uvicorn.plist`
5. Restart after code changes: `launchctl kickstart -k gui/$(id -u)/com.YOURNAME.ahacatcher-uvicorn`

**Do not** run a second uvicorn in Terminal while launchd already serves **8765**. If Voice Memos **Import** fails, add the plist’s **`venv/bin/uvicorn`** path to **Full Disk Access** (same as for Terminal).

---

## 3. Open the web pages

With uvicorn running (foreground or via launchd), use your browser:

| Page | URL |
|------|-----|
| Main app | http://127.0.0.1:8765/ |
| Topic map | http://127.0.0.1:8765/map.html |
| Usage metrics | http://127.0.0.1:8765/metrics.html |
| Background monitor | http://127.0.0.1:8765/background-monitor.html |

From another device, replace `127.0.0.1` with your Mac’s LAN IP or Tailscale IP.

---

## 4. Check history & past activity

- **Saved notes & transcripts** — use **Topic map** (`map.html`) and open notes by tag or detail.
- **Classifier / save usage (local)** — open **Usage metrics** (`metrics.html`); data is stored in `ahacatcher/data/metrics.jsonl` on this Mac.
- **Voice Memos mirror + quick-memo pipeline** — open **Background monitor** (`background-monitor.html`) for recent sync/scan rows and quick-memo events.

---

## 5. Logs (web server & background jobs)

- **Uvicorn / app (foreground)** — the terminal where you started uvicorn shows HTTP errors and `[ahacatcher]` stderr lines.
- **Uvicorn (launchd)** — if you configured **`StandardOutPath`** / **`StandardErrorPath`** in the plist, e.g. `~/Library/Logs/ahacatcher-uvicorn.log` and `~/Library/Logs/ahacatcher-uvicorn.err.log`.
- **Other launchd jobs** (optional) — e.g. `~/Library/Logs/voice-memos-quick-memo.log` and `voice-memos-quick-memo.err.log` for sync + quick-memo scan.
