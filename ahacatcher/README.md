# Aha! Catcher (MVP)

Web UI for recording voice, transcribing, and optionally fetching an AI research summary. The HTML is served by a small **FastAPI** app that proxies the AI Builder API using your repo `.env` key.

## Prerequisites

- Python **3.9+** (match your project venv)
- Dependencies installed in the **project root** venv (parent of this folder), e.g. `fastapi`, `uvicorn`, `httpx`, `python-dotenv`
- **`AI_BUILDER_API_KEY`** (or `AI_BUILDER_TOKEN`) set in **`cursor-project/.env`** at the **repository root** (not inside `ahacatcher/`)

## Activate the virtual environment

The `venv` directory lives next to this folder, under the repo root:

```bash
cd /path/to/cursor-project          # repository root (contains venv/ and ahacatcher/)
source venv/bin/activate             # macOS / Linux
```

Your prompt should show `(venv)`. To leave the venv:

```bash
deactivate
```

**Windows (PowerShell):**

```powershell
cd C:\path\to\cursor-project
.\venv\Scripts\Activate.ps1
```

## Run the web app

**Always run uvicorn from the repository root** so Python can import `ahacatcher.server` and load `.env`:

```bash
cd /path/to/cursor-project
source venv/bin/activate             # optional if you use the full path below
./venv/bin/uvicorn ahacatcher.server:app --reload --port 8765
./venv/bin/uvicorn ahacatcher.server:app --host 0.0.0.0 --reload --port 8765  #局域网设置iphone和mac同时登陆时候用
```

Or, after `activate`:

```bash
uvicorn ahacatcher.server:app --reload --port 8765
```

### Keep the server running after you close the terminal (macOS)

- **Quick:** `nohup ./venv/bin/uvicorn … >> ~/Library/Logs/ahacatcher-uvicorn.log 2>&1 &`, or run uvicorn inside **`tmux`** / **`screen`** so you can detach.
- **Persistent (login autostart + restart on crash):** use **launchd** with a **LaunchAgent** plist in **`~/Library/LaunchAgents/`**.

**launchd — suggested steps**

1. **Stop** any uvicorn already listening on **8765** (e.g. `Ctrl+C` in the terminal where it runs), or you will get *address already in use*.
2. Copy **[`ahacatcher/scripts/uvicorn_launchd.plist.example`](scripts/uvicorn_launchd.plist.example)** to **`~/Library/LaunchAgents/com.YOURNAME.ahacatcher-uvicorn.plist`** and edit it:
   - Set **`WorkingDirectory`** to the **repository root** (the folder that contains `venv/` and `ahacatcher/`), so **`.env`** loads the same way as when you run uvicorn by hand.
   - Set **`ProgramArguments`** to the **absolute** path of **`venv/bin/uvicorn`**, then `ahacatcher.server:app`, **`--host`**, **`0.0.0.0`** (LAN/phone) or **`127.0.0.1`** (this Mac only), **`--port`**, **`8765`**. The template plist uses **`bash -lc 'cd … && exec …'`** if you prefer that style.
   - Point **`StandardOutPath`** / **`StandardErrorPath`** to log files (example: **`~/Library/Logs/ahacatcher-uvicorn.log`** and **`ahacatcher-uvicorn.err.log`**).
3. **Load** the agent (modern macOS):

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.YOURNAME.ahacatcher-uvicorn.plist
```

**Unload / stop:**

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.YOURNAME.ahacatcher-uvicorn.plist
```

**Restart** the service after you change app code (the example plist has **no `--reload`**):

```bash
launchctl kickstart -k gui/$(id -u)/com.YOURNAME.ahacatcher-uvicorn
```

**Notes**

- **Only one** uvicorn should own port **8765** — do not start a second copy in Terminal while launchd is running the service.
- **Full Disk Access:** if **Import** / Voice Memos mirror fails under launchd, add the **`venv/bin/uvicorn`** path used in the plist to **System Settings → Privacy & Security → Full Disk Access** (same idea as running uvicorn in Terminal).

## Open the app in a browser

With **uvicorn** running on port **8765** (see above), open one of these in your browser:

| Page | URL |
|------|-----|
| **Main app** (recording, transcript, classify, save) | [http://127.0.0.1:8765/](http://127.0.0.1:8765/) or [http://localhost:8765/](http://localhost:8765/) |
| **Topic map** (dynamic scan of saved `*.md` by category & tags) | [http://127.0.0.1:8765/map.html](http://127.0.0.1:8765/map.html) or [http://localhost:8765/map.html](http://localhost:8765/map.html) |
| **By tag** (list notes with transcript; links to full note) | Open from topic map by clicking a tag, or [http://127.0.0.1:8765/records-by-tag.html?tag=YOUR_TAG](http://127.0.0.1:8765/records-by-tag.html?tag=shopping) |
| **Note detail** (full Markdown file) | [http://127.0.0.1:8765/record-detail.html?file=name.md](http://127.0.0.1:8765/record-detail.html?file=name.md) (usually opened from **By tag** → **details**) |
| **Fix category (phone)** | [http://127.0.0.1:8765/category-fix.html?file=name.md](http://127.0.0.1:8765/category-fix.html?file=name.md) — mobile page to change YAML `category` (see **iPhone / Watch 通知** below) |
| **Metrics** (tables & confusion matrix) | [http://127.0.0.1:8765/metrics.html](http://127.0.0.1:8765/metrics.html) or [http://localhost:8765/metrics.html](http://localhost:8765/metrics.html) |
| **Background monitor** (mirror sync + scan + quick-memo transcripts, last days) | [http://127.0.0.1:8765/background-monitor.html](http://127.0.0.1:8765/background-monitor.html) |
| **How to use** (venv, uvicorn, pages, history, logs) | Web: [how-to-use.html](http://127.0.0.1:8765/how-to-use.html) — local copy: [`ahacatcher/how-to-use.md`](how-to-use.md) |

The same process serves the static **`index.html`** at **`/`**, **`map.html`**, **`records-by-tag.html`**, **`record-detail.html`**, **`category-fix.html`**, **`metrics.html`**, **`background-monitor.html`** (launchd mirror + scan status), **`how-to-use.html`** (short usage guide), and the API routes under **`/api/*`** (including **`/api/classify`** for automatic category + tags after transcribe, **`/api/topics-map`** for the topic map, **`/api/notes-by-tag?tag=`** for records under a tag, **`/api/note-full?file=`** for the raw note body, **`/api/note-meta?file=`** for frontmatter summary, **`/api/note-category`** to update `category` with **`X-Aha-Push-Token`**, **`/api/quick-memo`** for iPhone Shortcuts uploads + quick classify + ntfy). The **Save note** section shows **How it was classified**: the model’s short **`why`** plus **`model`**, **`input_chars`**, and **`used_chars`** (if the transcript was truncated for classification, see **`AHA_CATCHER_CLASSIFY_MAX_CHARS`**).

## iPhone / Apple Watch 通知（每次保存到本机 Ideas 后）

使用 **[ntfy](https://ntfy.sh)**：在 iPhone 上安装 **ntfy** App 并订阅你与 Mac 约定好的 **topic** 名称后，同一 Apple ID 下的 **Apple Watch** 会镜像这些推送（系统「通知」行为取决于你对手表上 ntfy 的设置）。

1. 在 **`cursor-project/.env`** 增加下面变量后 **重启 uvicorn**。
2. **每次** 通过 **`/api/save-note`** 成功写入至少一个 Ideas 目录时，服务器会向 ntfy 发一条消息（标题为笔记 `title`，正文含当前分类与文件名）。
3. **`AHA_CATCHER_OPEN_BASE`**：填 iPhone **能访问到的** Mac 地址（例如同一 Wi‑Fi 下 `http://192.168.1.20:8765`，或 **Tailscale** 的 `http://100.x.x.x:8765`）。推送里的 **点击链接** 会打开 **`category-fix.html`**，可在手机浏览器里改分类并写回 Markdown。
4. **`AHA_CATCHER_PUSH_ACTION_SECRET`**：一串**只有你自己知道**的长随机字符串；用于 **`POST /api/note-category`** 的请求头 **`X-Aha-Push-Token`**（通知链接里会带 `token=` 查询参数，也可在页面上手动粘贴）。请勿提交到公开仓库。
5. 若未设置 **`AHA_CATCHER_NTFY_TOPIC`**，不会发送任何推送（其余功能不变）。

| Variable | 含义 |
|----------|------|
| **`AHA_CATCHER_NTFY_TOPIC`** | ntfy 主题名（与手机上订阅的一致）。设置后启用保存后推送。 |
| **`AHA_CATCHER_NTFY_SERVER`** | 可选，默认 **`https://ntfy.sh`**。自建 ntfy 时改为你的服务器根 URL。 |
| **`AHA_CATCHER_NTFY_TOKEN`** | 可选；若 ntfy 主题启用了 **登录/访问控制** ，这里填 Bearer token。 |
| **`AHA_CATCHER_OPEN_BASE`** | 手机浏览器打开本服务时的 **基地址**（无尾斜杠），用于推送中的「校正分类」链接。 |
| **`AHA_CATCHER_PUSH_ACTION_SECRET`** | 校正分类写回 API 的共享密钥；与 **`X-Aha-Push-Token`** / URL **`token=`** 一致。 |

**说明：** 从手表上**直接在通知里改** YAML 受系统限制，通常需要 **轻点通知 → 在 iPhone 上打开链接** 完成校正；本实现优先保证「省配置、可写回 Obsidian/iCloud 文件夹」而不是单独开发原生 iOS App + APNs。

### 快捷指令：`POST /api/quick-memo`（手机上传录音 → 截前几秒 → 分类 → ntfy）

用于 **Watch / iPhone 语音备忘录**：由快捷指令把**音频文件** POST 到 Mac（Tailscale 等），服务端用 **ffmpeg** 截取前 **`AHA_CATCHER_QUICK_MEMO_TRIM_SEC` 秒**（默认 **15**）→ 与网页相同链路 **转写 + 分类** → **发 ntfy**（仅 **message** 一行：`VoiceMemo, 分类, 转写开头…`）→ **若已配置 `AHA_CATCHER_IDEAS_DIR`（及可选镜像）且未关闭 `AHA_CATCHER_QUICK_MEMO_SAVE_IDEAS`**，会写入与网页保存**同一套** YAML + `## Transcript` 的 `.md` 到 **iCloud Ideas + 本地镜像**（与 **`/api/save-note`** 相同多目录逻辑）。

| Variable | 含义 |
|----------|------|
| **`AHA_CATCHER_QUICK_MEMO_SECRET`** | 必填；请求头 **`X-Aha-Quick-Memo-Token`** 必须与此一致（另生成一串随机值，勿与 `PUSH_ACTION_SECRET` 混用）。 |
| **`AHA_CATCHER_QUICK_MEMO_TRIM_SEC`** | 可选；截取时长秒数，默认 **15**，范围约 **5–120**。 |
| **`AHA_CATCHER_QUICK_MEMO_MAX_RAW_MB`** | 可选；**截断前**整文件上传上限（MB），默认 **512**（长语音备忘录可先上传再由 ffmpeg 截前几秒）。 |
| **`AHA_CATCHER_QUICK_MEMO_MAX_MB`** | 可选；**截断后**送入转写的音频上限（MB），默认 **32**（ffmpeg 缺失且整段过大时会 413）。 |

**依赖：** 本机已安装 **`ffmpeg`**（推荐 `brew install ffmpeg`）。未安装时会用**整段**音频转写（更慢、更费配额）。

**curl 示例（Mac 上测）：**

```bash
curl -sS -X POST "http://127.0.0.1:8765/api/quick-memo" \
  -H "X-Aha-Quick-Memo-Token: 你的_QUICK_MEMO_SECRET" \
  -F "file=@/path/to/clip.m4a" \
  -F "fingerprint=test-1"
```

响应 JSON 含 **`transcript`**、**`category`**、**`saved_to_ideas`**（成功写入的路径列表）、**`md_filename`** 等；若已配置 **`AHA_CATCHER_NTFY_TOPIC`**，手机 ntfy 仅收到 **一行正文**（`VoiceMemo`，**分类**，**转写片段**），无 title、无 click。

| Variable | 含义 |
|----------|------|
| **`AHA_CATCHER_QUICK_MEMO_LOCAL_URL`** | 可选；本机脚本调用 **`/api/quick-memo`** 时的基地址，默认 **`http://127.0.0.1:8765`**（须先启动 uvicorn）。 |
| **`AHA_CATCHER_QUICK_MEMO_SAVE_IDEAS`** | 可选；为 **`1`**（默认）时，quick-memo 成功后把 Markdown **写入 `AHA_CATCHER_IDEAS_DIR` + 镜像**（与网页保存一致）。设为 **`0`** / **`off`** 则只转写 + ntfy、不写文件。 |
| **`AHA_CATCHER_BACKGROUND_INTERVAL_SEC`** | 可选；与 launchd **`StartInterval`**（秒）一致，用于 **`background-monitor.html`** 估算「下次运行」，默认 **300**（范围 60–86400）。 |

### 选项 C：Mac 镜像目录 + 定时扫描 → quick-memo（免 iPhone 快捷指令）

适合：**Watch/iPhone 语音备忘录 → iCloud → Mac 上出现镜像文件** 后，由 **Mac** 自动对新文件调用 **`/api/quick-memo`** 并发 ntfy（**不**经过手机 Shortcuts）。

1. **电源**：Mac **接电** 且 **睡眠策略** 允许定时任务跑（或整晚不合盖、防睡眠），否则任务睡了就不跑。  
2. **依赖**：本机 **`ffmpeg`**（推荐 `brew install ffmpeg`）；**`uvicorn`** 在跑 **`ahacatcher.server:app`**。  
3. **同步**：定时跑 **`ahacatcher/scripts/sync_voice_memos_mirror.py`**（与现有镜像一致，见下节）。  
4. **快检**：紧接着跑 **`ahacatcher/scripts/quick_memo_scan_mirror.py`** —— 扫描镜像根目录下 **`.m4a` / `.qta` 等**，对**尚未成功 POST 过**的文件调用 **`http://127.0.0.1:8765/api/quick-memo`**（使用 **`.env` 里的 `AHA_CATCHER_QUICK_MEMO_SECRET`**）。已处理路径记在 **`ahacatcher/data/quick_memo_mirror_state.json`**。  
5. **定时**：用 **launchd** 每隔几分钟执行一次「先 sync 再 scan」，示例见 [quick_memo_launchd.plist.example](scripts/quick_memo_launchd.plist.example)。也可只跑 scan（若 sync 已由别的 plist 负责）。

**手动试跑：**

```bash
cd /path/to/cursor-project
./venv/bin/python3 ahacatcher/scripts/quick_memo_scan_mirror.py --dry-run
./venv/bin/python3 ahacatcher/scripts/quick_memo_scan_mirror.py
```

**延迟**：≈ **iCloud 把录音同步到 Mac** + **镜像脚本拷贝** + **定时间隔**；通常 **分钟级**，不是「停录秒推」。

## Voice Memos → mirror + Import (macOS)

Apple 的「语音备忘录」库在受保护路径下；**Import** 默认只读你本机上的**镜像文件夹**，这样运行 `uvicorn` 通常不需要「完全磁盘访问」也能列出录音。

1. **同步（镜像与 App 对齐：增量拷贝 + 默认删除「源已不存在」的镜像文件）**  
   在「语音备忘录」里删除的录音，下次同步会从镜像里删掉对应 `.m4a` / `.qta`（仅针对本次成功扫描到的来源目录；不会动系统库里的文件）。若只想拷贝、**永不删镜像**，加 **`--no-prune`** 或设置环境变量 **`VOICE_MEMOS_MIRROR_NO_PRUNE=1`**。

   在仓库根目录执行（默认目标 **`~/Documents/VoiceMemosMirror`**）：

   ```bash
   cd /path/to/cursor-project
   ./venv/bin/python3 ahacatcher/scripts/sync_voice_memos_mirror.py
   ```

   脚本与说明：[sync_voice_memos_mirror.py](scripts/sync_voice_memos_mirror.py)、[sync_voice_memos_mirror_README.txt](scripts/sync_voice_memos_mirror_README.txt)（完全磁盘访问、launchd 等）。定时同步可复制 [sync_voice_memos_launchd.plist.example](scripts/sync_voice_memos_launchd.plist.example) 到 `~/Library/LaunchAgents/` 后 `launchctl load …`。

   输出在**标准错误**（stderr）：注意 `done: copied=… skipped=… removed=…`。同步后的文件**不在镜像根目录**，而在子路径例如 **`VoiceMemosMirror/VoiceMemos.shared/Recordings/`**（含 **.m4a** 与 **.qta**）。加 **`-v`** 可看到每个复制/删除的路径。

2. **在网页里 Import**  
   打开 **Import** 后，列表来自 **`~/Documents/VoiceMemosMirror`**（及你在 `.env` 里用 **`AHA_CATCHER_VOICE_MEMOS_DIRS`** 追加的目录）。无需再配置与默认相同的路径。

3. **环境变量（可选）**

   | Variable | 含义 |
   |----------|------|
   | **`AHA_CATCHER_VOICE_MEMOS_MIRROR_DIR`** | 覆盖默认镜像路径（默认 `~/Documents/VoiceMemosMirror`） |
   | **`AHA_CATCHER_VOICE_MEMOS_DIRS`** | 额外扫描目录（逗号分隔） |
   | **`AHA_CATCHER_VOICE_MEMOS_USE_MIRROR_ONLY`** | 默认 **`1`**：只扫镜像 + `AHA_CATCHER_VOICE_MEMOS_DIRS`。设为 **`0`** 时恢复扫描系统 Voice Memos 路径（需 FDA） |

## Usage metrics & categorization feedback (local)

For long-term evaluation you get **three views** (all from a local **JSON Lines** file — **no transcript text** is stored):

1. **How many voice captures** — counted in **`/api/transcribe`** (`transcribe_ok`).
2. **How often each category appears** — **predicted**: histogram from each **`classify`** event; **final**: histogram from the **last save per recording session** (Download / Save as… / server save / auto-save).
3. **Implicit “accuracy”** — if **classify** succeeded and you **do not change** the category before saving, that counts as **implicit acceptance**; if you **change** the category before save, that counts as **user correction**. The agreement rate is *accepted / (accepted + corrected)* among saves that had a classifier baseline.

- **Main UI**: **Save note** shows a short summary when metrics are enabled; **Full tables** links to the same URLs as in the table above: [http://127.0.0.1:8765/metrics.html](http://127.0.0.1:8765/metrics.html) (or `localhost`).
- **API**: **`GET /api/metrics/summary`** (JSON), **`POST /api/metrics/record`** (used by the page).
- **Path**: default **`ahacatcher/data/metrics.jsonl`**. Override with **`AHA_CATCHER_METRICS_JSONL=/path/to/file.jsonl`**. Disable with **`AHA_CATCHER_METRICS_JSONL=off`**.

Auto-save may write the same note twice (transcript + summary); **evaluation uses the last `note_saved` row per `workSession`**, so the final category after your edits is what matters.

## Save transcripts as Markdown (YAML + body)

Browsers **cannot** silently write to both iCloud and `~/Documents` for you. **Automatic** dual storage only works when you run the **local** FastAPI server on your Mac and point it at one or two real folders (see below).

You can always export manually:

- **Download .md** — browser Downloads (then move wherever you want).
- **Save as…** — File System Access API (Chrome / Edge); pick **iCloud Drive → Ideas** or any folder.
- **Save on this Mac (server folder)** — calls `/api/save-note` once (same as auto-save, but on demand).

YAML frontmatter includes `title`, **`category`** (see below), optional **`source`** (book / article / URL when **Reading** is selected), **`tags`** (built-in `ahacatcher`, `transcript`, optional `with-summary`, plus **Extra tags** — comma-separated in the UI), `status`, `created`, **`origin: ahacatcher-web`** (where the note was captured — not the book), `has_summary`. Reuse the same extra tag strings across different `category` values to **search across pillars**. The body has `## Transcript` and, if you used **Get research summary**, `## Research summary`.

### Optional Ideas subfolders (six categories)

You can mirror the **Note category** dropdown in Finder under your Ideas root. **Unsorted** is the sixth bucket (inbox): anything that **does not** fit the first five yet.

| Folder (example) | `category` value in YAML |
|------------------|---------------------------|
| `01-life-daily/` | `life-daily` |
| `02-inner/` | `inner` |
| `03-career-learning/` | `career-learning` |
| `04-reading/` | `reading` (+ optional YAML `source:` for title/URL) |
| `05-pilates/` | `pilates` |
| `06-unsorted/` | `unsorted` |

The app still saves `.md` files into whatever directory you set in `.env` (flat by default). **Move** new notes into the matching subfolder by hand (or with a script) using `category` in the frontmatter. **Unsorted** is the inbox: review later and re-file or retag.

### Automatic save (transcript + again after summary)

When server-side folders are configured, the app **POSTs** the same note after a successful transcript and **again** after **Get research summary** (overwrites the same filename so the file gains the summary section). Disable with:

```bash
AHA_CATCHER_AUTO_SAVE=0
```

(Default is on whenever at least one save folder exists.) You can also use the **Auto-save to server folders** checkbox in the Save note section; it is stored in the browser (`localStorage`) and overrides the default until you change it again.

### iCloud + local at once (two folders)

Set a **primary** and an optional **mirror** path. Both must already exist. The server writes the **same** `.md` to each path.

```bash
# iCloud Drive → Ideas (syncs via Apple ID)
AHA_CATCHER_IDEAS_DIR=/Users/you/Library/Mobile Documents/com~apple~CloudDocs/Ideas

# Second copy under Documents (local backup)
AHA_CATCHER_IDEAS_MIRROR_DIR=/Users/you/Documents/Ideas
```

Restart uvicorn. `/api/config` returns `server_save: true` and `save_targets: 2` when both directories resolve. If one path fails to write, the response may still return `200` with a `warnings` list for the failed path.

## Troubleshooting

| Issue | What to check |
|--------|----------------|
| **Address already in use** / bind error on **8765** | Something else (often a **second** uvicorn) is using the port. Stop the terminal uvicorn **or** `launchctl bootout …` the LaunchAgent — only one listener. |
| `No such file ... ./venv/bin/uvicorn` | You are probably in `ahacatcher/`; `cd ..` to the repo root first. |
| `ModuleNotFoundError: ahacatcher` | Start uvicorn from the **repo root**, not from inside `ahacatcher/`. |
| API / CORS errors | Use the URL above (same origin as the page). Do not open `index.html` via `file://` for full functionality. |
| 503 about API key | Add `AI_BUILDER_API_KEY` to **repo root** `.env` and restart uvicorn. |
| **Transcription failed (500)** or **502** with “Speech-to-text API…” | The AI Builder **`/v1/audio/transcriptions`** call failed on their side (sometimes only `Internal Server Error` in the body). Check the **uvicorn terminal** for `[ahacatcher] transcribe upstream HTTP …` (status, upload bytes, body). Retry later, confirm **API key / quota**, try a **slightly longer** recording, and verify **`AI_BUILDER_BASE_URL`** in `.env` if your docs specify another host. |
| Save on this Mac missing or 503 | Set `AHA_CATCHER_IDEAS_DIR` to an existing directory and restart uvicorn. |
| Only one of two folders gets a file | Check the path exists, permissions, and server log; partial writes return `warnings` in the JSON. |
| Turn off auto-save | Set `AHA_CATCHER_AUTO_SAVE=0` in `.env` and restart uvicorn. |
| **Get research summary** fails or red error under transcript | `supermind-agent-v1` occasionally returns **empty** `message.content` while still billing tokens. The server now sends **`debug: true`**, reads **`orchestrator_trace`**, then **retries** with **`AHA_CATCHER_CHAT_FALLBACK_MODEL`** (default `gemini-2.5-pro`). Override in `.env` if needed. Very long transcripts are trimmed (`AHA_CATCHER_MAX_TRANSCRIPT_CHARS`, default 48000). |
| Category **unsorted** + “Model returned no usable text” | Classifier uses **`debug: true`**, **`max_tokens` 4096**, parses **`orchestrator_trace`** when `message.content` is empty, then retries with **`AHA_CATCHER_CLASSIFY_FALLBACK_MODEL`** if set (else **`AHA_CATCHER_CHAT_FALLBACK_MODEL`** when it differs from **`AHA_CATCHER_CLASSIFY_MODEL`**). Check the **uvicorn terminal** for logged API keys/snippet. |
| **Import** 列表为空 | 先运行 **`ahacatcher/scripts/sync_voice_memos_mirror.py`**（默认写入 **`~/Documents/VoiceMemosMirror`**）或配置 launchd；确认 uvicorn 重启后仍默认 **`AHA_CATCHER_VOICE_MEMOS_USE_MIRROR_ONLY=1`**。 |
| Category/tags wrong (but classify ran) | Set **`AHA_CATCHER_CLASSIFY_MODEL`** / fallback; tune **`AHA_CATCHER_CLASSIFY_MAX_CHARS`** for long transcripts. |
| Save as… disabled | Use **Download .md**, or a Chromium-based browser with File System Access support. |

## Project layout (relevant bits)

```text
cursor-project/
  .env                 # AI_BUILDER_API_KEY; optional AHA_CATCHER_CHAT_MODEL, idea dirs, etc.
  venv/
  ahacatcher/
    README.md          # this file
    index.html         # front-end
    server.py          # FastAPI app (static + /api/transcribe, /api/research)
```
