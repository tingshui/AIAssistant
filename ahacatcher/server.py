"""
Aha! Catcher local server: serves static `index.html` and proxies AI Builder
transcription + chat using `AI_BUILDER_API_KEY` from the project `.env`.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

HERE = Path(__file__).resolve().parent
AI_BUILDER_BASE = os.getenv("AI_BUILDER_BASE_URL", "https://space.ai-builders.com/backend").rstrip("/")
AI_BUILDER_KEY = (os.getenv("AI_BUILDER_API_KEY") or os.getenv("AI_BUILDER_TOKEN", "")).strip()
CHAT_MODEL = (os.getenv("AHA_CATCHER_CHAT_MODEL") or "supermind-agent-v1").strip() or "supermind-agent-v1"
CHAT_FALLBACK_MODEL = (os.getenv("AHA_CATCHER_CHAT_FALLBACK_MODEL") or "gemini-2.5-pro").strip()
CLASSIFY_MODEL = (os.getenv("AHA_CATCHER_CLASSIFY_MODEL") or "gemini-2.5-pro").strip() or "gemini-2.5-pro"
_raw_cf_fb = (os.getenv("AHA_CATCHER_CLASSIFY_FALLBACK_MODEL") or "").strip()
CLASSIFY_FALLBACK_MODEL = _raw_cf_fb or (
    CHAT_FALLBACK_MODEL if CHAT_FALLBACK_MODEL != CLASSIFY_MODEL else ""
)
NOTE_CATEGORIES_ORDERED = (
    "life-daily",
    "inner",
    "career-learning",
    "reading",
    "pilates",
    "unsorted",
)
NOTE_CATEGORIES = frozenset(NOTE_CATEGORIES_ORDERED)
NOTE_CATEGORIES_LIST = NOTE_CATEGORIES_ORDERED


def _safe_int_env(key: str, default: int, lo: int, hi: int) -> int:
    raw = os.getenv(key)
    if raw is None or not str(raw).strip():
        return default
    try:
        return max(lo, min(hi, int(str(raw).strip())))
    except ValueError:
        return default


CHAT_MAX_TOKENS = _safe_int_env("AHA_CATCHER_CHAT_MAX_TOKENS", 4096, 512, 8192)
MAX_RESEARCH_TRANSCRIPT_CHARS = _safe_int_env("AHA_CATCHER_MAX_TRANSCRIPT_CHARS", 48_000, 8000, 200_000)
MAX_CLASSIFY_TRANSCRIPT_CHARS = _safe_int_env("AHA_CATCHER_CLASSIFY_MAX_CHARS", 16_000, 2000, 100_000)
QUICK_MEMO_TRIM_SEC = _safe_int_env("AHA_CATCHER_QUICK_MEMO_TRIM_SEC", 15, 5, 120)
# Incoming body can be a long Voice Memo; we trim before transcribe. Cap raw upload for DoS safety.
QUICK_MEMO_MAX_RAW_MB = _safe_int_env("AHA_CATCHER_QUICK_MEMO_MAX_RAW_MB", 512, 32, 2048)
QUICK_MEMO_MAX_RAW_BYTES = QUICK_MEMO_MAX_RAW_MB * 1024 * 1024
# Max size of ffmpeg output (or full file if ffmpeg missing/failed) sent to transcribe.
QUICK_MEMO_MAX_CLIP_BYTES = _safe_int_env(
    "AHA_CATCHER_QUICK_MEMO_MAX_MB", 32, 1, 128
) * 1024 * 1024
# Expected launchd/cron interval for background sync + scan (used for “next run” estimate).
BACKGROUND_INTERVAL_SEC = _safe_int_env("AHA_CATCHER_BACKGROUND_INTERVAL_SEC", 300, 60, 86_400)
# Rows returned for background-monitor.html “Mirror & scan log” (latest N within the days window).
BACKGROUND_MONITOR_LOG_UI_MAX = 30

app = FastAPI(title="Aha! Catcher (local)", version="0.1.0")


def _env_flag_true(key: str, default: str = "1") -> bool:
    raw = (os.getenv(key) or default).strip().lower()
    return raw not in ("0", "false", "no", "off")


def _yaml_escape_double(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _yaml_dq(s: str) -> str:
    return '"' + _yaml_escape_double(s) + '"'


def _slug_from_transcript(text: str, max_len: int = 48) -> str:
    t = re.sub(r"\(empty transcript\)", "", text, flags=re.I).strip()
    if not t:
        return "note"
    line = t.splitlines()[0][:max_len]
    slug = re.sub(r"[^\w\u4e00-\u9fff._-]", "-", line)
    slug = re.sub(r"-+", "-", slug).strip("-") or "note"
    return slug[:max_len]


def _quick_memo_md_filename(orig_name: str, transcript: str) -> str:
    # Match web app pattern: ISO-like prefix with colons replaced (safe filename).
    iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    slug = _slug_from_transcript(transcript or "", 48)
    stem = Path(orig_name or "memo.m4a").stem
    stem_clean = re.sub(r"[^\w\u4e00-\u9fff._-]", "-", stem)[:24].strip("-") if stem else ""
    if stem_clean:
        name = f"{iso}-ahacatcher-{stem_clean}-{slug}.md"
    else:
        name = f"{iso}-ahacatcher-{slug}.md"
    return name[:200]


def _fmt_yaml_tags_list(tags: list[str]) -> str:
    return ", ".join(_yaml_dq(t) for t in tags)


def _build_quick_memo_markdown(
    *,
    transcript: str,
    category: str,
    extra_tags: list[str],
    reading_source: Optional[str],
    created_iso: str,
) -> str:
    cat = category if category in NOTE_CATEGORIES else "unsorted"
    first = (transcript or "").splitlines()[0].strip() if transcript else ""
    title = (first[:120] if first else "Voice capture") or "Voice capture"
    tags_merged = ["ahacatcher", "transcript", "voice-memo"]
    for t in extra_tags:
        ts = str(t).strip()
        if ts and ts not in tags_merged:
            tags_merged.append(ts[:80])
    lines = [
        "---",
        f"title: {_yaml_dq(title)}",
        f"category: {cat}",
    ]
    rs = (reading_source or "").strip() if isinstance(reading_source, str) else ""
    if cat == "reading" and rs and rs.lower() != "unknown":
        lines.append(f"source: {_yaml_dq(rs)}")
    lines.extend(
        [
            f"tags: [{_fmt_yaml_tags_list(tags_merged)}]",
            "status: captured",
            f"created: {_yaml_dq(created_iso)}",
            "origin: ahacatcher-voice-memo",
            "has_summary: false",
            "---",
            "",
            "## Transcript",
            "",
            transcript or "",
            "",
        ]
    )
    return "\n".join(lines)


def _ideas_target_path(base: Path, filename: str) -> Path:
    """Same rules as _safe_note_path but raises ValueError instead of HTTPException."""
    name = filename.strip()
    if not name or "/" in name or "\\" in name:
        raise ValueError("Invalid filename")
    if not name.lower().endswith(".md"):
        name = f"{name}.md"
    safe = Path(name).name
    if not safe:
        raise ValueError("Invalid filename")
    target = (base / safe).resolve()
    base_r = base.resolve()
    target.relative_to(base_r)
    return target


def _quick_memo_save_to_ideas_dirs(
    *,
    filename: str,
    content: str,
) -> tuple[list[str], list[str]]:
    """Write the same file to every configured ideas dir (iCloud + mirror)."""
    dirs = _ideas_dirs()
    ok: list[str] = []
    errs: list[str] = []
    for base in dirs:
        try:
            target = _ideas_target_path(base, filename)
            target.write_text(content, encoding="utf-8")
            ok.append(str(target))
        except ValueError as e:
            errs.append(f"{base}: {e}")
        except OSError as e:
            errs.append(f"{base}: {e}")
            print(f"[ahacatcher] quick-memo save: {e}", file=sys.stderr)
    return ok, errs


class ResearchRequest(BaseModel):
    transcript: str = Field(..., min_length=0, max_length=500_000)


class TranscribeLocalFileRequest(BaseModel):
    """Transcribe a file that already exists on the Mac (Voice Memos folder). Path validated server-side."""

    path: str = Field(..., min_length=1, max_length=4096)


class SaveNoteRequest(BaseModel):
    """Write a Markdown note to AHA_CATCHER_IDEAS_DIR (+ optional mirror folder)."""

    filename: str = Field(..., min_length=1, max_length=240)
    content: str = Field(..., min_length=1, max_length=2_000_000)


class NoteCategoryUpdate(BaseModel):
    """Rewrite `category:` in YAML frontmatter for an existing note (all configured idea dirs that contain the file)."""

    filename: str = Field(..., min_length=1, max_length=240)
    category: str = Field(..., min_length=1, max_length=64)


class MetricsRecord(BaseModel):
    """Client- or server-emitted usage / evaluation row (appended as JSON Lines)."""

    event: str = Field(..., min_length=2, max_length=64)
    text_chars: Optional[int] = Field(default=None, ge=0, le=2_000_000)
    category: Optional[str] = Field(default=None, max_length=64)
    model: Optional[str] = Field(default=None, max_length=128)
    predicted_category: Optional[str] = Field(default=None, max_length=64)
    final_category: Optional[str] = Field(default=None, max_length=64)
    user_changed: Optional[bool] = None
    classify_succeeded: Optional[bool] = None
    channel: Optional[str] = Field(default=None, max_length=32)
    has_summary: Optional[bool] = None
    session: Optional[int] = Field(default=None, ge=0, le=10_000_000)
    pass_label: Optional[str] = Field(default=None, max_length=32)


def _ideas_dirs() -> list[Path]:
    """Primary (AHA_CATCHER_IDEAS_DIR) + optional AHA_CATCHER_IDEAS_MIRROR_DIR; deduped, existing dirs only."""
    out: list[Path] = []
    seen: set[str] = set()
    for key in ("AHA_CATCHER_IDEAS_DIR", "AHA_CATCHER_IDEAS_MIRROR_DIR"):
        raw = (os.getenv(key) or "").strip()
        if not raw:
            continue
        p = Path(raw).expanduser().resolve()
        if not p.is_dir():
            print(f"[ahacatcher] save-note skip {key}={raw!r} (not an existing directory)", file=sys.stderr)
            continue
        key_str = str(p)
        if key_str in seen:
            continue
        seen.add(key_str)
        out.append(p)
    return out


def _auto_save_enabled() -> bool:
    v = (os.getenv("AHA_CATCHER_AUTO_SAVE") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _metrics_path() -> Path | None:
    """Local JSONL log for usage & implicit categorization feedback. Disable with AHA_CATCHER_METRICS_JSONL=off."""
    raw = (os.getenv("AHA_CATCHER_METRICS_JSONL") or "").strip()
    if raw.lower() in ("0", "off", "false", "no", "disabled"):
        return None
    if raw:
        p = Path(raw).expanduser()
    else:
        p = HERE / "data" / "metrics.jsonl"
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"[ahacatcher] metrics: cannot create directory {p.parent}: {e}", file=sys.stderr)
        return None
    return p


def _metrics_append(record: dict[str, Any]) -> None:
    path = _metrics_path()
    if path is None:
        return
    row = dict(record)
    row["ts"] = datetime.now(timezone.utc).isoformat()
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"[ahacatcher] metrics: append failed: {e}", file=sys.stderr)


def _background_monitor_path() -> Path:
    return HERE / "data" / "background_monitor.jsonl"


def _background_monitor_read_rows(max_lines: int = 20_000) -> list[dict[str, Any]]:
    path = _background_monitor_path()
    if not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        print(f"[ahacatcher] background_monitor: read failed: {e}", file=sys.stderr)
        return []
    lines = text.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    out: list[dict[str, Any]] = []
    for ln in lines:
        try:
            row = json.loads(ln)
            if isinstance(row, dict):
                out.append(row)
        except json.JSONDecodeError:
            continue
    return out


def _parse_ts_iso(s: object) -> datetime | None:
    if not isinstance(s, str) or not s.strip():
        return None
    t = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _metrics_read_rows(max_lines: int = 80_000) -> list[dict[str, Any]]:
    path = _metrics_path()
    if path is None or not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        print(f"[ahacatcher] metrics: read failed: {e}", file=sys.stderr)
        return []
    lines = text.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    out: list[dict[str, Any]] = []
    for ln in lines:
        try:
            row = json.loads(ln)
            if isinstance(row, dict):
                out.append(row)
        except json.JSONDecodeError:
            continue
    return out


def _strip_yaml_scalar(val: str) -> str:
    s = val.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        inner = s[1:-1]
        return inner.replace('\\"', '"').replace("\\'", "'")
    return s


def _parse_yaml_inline_tags_list(val: str) -> list[str]:
    """Parse `tags: [a, b, "c d"]` value part only (right of `tags:`)."""
    val = val.strip()
    if not (val.startswith("[") and val.endswith("]")):
        return []
    inner = val[1:-1]
    if not inner.strip():
        return []
    parts: list[str] = []
    cur: list[str] = []
    in_dq = False
    in_sq = False
    i = 0
    while i < len(inner):
        c = inner[i]
        if c == '"' and not in_sq:
            in_dq = not in_dq
            cur.append(c)
        elif c == "'" and not in_dq:
            in_sq = not in_sq
            cur.append(c)
        elif c == "," and not in_dq and not in_sq:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
        i += 1
    if cur:
        parts.append("".join(cur).strip())
    return [p for p in (_strip_yaml_scalar(p) for p in parts) if p]


def _parse_note_frontmatter_block(md_raw: str) -> dict[str, Any]:
    """Minimal frontmatter parser (no PyYAML): expects Aha Catcher export shape."""
    text = md_raw.lstrip("\ufeff")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}
    fm_lines: list[str] = []
    i = 1
    while i < len(lines):
        if lines[i].strip() == "---":
            break
        fm_lines.append(lines[i])
        i += 1
    else:
        return {}
    out: dict[str, Any] = {}
    for raw_ln in fm_lines:
        ln = raw_ln.rstrip()
        if not ln or ln.lstrip().startswith("#"):
            continue
        if ":" not in ln:
            continue
        key, _, rhs = ln.partition(":")
        key = key.strip()
        rhs = rhs.strip()
        if key == "tags":
            out["tags"] = _parse_yaml_inline_tags_list(rhs)
        elif key == "category":
            out["category"] = _strip_yaml_scalar(rhs)
        elif key == "title":
            out["title"] = _strip_yaml_scalar(rhs)
        elif key == "created":
            out["created"] = _strip_yaml_scalar(rhs)
    return out


def _body_after_frontmatter(md_raw: str) -> str:
    """Everything after the closing `---` of YAML frontmatter, or full text if no frontmatter."""
    text = md_raw.lstrip("\ufeff")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return text
    i = 1
    while i < len(lines):
        if lines[i].strip() == "---":
            return "\n".join(lines[i + 1 :])
        i += 1
    return text


def _section_under_markdown_heading(body: str, heading_title: str) -> str:
    """
    Text under `## {heading_title}` until the next `## ` heading or EOF.
    Heading line match is case-insensitive.
    """
    title = heading_title.strip()
    if not title:
        return ""
    lines = body.splitlines()
    marker_lower = ("## " + title).lower()
    start: int | None = None
    for idx, ln in enumerate(lines):
        if ln.strip().lower() == marker_lower:
            start = idx + 1
            break
    if start is None:
        return ""
    parts: list[str] = []
    for j in range(start, len(lines)):
        s2 = lines[j].strip()
        if s2.startswith("## ") and s2.lower() != marker_lower:
            break
        parts.append(lines[j])
    return "\n".join(parts).strip()


def _replace_frontmatter_category(md_raw: str, new_category: str) -> str:
    """Replace or insert `category:` inside the first YAML frontmatter block."""
    if new_category not in NOTE_CATEGORIES:
        raise ValueError(f"Invalid category {new_category!r}")
    text = md_raw.lstrip("\ufeff")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError("Note has no YAML frontmatter")
    end: int | None = None
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            end = i
            break
    if end is None:
        raise ValueError("Unclosed frontmatter")
    fm_lines = lines[1:end]
    new_fm: list[str] = []
    found = False
    for raw_ln in fm_lines:
        ln_stripped = raw_ln.strip()
        if ln_stripped.startswith("category:"):
            new_fm.append(f"category: {new_category}")
            found = True
        else:
            new_fm.append(raw_ln)
    if not found:
        insert_after = -1
        for j, raw_ln in enumerate(new_fm):
            if raw_ln.strip().startswith("title:"):
                insert_after = j
                break
        insert_at = insert_after + 1 if insert_after >= 0 else 0
        new_fm.insert(insert_at, f"category: {new_category}")
    body_rest = lines[end + 1 :]
    out_lines = ["---", *new_fm, "---", *body_rest]
    out = "\n".join(out_lines)
    if text.endswith("\n") and not out.endswith("\n"):
        out += "\n"
    return out


def _notify_ntfy_after_save(filename: str, content: str, paths_ok: list[str]) -> None:
    """Fire-and-forget: notify ntfy topic after a successful /api/save-note (Watch mirrors iPhone via ntfy app)."""
    topic = (os.getenv("AHA_CATCHER_NTFY_TOPIC") or "").strip()
    if not topic:
        return
    server = (os.getenv("AHA_CATCHER_NTFY_SERVER") or "https://ntfy.sh").rstrip("/")
    post_url = f"{server}/{quote(topic, safe='')}"
    access_token = (os.getenv("AHA_CATCHER_NTFY_TOKEN") or "").strip()
    open_base = (os.getenv("AHA_CATCHER_OPEN_BASE") or "").strip().rstrip("/")
    push_secret = (os.getenv("AHA_CATCHER_PUSH_ACTION_SECRET") or "").strip()

    meta = _parse_note_frontmatter_block(content)
    title_fm = str(meta.get("title") or "").strip()
    cat_raw = meta.get("category")
    cat = str(cat_raw).strip() if isinstance(cat_raw, str) else "(none)"
    tags_val = meta.get("tags")
    tags_s = ""
    if isinstance(tags_val, list) and tags_val:
        tags_s = ", ".join(str(t) for t in tags_val[:12])

    n_title = (title_fm or filename)[:190]
    msg_lines = [f"分类 category: {cat}"]
    if tags_s:
        msg_lines.append(f"标签 tags: {tags_s}")
    msg_lines.append(f"文件: {filename}")
    if paths_ok:
        msg_lines.append("已写入本机文件夹。")
    payload: dict[str, Any] = {
        "title": n_title,
        "message": "\n".join(msg_lines)[:3800],
        "priority": 4,
    }
    if open_base:
        qf = quote(filename, safe="")
        fix_url = f"{open_base}/category-fix.html?file={qf}"
        if push_secret:
            fix_url += f"&token={quote(push_secret, safe='')}"
        payload["click"] = fix_url

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    try:
        with httpx.Client(timeout=12.0) as client:
            resp = client.post(post_url, json=payload, headers=headers)
        if resp.status_code not in (200, 201):
            preview = (resp.text or "").replace("\n", " ")[:400]
            print(
                f"[ahacatcher] ntfy: HTTP {resp.status_code} url={post_url!r} body={preview!r}",
                file=sys.stderr,
            )
        else:
            print(
                f"[ahacatcher] ntfy: pushed ok topic={topic!r} file={filename!r} click={'set' if open_base else 'none'}",
                file=sys.stderr,
            )
    except Exception as e:
        print(f"[ahacatcher] ntfy: request failed: {e}", file=sys.stderr)


def _trim_audio_first_seconds(data: bytes, original_filename: str, seconds: int) -> tuple[bytes, str]:
    """
    Keep only the first `seconds` of audio for quick transcribe. Prefer ffmpeg; if missing or fails, return original.
    Output name is always quick-trim.m4a when ffmpeg succeeds (re-encoded or copy).
    """
    name = (original_filename or "upload.m4a").strip() or "upload.m4a"
    ext = Path(name).suffix.lower()
    if ext not in (".m4a", ".mp4", ".m4v", ".mp3", ".wav", ".caf", ".qta", ".aac"):
        ext = ".m4a"
    if not shutil.which("ffmpeg"):
        print(
            "[ahacatcher] quick-memo: ffmpeg not found; using full clip (install: brew install ffmpeg)",
            file=sys.stderr,
        )
        return data, name
    path_in = tempfile.NamedTemporaryFile(prefix="aha-in-", suffix=ext, delete=False)
    path_in.write(data)
    path_in.close()
    path_out = tempfile.NamedTemporaryFile(prefix="aha-out-", suffix=".m4a", delete=False)
    path_out.close()
    try:
        cmd_copy = [
            "ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            path_in.name,
            "-t",
            str(seconds),
            "-c",
            "copy",
            path_out.name,
        ]
        r = subprocess.run(cmd_copy, capture_output=True, timeout=120)
        if r.returncode != 0:
            cmd_enc = [
                "ffmpeg",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                path_in.name,
                "-t",
                str(seconds),
                "-acodec",
                "aac",
                "-b:a",
                "96k",
                path_out.name,
            ]
            r = subprocess.run(cmd_enc, capture_output=True, timeout=120)
            if r.returncode != 0:
                err = (r.stderr or b"").decode("utf-8", errors="replace")[:400]
                print(f"[ahacatcher] quick-memo: ffmpeg failed, using full clip: {err}", file=sys.stderr)
                return data, name
        out = Path(path_out.name).read_bytes()
        if len(out) < 32:
            return data, name
        return out, "quick-trim.m4a"
    except (OSError, subprocess.TimeoutExpired) as e:
        print(f"[ahacatcher] quick-memo: ffmpeg error {e}; using full clip", file=sys.stderr)
        return data, name
    finally:
        try:
            os.unlink(path_in.name)
        except OSError:
            pass
        try:
            os.unlink(path_out.name)
        except OSError:
            pass


def _notify_ntfy_quick_memo(
    *,
    category: str,
    tags: list[str],
    why: Optional[str],
    transcript_preview: str,
    fingerprint: str,
) -> None:
    """Push quick Voice Memo classification (no Markdown file yet)."""
    topic = (os.getenv("AHA_CATCHER_NTFY_TOPIC") or "").strip()
    if not topic:
        return
    server = (os.getenv("AHA_CATCHER_NTFY_SERVER") or "https://ntfy.sh").rstrip("/")
    post_url = f"{server}/{quote(topic, safe='')}"
    access_token = (os.getenv("AHA_CATCHER_NTFY_TOKEN") or "").strip()

    # One line, comma-separated: VoiceMemo, Category, transcript excerpt (no title / click — saves space).
    cat_raw = str(category or "unsorted").strip()
    cat_disp = " ".join(part.capitalize() for part in cat_raw.replace("_", "-").split("-"))
    preview = transcript_preview.strip().replace("\n", " ")
    preview = preview[:400] if preview else ""
    if preview:
        line = f"VoiceMemo, {cat_disp}, {preview}"
    else:
        line = f"VoiceMemo, {cat_disp}"

    payload: dict[str, Any] = {"message": line[:3800]}

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    try:
        with httpx.Client(timeout=12.0) as client:
            resp = client.post(post_url, json=payload, headers=headers)
        if resp.status_code not in (200, 201):
            preview = (resp.text or "").replace("\n", " ")[:400]
            print(
                f"[ahacatcher] ntfy quick-memo: HTTP {resp.status_code} body={preview!r}",
                file=sys.stderr,
            )
        else:
            print(f"[ahacatcher] ntfy quick-memo: pushed ok topic={topic!r} category={category!r}", file=sys.stderr)
    except Exception as e:
        print(f"[ahacatcher] ntfy quick-memo: request failed: {e}", file=sys.stderr)


def _list_ideas_md_files() -> tuple[list[Path], list[str]]:
    """
    All `*.md` under configured ideas dirs; duplicate basenames (mirror) counted once
    using the first path order in _ideas_dirs().
    """
    dirs = _ideas_dirs()
    roots = [str(p) for p in dirs]
    if not dirs:
        return [], roots
    seen: set[str] = set()
    files: list[Path] = []
    for base in dirs:
        try:
            for p in sorted(base.glob("*.md")):
                if p.name in seen:
                    continue
                seen.add(p.name)
                files.append(p)
        except OSError as e:
            print(f"[ahacatcher] topics-map: cannot list {base}: {e}", file=sys.stderr)
    return files, roots


def build_topics_map_payload() -> dict[str, Any]:
    """Scan ideas folders and aggregate categories / tags (dynamic, each request)."""
    files, roots = _list_ideas_md_files()
    generated = datetime.now(timezone.utc).isoformat()
    if not roots:
        return {
            "generated_at": generated,
            "ideas_dirs_configured": False,
            "ideas_dirs": [],
            "note_files_scanned": 0,
            "parse_errors": [],
            "by_category": {},
            "tag_counts": {},
            "notes": [],
            "hint": "Set AHA_CATCHER_IDEAS_DIR in .env to your Ideas folder and restart uvicorn.",
        }

    by_cat_buckets: dict[str, dict[str, Any]] = {c: {"count": 0, "tags": defaultdict(int)} for c in NOTE_CATEGORIES_LIST}
    by_cat_buckets["(other)"] = {"count": 0, "tags": defaultdict(int)}
    tag_counts: dict[str, int] = defaultdict(int)
    notes_out: list[dict[str, Any]] = []
    parse_errors: list[dict[str, str]] = []

    for path in files:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as e:
            parse_errors.append({"file": path.name, "error": str(e)})
            continue
        meta = _parse_note_frontmatter_block(raw)
        cat_raw = meta.get("category")
        if isinstance(cat_raw, str) and cat_raw.strip() in NOTE_CATEGORIES:
            bucket = cat_raw.strip()
        else:
            bucket = "(other)"
        tags_val = meta.get("tags")
        tag_list: list[str]
        if isinstance(tags_val, list):
            tag_list = [str(t).strip() for t in tags_val if str(t).strip()]
        else:
            tag_list = []
        title = str(meta["title"]).strip() if meta.get("title") else ""
        created = str(meta["created"]).strip() if meta.get("created") else None

        st = by_cat_buckets[bucket]
        st["count"] += 1
        for t in tag_list:
            st["tags"][t] += 1
            tag_counts[t] += 1

        if bucket == "(other)":
            cr = cat_raw if isinstance(cat_raw, str) else ""
            cat_display = cr.strip() or "unknown"
        else:
            cat_display = bucket
        notes_out.append(
            {
                "file": path.name,
                "category": cat_display,
                "tags": tag_list,
                "title": title or path.stem,
                "created": created,
            }
        )

    notes_out.sort(key=lambda n: n["file"], reverse=True)

    by_category: dict[str, Any] = {}
    for cat in NOTE_CATEGORIES_LIST:
        cv = by_cat_buckets[cat]
        td = cv["tags"]
        by_category[cat] = {
            "count": cv["count"],
            "tags": {t: int(td[t]) for t in sorted(td.keys(), key=lambda x: (-td[x], x.lower()))},
        }
    other_cv = by_cat_buckets["(other)"]
    if other_cv["count"] > 0:
        td_o = other_cv["tags"]
        by_category["(other)"] = {
            "count": other_cv["count"],
            "tags": {t: int(td_o[t]) for t in sorted(td_o.keys(), key=lambda x: (-td_o[x], x.lower()))},
        }

    tag_counts_out = {t: int(tag_counts[t]) for t in sorted(tag_counts.keys(), key=lambda x: (-tag_counts[x], x.lower()))}

    print(
        f"[ahacatcher] topics-map: scanned {len(files)} md file(s) under {len(roots)} root(s)",
        file=sys.stderr,
    )

    return {
        "generated_at": generated,
        "ideas_dirs_configured": True,
        "ideas_dirs": roots,
        "note_files_scanned": len(files),
        "parse_errors": parse_errors,
        "by_category": by_category,
        "tag_counts": tag_counts_out,
        "notes": notes_out,
        "hint": None,
    }


def _safe_note_path(base: Path, filename: str) -> Path:
    name = filename.strip()
    if not name or "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not name.lower().endswith(".md"):
        name = f"{name}.md"
    safe = Path(name).name
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid filename")
    target = (base / safe).resolve()
    base_r = base.resolve()
    try:
        target.relative_to(base_r)
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Invalid filename") from e
    return target


def _resolve_note_file(filename: str) -> Path:
    """Resolve a single-note basename to an existing file under `_ideas_dirs()` (first match)."""
    name = filename.strip()
    if not name or "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not name.lower().endswith(".md"):
        name = f"{name}.md"
    safe = Path(name).name
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid filename")
    for base in _ideas_dirs():
        base_r = base.resolve()
        target = (base / safe).resolve()
        try:
            target.relative_to(base_r)
        except ValueError:
            continue
        if target.is_file():
            return target
    raise HTTPException(
        status_code=404,
        detail="Note not found in configured ideas directories.",
    )


def _need_key() -> None:
    if not AI_BUILDER_KEY:
        raise HTTPException(
            status_code=503,
            detail="AI_BUILDER_API_KEY (or AI_BUILDER_TOKEN) is not set. Add it to the project .env file.",
        )


def _extract_assistant_text(data: Any) -> str | None:
    """Best-effort parse of chat/completions JSON from OpenAI-compatible and variant APIs."""
    if not isinstance(data, dict):
        return None
    pre = data.get("assistant_text")
    if isinstance(pre, str) and pre.strip():
        return pre

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        ch0 = choices[0]
        if isinstance(ch0, dict):
            msg = ch0.get("message")
            if isinstance(msg, dict):
                c = msg.get("content")
                if isinstance(c, str) and c.strip():
                    return c
                if isinstance(c, list):
                    parts: list[str] = []
                    for p in c:
                        if isinstance(p, str):
                            parts.append(p)
                        elif isinstance(p, dict):
                            t = p.get("text")
                            if isinstance(t, str):
                                parts.append(t)
                            elif isinstance(t, dict):
                                v = t.get("value")
                                if isinstance(v, str):
                                    parts.append(v)
                    if parts:
                        return "".join(parts)
            t0 = ch0.get("text")
            if isinstance(t0, str) and t0.strip():
                return t0
            delta = ch0.get("delta")
            if isinstance(delta, dict):
                dc = delta.get("content")
                if isinstance(dc, str) and dc.strip():
                    return dc

    for key in ("response", "output_text", "content", "text", "message", "result"):
        v = data.get(key)
        if isinstance(v, str) and v.strip():
            return v

    out = data.get("output")
    if isinstance(out, list):
        buf: list[str] = []
        for item in out:
            if not isinstance(item, dict):
                continue
            for c in item.get("content") or []:
                if not isinstance(c, dict):
                    continue
                if c.get("type") == "text" and isinstance(c.get("text"), str):
                    buf.append(c["text"])
        if buf:
            return "".join(buf)

    return None


def _collect_long_strings_from_trace(obj: Any, depth: int = 0, acc: list[str] | None = None, min_len: int = 48) -> list[str]:
    """supermind-agent-v1 may leave message.content empty; debug trace can hold the final prose."""
    if acc is None:
        acc = []
    if depth > 28:
        return acc
    if isinstance(obj, str):
        t = obj.strip()
        if len(t) >= min_len:
            acc.append(t)
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_long_strings_from_trace(v, depth + 1, acc, min_len)
    elif isinstance(obj, list):
        for v in obj:
            _collect_long_strings_from_trace(v, depth + 1, acc, min_len)
    return acc


def _best_text_from_orchestrator_trace(trace: Any) -> str | None:
    if trace is None:
        return None
    candidates = _collect_long_strings_from_trace(trace)
    if not candidates:
        return None
    return max(candidates, key=len)


def _best_classify_json_from_trace(trace: Any) -> str | None:
    """Prefer a trace string that parses as classify JSON (avoid unrelated long prose)."""
    if trace is None:
        return None
    candidates = _collect_long_strings_from_trace(trace, min_len=12)
    if not candidates:
        return None
    for s in sorted(candidates, key=len, reverse=True):
        if "category" not in s:
            continue
        try:
            parsed = json.loads(_strip_json_code_fences(s))
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and str(parsed.get("category", "")).strip():
            return s
    return None


def _extract_classify_raw_text(data: dict[str, Any]) -> str | None:
    """Message content, then valid JSON from orchestrator debug trace."""
    t = _extract_assistant_text(data)
    if t and str(t).strip():
        return str(t).strip()
    trace_json = _best_classify_json_from_trace(data.get("orchestrator_trace"))
    if trace_json and trace_json.strip():
        print("[ahacatcher] classify: using JSON-shaped string from orchestrator_trace", file=sys.stderr)
        return trace_json.strip()
    return None


def _trim_transcript_for_research(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    head = max(2000, max_chars // 5)
    tail_take = max_chars - head - 120
    if tail_take < 4000:
        tail_take = max_chars - 200
        head = 200
    return (
        text[:head]
        + "\n\n[... transcript truncated for API limits; tail preserved below ...]\n\n"
        + text[-tail_take:]
    )


_VOICE_MEMO_GLOBS = ("*.m4a", "*.mp4", "*.wav", "*.caf", "*.qta")


def _voice_memo_default_mirror_path() -> Path:
    """Mirror folder produced by ahacatcher/scripts/sync_voice_memos_mirror.py (default: ~/Documents/VoiceMemosMirror)."""
    raw = (os.getenv("AHA_CATCHER_VOICE_MEMOS_MIRROR_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path.home() / "Documents" / "VoiceMemosMirror"


def _voice_memo_scan_roots() -> list[Path]:
    """Dirs to scan for Import: by default only the Voice Memos mirror + AHA_CATCHER_VOICE_MEMOS_DIRS.

    Default mirror path: ~/Documents/VoiceMemosMirror (override with AHA_CATCHER_VOICE_MEMOS_MIRROR_DIR).
    Set AHA_CATCHER_VOICE_MEMOS_USE_MIRROR_ONLY=0 to also scan Apple Voice Memos library paths (needs FDA).
    """
    home = Path.home()
    mirror = _voice_memo_default_mirror_path()
    extra = (os.getenv("AHA_CATCHER_VOICE_MEMOS_DIRS") or "").strip()
    extras: list[Path] = []
    if extra:
        for part in extra.split(","):
            p = Path(part.strip()).expanduser()
            if str(p):
                extras.append(p)
    use_mirror_only = (os.getenv("AHA_CATCHER_VOICE_MEMOS_USE_MIRROR_ONLY") or "1").strip().lower()
    mirror_only = use_mirror_only not in ("0", "false", "no", "off")

    candidates: list[Path] = []
    if mirror_only:
        candidates.append(mirror)
        candidates.extend(extras)
    else:
        # Whole VoiceMemos container (layout varies). Legacy + iTunes paths.
        candidates = [
            home / "Library/Group Containers/group.com.apple.VoiceMemos.shared",
            home / "Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings",
            home / "Library/Application Support/com.apple.voicememos",
            home / "Library/Application Support/com.apple.voicememos/Recordings",
            home / "Music/iTunes/iTunes Media/Voice Memos",
        ]
        candidates.extend(extras)

    out: list[Path] = []
    seen: set[str] = set()
    for raw in candidates:
        try:
            p = raw.resolve()
        except OSError:
            continue
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def _path_is_under_root(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (ValueError, OSError):
        return False


def _safe_voice_memo_user_path(raw: str) -> Path:
    """Resolve path; must be a file under one of the Voice Memos scan roots."""
    if not raw or not str(raw).strip():
        raise HTTPException(status_code=400, detail="path is required")
    try:
        p = Path(raw.strip()).expanduser().resolve(strict=False)
    except OSError as e:
        raise HTTPException(status_code=400, detail=f"Invalid path: {e}") from e
    roots = _voice_memo_scan_roots()
    ok = False
    for root in roots:
        if root.is_dir() and _path_is_under_root(p, root):
            ok = True
            break
    if not ok:
        print(
            f"[ahacatcher] voice-memo: rejected path outside allowed roots: {p!r}",
            file=sys.stderr,
        )
        raise HTTPException(
            status_code=403,
            detail="Path must be under the Voice Memos mirror folder, AHA_CATCHER_VOICE_MEMOS_DIRS, "
            "or (when AHA_CATCHER_VOICE_MEMOS_USE_MIRROR_ONLY=0) system Voice Memos paths.",
        )
    return p


def _guess_audio_content_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    if ext == ".wav":
        return "audio/wav"
    if ext in (".m4a", ".mp4", ".m4v"):
        return "audio/mp4"
    if ext == ".caf":
        return "audio/x-caf"
    if ext == ".qta":
        return "video/quicktime"
    return "application/octet-stream"


def _list_voice_memo_recordings(
    limit: int = 400,
) -> tuple[list[dict[str, Any]], list[str], bool]:
    """Return (recordings, roots_checked, had_permission_error)."""
    roots = _voice_memo_scan_roots()
    checked: list[str] = []
    found: list[tuple[float, Path]] = []
    seen_paths: set[str] = set()
    had_permission_error = False
    for root in roots:
        checked.append(str(root))
        try:
            if not root.exists() or not root.is_dir():
                continue
        except OSError:
            continue
        try:
            with os.scandir(root) as _probe:
                next(_probe, None)
        except PermissionError:
            had_permission_error = True
            print(
                f"[ahacatcher] voice-memo: PermissionError listing {root} "
                "(grant Full Disk Access to Terminal/Python running uvicorn)",
                file=sys.stderr,
            )
            continue
        except OSError as e:
            print(f"[ahacatcher] voice-memo: cannot probe {root}: {e}", file=sys.stderr)
            continue
        try:
            for pattern in _VOICE_MEMO_GLOBS:
                for p in root.rglob(pattern):
                    if not p.is_file():
                        continue
                    try:
                        key = str(p.resolve())
                        if key in seen_paths:
                            continue
                        seen_paths.add(key)
                        st = p.stat()
                        found.append((float(st.st_mtime), p))
                    except OSError:
                        continue
        except PermissionError:
            had_permission_error = True
            print(
                f"[ahacatcher] voice-memo: PermissionError during rglob under {root}",
                file=sys.stderr,
            )
        except OSError as e:
            print(f"[ahacatcher] voice-memo: scan error under {root}: {e}", file=sys.stderr)
    found.sort(key=lambda x: -x[0])
    recs: list[dict[str, Any]] = []
    for _mtime, p in found[:limit]:
        try:
            st = p.stat()
        except OSError:
            continue
        recs.append(
            {
                "path": str(p),
                "name": p.name,
                "size": st.st_size,
                "modified": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
            },
        )
    return recs, checked, had_permission_error


async def _forward_transcribe_to_ai_builder(body: bytes, filename: str, ctype: str) -> Any:
    """POST bytes to AI Builder speech-to-text; returns dict on success or JSONResponse on proxy errors."""
    _need_key()
    if not body:
        raise HTTPException(status_code=400, detail="Empty upload")
    url = f"{AI_BUILDER_BASE}/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {AI_BUILDER_KEY}"}
    files = {"file": (filename, body, ctype)}
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.post(url, headers=headers, files=files)
    except httpx.RequestError as e:
        print(f"[ahacatcher] transcribe upstream error: {e}", file=sys.stderr)
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}") from e
    if resp.status_code != 200:
        preview = (resp.text or "").strip().replace("\n", " ")[:900]
        resp_ct = (resp.headers.get("content-type") or "").split(";")[0].strip()
        print(
            f"[ahacatcher] transcribe upstream HTTP {resp.status_code} "
            f"upload_bytes={len(body)} upload_name={filename!r} upload_ct={ctype!r} "
            f"response_ct={resp_ct!r} body={preview!r}",
            file=sys.stderr,
        )
        out_status = 502 if resp.status_code >= 500 else resp.status_code
        detail = (
            f"Speech-to-text API returned HTTP {resp.status_code} (proxy status {out_status}). "
            f"Provider message: {preview or '(empty body)'}. "
            f"Uploaded {len(body)} bytes as {filename!r}. "
            "Check this terminal for the line starting with [ahacatcher] transcribe upstream; verify "
            f"AI_BUILDER_API_KEY, quota, and {AI_BUILDER_BASE!r} availability."
        )
        return JSONResponse(status_code=out_status, content={"detail": detail})
    try:
        data = resp.json()
    except Exception as e:
        txt = (resp.text or "").strip()[:500]
        print(f"[ahacatcher] transcribe: expected JSON, got: {txt!r} ({e})", file=sys.stderr)
        raise HTTPException(
            status_code=502,
            detail=f"Transcription API returned non-JSON (HTTP {resp.status_code}): {txt or '(empty)'}",
        ) from e
    t = data.get("text") if isinstance(data, dict) else None
    tc = len(t) if isinstance(t, str) else 0
    _metrics_append({"event": "transcribe_ok", "text_chars": tc})
    return data


@app.get("/api/voice-memos")
async def api_voice_memos():
    """List audio files under macOS Voice Memos recording folders (local server only)."""
    recordings, roots_checked, had_permission_error = _list_voice_memo_recordings()
    hint = ""
    permission_blocked = had_permission_error and not recordings
    if permission_blocked:
        hint = (
            "系统不允许当前进程读取语音备忘录目录（常见表现：列表为空但本机明明有录音）。"
            "请打开 **系统设置 → 隐私与安全性 → 完全磁盘访问权限**，为 **终端**、**Cursor** 或实际运行 "
            "`uvicorn` 的 **Python** 勾选权限，**完全退出并重启**终端与 server 后再试。"
            "若仍不行，可在语音备忘录里将录音 **共享/储存到文件**，把文件夹路径写入 .env 的 "
            "AHA_CATCHER_VOICE_MEMOS_DIRS（逗号分隔）。"
        )
    elif not recordings:
        hint = (
            "未找到可导入的音频。默认只扫描「语音备忘录镜像」目录（见 README："
            "`~/Documents/VoiceMemosMirror`，由 sync_voice_memos_mirror 同步）。"
            "请先运行一次同步脚本或 launchd 任务；也可在 .env 设置 AHA_CATCHER_VOICE_MEMOS_DIRS。"
            "若需直接从系统语音备忘录库读取（需完全磁盘访问权限），请设置 "
            "AHA_CATCHER_VOICE_MEMOS_USE_MIRROR_ONLY=0 并重启 uvicorn。"
        )
    return {
        "recordings": recordings,
        "roots_checked": roots_checked,
        "hint": hint,
        "permission_blocked": permission_blocked,
    }


@app.get("/api/voice-memos/audio")
async def api_voice_memos_audio(path: str):
    """Stream a verified Voice Memos file for replay in the browser."""
    p = _safe_voice_memo_user_path(path)
    if not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    ctype = _guess_audio_content_type(p.name)
    print(f"[ahacatcher] voice-memo audio: serving {p.name!r} ct={ctype!r}", file=sys.stderr)
    return FileResponse(path=str(p), media_type=ctype, filename=p.name)


@app.post("/api/transcribe-local")
async def api_transcribe_local(body: TranscribeLocalFileRequest):
    """Transcribe a Voice Memos file by absolute path (same upstream as /api/transcribe)."""
    p = _safe_voice_memo_user_path(body.path)
    if not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    try:
        raw = p.read_bytes()
    except PermissionError as e:
        raise HTTPException(
            status_code=403,
            detail=(
                "无法读取该文件：请为运行本服务的终端或 Python 开启「完全磁盘访问权限」"
                "（系统设置 → 隐私与安全性），或将录音复制到自定义文件夹后在 .env 设置 "
                "AHA_CATCHER_VOICE_MEMOS_DIRS。"
            ),
        ) from e
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Cannot read file: {e}") from e
    print(
        f"[ahacatcher] transcribe-local: {p.name!r} bytes={len(raw)}",
        file=sys.stderr,
    )
    filename = p.name
    ctype = _guess_audio_content_type(filename)
    return await _forward_transcribe_to_ai_builder(raw, filename, ctype)


@app.post("/api/transcribe")
async def api_transcribe(file: UploadFile = File(...)):
    """Forward audio to AI Builder /v1/audio/transcriptions (multipart)."""
    body = await file.read()
    filename = file.filename or "capture.wav"
    ctype = file.content_type or "audio/wav"
    out = await _forward_transcribe_to_ai_builder(body, filename, ctype)
    if isinstance(out, JSONResponse):
        return out
    return out


@app.post("/api/research")
async def api_research(body: ResearchRequest):
    """Run research / summary on the transcript (supermind + debug trace, or fallback chat model)."""
    _need_key()
    transcript = _trim_transcript_for_research(body.transcript, MAX_RESEARCH_TRANSCRIPT_CHARS)
    if len(transcript) < len(body.transcript):
        print(
            f"[ahacatcher] research: transcript trimmed {len(body.transcript)} -> {len(transcript)} chars",
            file=sys.stderr,
        )
    messages = [
        {
            "role": "system",
            "content": (
                "You are Aha! Catcher. Given the user's spoken note (already transcribed), "
                "produce a concise research summary. If they asked a question or implied one, "
                "use web search when helpful to ground facts if your stack supports it. "
                "If it is not a question, still add useful context, definitions, or adjacent ideas. "
                "Reply with plain prose only (no meta-commentary about APIs or tools)."
            ),
        },
        {
            "role": "user",
            "content": (
                f'Transcript:\n"""{transcript}"""\n\n'
                "Give a short research summary and any direct answers if applicable."
            ),
        },
    ]
    url = f"{AI_BUILDER_BASE}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {AI_BUILDER_KEY}",
        "Content-Type": "application/json",
    }
    payload_primary = {
        "model": CHAT_MODEL,
        "max_tokens": CHAT_MAX_TOKENS,
        "messages": messages,
        "debug": True,
    }
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.post(url, json=payload_primary, headers=headers)
            try:
                data = resp.json()
            except Exception:
                data = {"raw": resp.text}
            if resp.status_code != 200:
                return JSONResponse(status_code=resp.status_code, content=data)

            extracted = _extract_assistant_text(data)
            if not (extracted and extracted.strip()):
                trace_text = _best_text_from_orchestrator_trace(
                    data.get("orchestrator_trace") if isinstance(data, dict) else None,
                )
                if trace_text and trace_text.strip():
                    extracted = trace_text.strip()
                    print("[ahacatcher] research: using text from orchestrator_trace (debug)", file=sys.stderr)

            usage = data.get("usage") if isinstance(data, dict) else None
            if isinstance(usage, dict):
                pt = usage.get("prompt_tokens")
                ct = usage.get("completion_tokens")
                if pt is not None or ct is not None:
                    print(f"[ahacatcher] research primary usage: prompt_tokens={pt} completion_tokens={ct}", file=sys.stderr)

            if (not extracted or not extracted.strip()) and CHAT_FALLBACK_MODEL and CHAT_FALLBACK_MODEL != CHAT_MODEL:
                print(
                    f"[ahacatcher] research: empty assistant content; retry with model={CHAT_FALLBACK_MODEL!r}",
                    file=sys.stderr,
                )
                payload_fb = {
                    "model": CHAT_FALLBACK_MODEL,
                    "max_tokens": CHAT_MAX_TOKENS,
                    "messages": messages,
                    "debug": False,
                }
                resp2 = await client.post(url, json=payload_fb, headers=headers)
                try:
                    data2 = resp2.json()
                except Exception:
                    data2 = {"raw": resp2.text}
                if resp2.status_code != 200:
                    return JSONResponse(status_code=resp2.status_code, content=data2)
                data = data2
                extracted = _extract_assistant_text(data)

    except httpx.RequestError as e:
        print(f"[ahacatcher] chat upstream error: {e}", file=sys.stderr)
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}") from e

    if not extracted or not extracted.strip():
        snippet = json.dumps(data, ensure_ascii=False)[:1800] if isinstance(data, dict) else str(data)[:1800]
        print(
            f"[ahacatcher] research: still no assistant text. Keys: "
            f"{list(data.keys()) if isinstance(data, dict) else type(data)}\n{snippet}",
            file=sys.stderr,
        )
        raise HTTPException(
            status_code=502,
            detail=(
                "Chat API returned 200 but no assistant text. "
                "supermind-agent-v1 sometimes returns empty `content`; this server retried with "
                f"{CHAT_FALLBACK_MODEL!r}. Set AHA_CATCHER_CHAT_MODEL / AHA_CATCHER_CHAT_FALLBACK_MODEL in .env. "
                f"Top-level keys: {list(data.keys()) if isinstance(data, dict) else 'non-dict'}."
            ),
        )
    return {"assistant_text": extracted.strip(), **data}


def _strip_json_code_fences(text: str) -> str:
    t = text.strip()
    if not t.startswith("```"):
        return t
    lines = t.split("\n")
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    while lines and lines[-1].strip() in ("```", ""):
        lines.pop()
    return "\n".join(lines).strip()


def _normalize_classify_result(raw: dict[str, Any]) -> dict[str, Any]:
    cat = raw.get("category", "unsorted")
    if not isinstance(cat, str) or cat.strip() not in NOTE_CATEGORIES:
        cat = "unsorted"
    else:
        cat = cat.strip()
    tags_raw = raw.get("tags")
    tags: list[str] = []
    if isinstance(tags_raw, list):
        seen_lower: set[str] = set()
        for item in tags_raw[:16]:
            s = str(item).strip()
            if not s:
                continue
            low = s.lower()
            if low in seen_lower:
                continue
            seen_lower.add(low)
            tags.append(s)
    rs_raw = raw.get("reading_source")
    if cat == "reading":
        if rs_raw is None or (isinstance(rs_raw, str) and not rs_raw.strip()):
            reading_source: str | None = "unknown"
        else:
            reading_source = str(rs_raw).strip()
    else:
        reading_source = None
    why_raw = raw.get("why") if isinstance(raw.get("why"), str) else raw.get("rationale")
    why: str | None
    if isinstance(why_raw, str) and why_raw.strip():
        why = why_raw.strip()[:800]
    else:
        why = None
    return {"category": cat, "tags": tags, "reading_source": reading_source, "why": why}


def _classify_response_payload(
    *,
    category: str,
    tags: list[str],
    reading_source: str | None,
    why: str | None,
    input_chars: int,
    used_chars: int,
    response_model: str | None = None,
) -> dict[str, Any]:
    return {
        "category": category,
        "tags": tags,
        "reading_source": reading_source,
        "why": why,
        "model": response_model or CLASSIFY_MODEL,
        "input_chars": input_chars,
        "used_chars": used_chars,
    }


async def _classify_transcript_core(raw_input: str) -> dict[str, Any]:
    """LLM classify; caller must call _need_key() first."""
    raw_input = (raw_input or "").strip()
    input_chars = len(raw_input)
    t = raw_input
    if not t or t == "(empty transcript)":
        return _classify_response_payload(
            category="unsorted",
            tags=[],
            reading_source=None,
            why="Empty transcript — defaulted to unsorted.",
            input_chars=input_chars,
            used_chars=0,
        )
    if len(t) > MAX_CLASSIFY_TRANSCRIPT_CHARS:
        t = _trim_transcript_for_research(t, MAX_CLASSIFY_TRANSCRIPT_CHARS)
        print(f"[ahacatcher] classify: transcript trimmed to {len(t)} chars", file=sys.stderr)

    system = """You classify voice transcripts for a personal notes app. Output ONLY one JSON object. No markdown fences, no explanations.

Keys:
- "category": exactly one of: life-daily, inner, career-learning, reading, pilates, unsorted
- "tags": array of 2 to 8 short search tags (English hyphenated words and/or brief Chinese). Use concrete keywords for search; do not output only generic words.
- "reading_source": string or null

Definitions:
- life-daily: shopping lists, chores, reminders, to-do, errands, everyday logistics.
- inner: emotions, therapy, relationships, self-reflection, mental health, diary-like feelings.
- career-learning: work, study, skills, career growth, learning, professional content (e.g. social media for work).
- reading: reacting to a book, article, podcast, lecture, quotes.
- pilates: Pilates / 普拉提 — classes, studio sessions, mat or reformer practice, scheduling, instructors, movement notes clearly about Pilates (not general fitness unless centered on Pilates).
- unsorted: truly unclear or evenly mixed; use only when none of the above fits.

If category is "reading":
  - If the transcript names a specific book, article, podcast, or publication, set reading_source to a short string (title; add author if clearly stated).
  - If NO such source is named or inferable, set reading_source to exactly: unknown
If category is not "reading", set reading_source to null.

Include key "why": one short sentence in the same language as the transcript when possible, explaining why you chose this category (for the user to audit mistakes). Max ~200 characters."""

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": f'Transcript:\n"""\n{t}\n"""'},
    ]
    url = f"{AI_BUILDER_BASE}/v1/chat/completions"
    req_headers = {
        "Authorization": f"Bearer {AI_BUILDER_KEY}",
        "Content-Type": "application/json",
    }
    models_try: list[str] = [CLASSIFY_MODEL]
    if CLASSIFY_FALLBACK_MODEL and CLASSIFY_FALLBACK_MODEL not in models_try:
        models_try.append(CLASSIFY_FALLBACK_MODEL)

    raw_text: str | None = None
    data: dict[str, Any] = {}
    model_used = CLASSIFY_MODEL

    try:
        async with httpx.AsyncClient(timeout=90.0) as client:
            for attempt, model in enumerate(models_try):
                payload = {
                    "model": model,
                    "max_tokens": 4096,
                    "temperature": 0.2,
                    "messages": messages,
                    "debug": True,
                }
                resp = await client.post(url, json=payload, headers=req_headers)
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                if resp.status_code != 200:
                    detail = data if isinstance(data, dict) else str(data)
                    print(f"[ahacatcher] classify HTTP {resp.status_code} model={model!r}: {detail}", file=sys.stderr)
                    raise HTTPException(status_code=502, detail=f"Classify API error ({resp.status_code})")

                if not isinstance(data, dict):
                    data = {}
                cand = _extract_classify_raw_text(data)
                if cand and cand.strip():
                    raw_text = cand.strip()
                    model_used = model
                    break

                usage = data.get("usage") if isinstance(data, dict) else None
                u_snip = ""
                if isinstance(usage, dict):
                    u_snip = f" usage={usage!r}"
                keys = list(data.keys()) if isinstance(data, dict) else type(data)
                snippet = json.dumps(data, ensure_ascii=False)[:1200] if isinstance(data, dict) else str(data)[:1200]
                print(
                    f"[ahacatcher] classify: empty content from model={model!r}; keys={keys}{u_snip}\n{snippet}",
                    file=sys.stderr,
                )
                model_used = model
                if attempt < len(models_try) - 1:
                    nxt = models_try[attempt + 1]
                    print(f"[ahacatcher] classify: retry with model={nxt!r}", file=sys.stderr)

    except HTTPException:
        raise
    except httpx.RequestError as e:
        print(f"[ahacatcher] classify upstream error: {e}", file=sys.stderr)
        raise HTTPException(status_code=502, detail=f"Classify request failed: {e}") from e

    if not raw_text or not raw_text.strip():
        norm = _normalize_classify_result({})
        return _classify_response_payload(
            category=norm["category"],
            tags=norm["tags"],
            reading_source=norm["reading_source"],
            why="Model returned no usable text — defaulted to unsorted. Check server terminal logs for the raw API keys/snippet; try AHA_CATCHER_CLASSIFY_FALLBACK_MODEL or a higher AHA_CATCHER_CLASSIFY_MAX_CHARS only if needed.",
            input_chars=input_chars,
            used_chars=len(t),
            response_model=model_used,
        )

    try:
        parsed = json.loads(_strip_json_code_fences(raw_text))
    except json.JSONDecodeError as e:
        print(f"[ahacatcher] classify: JSON error {e}: {raw_text[:800]!r}", file=sys.stderr)
        norm = _normalize_classify_result({})
        return _classify_response_payload(
            category=norm["category"],
            tags=norm["tags"],
            reading_source=norm["reading_source"],
            why="Could not parse model JSON — check server logs.",
            input_chars=input_chars,
            used_chars=len(t),
            response_model=model_used,
        )

    if not isinstance(parsed, dict):
        norm = _normalize_classify_result({})
        return _classify_response_payload(
            category=norm["category"],
            tags=norm["tags"],
            reading_source=norm["reading_source"],
            why=None,
            input_chars=input_chars,
            used_chars=len(t),
            response_model=model_used,
        )

    norm = _normalize_classify_result(parsed)
    used = len(t)
    trim_note = ""
    if input_chars > used:
        trim_note = f" (Classifier saw first+last ~{used} chars of {input_chars}; see AHA_CATCHER_CLASSIFY_MAX_CHARS.)"
    why_out = norm["why"]
    if why_out and trim_note:
        why_out = why_out + trim_note
    elif not why_out and input_chars > used:
        why_out = f"Transcript was truncated for the classifier.{trim_note.strip()}"

    return _classify_response_payload(
        category=norm["category"],
        tags=norm["tags"],
        reading_source=norm["reading_source"],
        why=why_out,
        input_chars=input_chars,
        used_chars=used,
        response_model=model_used,
    )


@app.post("/api/classify")
async def api_classify(body: ResearchRequest):
    """LLM: pick note category, suggest tags, extract reading source (or 'unknown')."""
    _need_key()
    return await _classify_transcript_core(body.transcript)


@app.get("/api/topics-map")
async def api_topics_map():
    """Scan configured ideas dirs for `*.md` and return category/tag aggregates (no caching)."""
    return build_topics_map_payload()


@app.get("/api/notes-by-tag")
async def api_notes_by_tag(tag: str):
    """All notes whose `tags` frontmatter contains `tag` (ASCII case-insensitive)."""
    t = (tag or "").strip()
    if not t:
        raise HTTPException(status_code=400, detail="Query parameter 'tag' is required.")
    t_lower = t.lower()
    files, roots = _list_ideas_md_files()
    generated = datetime.now(timezone.utc).isoformat()
    if not roots:
        return {
            "generated_at": generated,
            "tag": t,
            "ideas_dirs_configured": False,
            "match_count": 0,
            "records": [],
            "hint": "Set AHA_CATCHER_IDEAS_DIR in .env to your Ideas folder and restart uvicorn.",
        }

    records: list[dict[str, Any]] = []
    for path in files:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            continue
        meta = _parse_note_frontmatter_block(raw)
        cat_raw = meta.get("category")
        if isinstance(cat_raw, str) and cat_raw.strip() in NOTE_CATEGORIES:
            bucket = cat_raw.strip()
        else:
            bucket = "(other)"
        tags_val = meta.get("tags")
        if isinstance(tags_val, list):
            tag_list = [str(x).strip() for x in tags_val if str(x).strip()]
        else:
            tag_list = []
        if not any(x.lower() == t_lower for x in tag_list):
            continue
        if bucket == "(other)":
            cr = cat_raw if isinstance(cat_raw, str) else ""
            cat_display = cr.strip() or "unknown"
        else:
            cat_display = bucket
        title = str(meta["title"]).strip() if meta.get("title") else ""
        created = str(meta["created"]).strip() if meta.get("created") else None
        body = _body_after_frontmatter(raw)
        transcript = _section_under_markdown_heading(body, "Transcript")
        summary = _section_under_markdown_heading(body, "Research summary")
        records.append(
            {
                "file": path.name,
                "title": title or path.stem,
                "category": cat_display,
                "tags": tag_list,
                "created": created,
                "transcript": transcript,
                "has_summary": bool(summary.strip()),
            }
        )

    records.sort(key=lambda r: r["file"], reverse=True)
    print(
        f"[ahacatcher] notes-by-tag tag={t!r} matches={len(records)}",
        file=sys.stderr,
    )
    return {
        "generated_at": generated,
        "tag": t,
        "ideas_dirs_configured": True,
        "ideas_dirs": roots,
        "match_count": len(records),
        "records": records,
        "hint": None,
    }


@app.get("/api/note-full")
async def api_note_full(file: str):
    """Return full Markdown (UTF-8) for a note basename under configured ideas dirs."""
    q = (file or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'file' is required.")
    path = _resolve_note_file(q)
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return {"file": path.name, "content": content}


@app.get("/api/note-meta")
async def api_note_meta(file: str):
    """Frontmatter summary for a note (used by category-fix page on phone)."""
    q = (file or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'file' is required.")
    path = _resolve_note_file(q)
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    meta = _parse_note_frontmatter_block(raw)
    cat_raw = meta.get("category")
    cat: str | None
    if isinstance(cat_raw, str) and cat_raw.strip():
        cat = cat_raw.strip()
    else:
        cat = None
    title = str(meta["title"]).strip() if meta.get("title") else ""
    tags_val = meta.get("tags")
    tags: list[str] = []
    if isinstance(tags_val, list):
        tags = [str(t).strip() for t in tags_val if str(t).strip()]
    return {
        "file": path.name,
        "title": title or path.stem,
        "category": cat,
        "tags": tags,
        "categories_allowed": list(NOTE_CATEGORIES_LIST),
        "push_token_configured": bool((os.getenv("AHA_CATCHER_PUSH_ACTION_SECRET") or "").strip()),
    }


@app.get("/api/config")
async def api_config():
    """Server-side save: one or two folders; auto_save tells the UI whether to POST after transcript/summary."""
    dirs = _ideas_dirs()
    mp = _metrics_path()
    return {
        "server_save": len(dirs) > 0,
        "save_targets": len(dirs),
        "auto_save": _auto_save_enabled() if dirs else False,
        "metrics_enabled": mp is not None,
        "metrics_file": str(mp) if mp is not None else None,
        "ntfy_topic_configured": bool((os.getenv("AHA_CATCHER_NTFY_TOPIC") or "").strip()),
        "open_base_for_push": bool((os.getenv("AHA_CATCHER_OPEN_BASE") or "").strip()),
        "push_action_secret_configured": bool((os.getenv("AHA_CATCHER_PUSH_ACTION_SECRET") or "").strip()),
        "quick_memo_secret_configured": bool((os.getenv("AHA_CATCHER_QUICK_MEMO_SECRET") or "").strip()),
        "quick_memo_trim_seconds": QUICK_MEMO_TRIM_SEC,
    }


@app.post("/api/metrics/record")
async def api_metrics_record(body: MetricsRecord):
    """Append one JSON line for classify / note_saved / research_ok (local evaluation)."""
    if body.event not in ("classify", "note_saved", "research_ok"):
        raise HTTPException(status_code=400, detail="event must be classify, note_saved, or research_ok")
    if body.event == "classify":
        if not body.category or body.category not in NOTE_CATEGORIES:
            raise HTTPException(status_code=400, detail="classify requires category in allowed set")
        _metrics_append(
            {
                "event": "classify",
                "category": body.category,
                "model": body.model,
            },
        )
        return {"ok": True}
    if body.event == "research_ok":
        _metrics_append({"event": "research_ok"})
        return {"ok": True}
    if body.event == "note_saved":
        if not body.final_category or body.final_category not in NOTE_CATEGORIES:
            raise HTTPException(status_code=400, detail="note_saved requires final_category")
        _metrics_append(
            {
                "event": "note_saved",
                "session": body.session,
                "pass_label": body.pass_label,
                "channel": body.channel,
                "predicted_category": body.predicted_category,
                "final_category": body.final_category,
                "user_changed": body.user_changed,
                "classify_succeeded": body.classify_succeeded,
                "has_summary": body.has_summary,
            },
        )
        return {"ok": True}
    raise HTTPException(status_code=400, detail="unhandled event")


@app.get("/api/metrics/summary")
async def api_metrics_summary():
    """Aggregate locally stored metrics (transcribe, classify, saves, implicit accuracy)."""
    path = _metrics_path()
    if path is None:
        return {
            "enabled": False,
            "message": "Metrics disabled (set AHA_CATCHER_METRICS_JSONL to a path, or unset for default ahacatcher/data/metrics.jsonl).",
        }
    rows = _metrics_read_rows()
    transcribe_n = sum(1 for r in rows if r.get("event") == "transcribe_ok")
    classify_n = sum(1 for r in rows if r.get("event") == "classify")
    research_n = sum(1 for r in rows if r.get("event") == "research_ok")
    predicted_hist: dict[str, int] = defaultdict(int)
    for r in rows:
        if r.get("event") != "classify":
            continue
        cat = r.get("category")
        if isinstance(cat, str) and cat in NOTE_CATEGORIES:
            predicted_hist[cat] += 1

    last_saved: dict[str, dict[str, Any]] = {}
    for r in rows:
        if r.get("event") != "note_saved":
            continue
        sess = r.get("session")
        if sess is None:
            continue
        last_saved[str(sess)] = r

    final_hist: dict[str, int] = defaultdict(int)
    eval_with_pred = 0
    eval_accepted = 0
    eval_corrected = 0
    for r in last_saved.values():
        fc = r.get("final_category")
        if isinstance(fc, str) and fc in NOTE_CATEGORIES:
            final_hist[fc] += 1
        pc = r.get("predicted_category")
        cs = r.get("classify_succeeded")
        uc = r.get("user_changed")
        if cs is True and isinstance(pc, str) and pc in NOTE_CATEGORIES:
            eval_with_pred += 1
            if uc is True:
                eval_corrected += 1
            else:
                eval_accepted += 1

    rate: float | None
    if eval_with_pred:
        rate = round(eval_accepted / eval_with_pred, 4)
    else:
        rate = None

    conf: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in last_saved.values():
        pc = r.get("predicted_category")
        fc = r.get("final_category")
        if not (isinstance(pc, str) and pc in NOTE_CATEGORIES and isinstance(fc, str) and fc in NOTE_CATEGORIES):
            continue
        if r.get("classify_succeeded") is not True:
            continue
        conf[pc][fc] += 1

    confusion = {pk: {fk: int(conf[pk][fk]) for fk in NOTE_CATEGORIES_LIST} for pk in NOTE_CATEGORIES_LIST}

    return {
        "enabled": True,
        "metrics_file": str(path),
        "row_count_scanned": len(rows),
        "transcribe_ok_count": transcribe_n,
        "classify_event_count": classify_n,
        "research_ok_count": research_n,
        "notes_saved_sessions": len(last_saved),
        "predicted_category_counts": {k: predicted_hist[k] for k in NOTE_CATEGORIES_LIST},
        "final_category_counts": {k: final_hist[k] for k in NOTE_CATEGORIES_LIST},
        "evaluation": {
            "notes_with_classifier_baseline": eval_with_pred,
            "implicitly_accepted": eval_accepted,
            "user_corrected_category": eval_corrected,
            "implicit_agreement_rate": rate,
            "definition": (
                "Agreement = classify ran and user left category unchanged through the last save for that "
                "recording session. Correction = user changed category before that save."
            ),
        },
        "confusion_predicted_vs_final": confusion,
    }


@app.get("/api/background-status")
async def api_background_status(days: int = 3):
    """
    Dashboard for launchd/cron: mirror sync + quick_memo_scan logs (background_monitor.jsonl)
    and quick_memo transcript rows from metrics.jsonl. Used by background-monitor.html.
    """
    if days < 1:
        days = 1
    if days > 14:
        days = 14
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)

    mon_rows = _background_monitor_read_rows()
    mon_filtered: list[dict[str, Any]] = []
    for r in mon_rows:
        ts = _parse_ts_iso(r.get("ts"))
        if ts is None or ts < cutoff:
            continue
        mon_filtered.append(r)

    last_sync: dict[str, Any] | None = None
    last_scan: dict[str, Any] | None = None
    for r in mon_rows:
        ev = r.get("event")
        if ev == "voice_memos_sync":
            last_sync = r
        elif ev == "quick_memo_scan":
            last_scan = r

    metrics_rows: list[dict[str, Any]] = []
    mp = _metrics_path()
    if mp is not None and mp.is_file():
        metrics_rows = _metrics_read_rows()

    qm_events: list[dict[str, Any]] = []
    last_qm: dict[str, Any] | None = None
    for r in metrics_rows:
        if r.get("event") != "quick_memo":
            continue
        ts = _parse_ts_iso(r.get("ts"))
        if ts is None or ts < cutoff:
            continue
        qm_events.append(r)
        last_qm = r

    times_chain: list[datetime] = []
    for r in (last_sync, last_scan):
        if r:
            t = _parse_ts_iso(r.get("ts"))
            if t:
                times_chain.append(t)
    last_chain = max(times_chain) if times_chain else None

    next_run: str | None = None
    if last_chain is not None:
        nxt = last_chain + timedelta(seconds=BACKGROUND_INTERVAL_SEC)
        next_run = nxt.isoformat()

    stale_sec: int | None = None
    health = "unknown"
    msg = ""
    if last_chain is None:
        health = "no_data"
        msg = (
            "No background_monitor.jsonl entries yet. Run ahacatcher/scripts/sync + quick_memo_scan once "
            "(or wait for launchd) after updating scripts."
        )
    else:
        stale_sec = int((now - last_chain).total_seconds())
        if stale_sec > 2 * BACKGROUND_INTERVAL_SEC:
            health = "stale"
            msg = (
                f"No sync/scan activity for ~{stale_sec // 60} min (expected about every "
                f"{BACKGROUND_INTERVAL_SEC // 60} min). Check launchd, Mac sleep, or Full Disk Access."
            )
        else:
            health = "ok"
            msg = "Recent mirror sync + scan activity looks normal."

    sync_ok = None
    if last_sync is not None:
        sync_ok = bool(last_sync.get("ok"))

    return {
        "server_time_utc": now.isoformat(),
        "window_days": days,
        "background_interval_sec": BACKGROUND_INTERVAL_SEC,
        "health": health,
        "health_message": msg,
        "staleness_seconds": stale_sec,
        "last_voice_memos_sync": last_sync,
        "last_quick_memo_scan": last_scan,
        "last_quick_memo_transcribe": last_qm,
        "last_background_chain_time_utc": last_chain.isoformat() if last_chain else None,
        "next_expected_background_run_utc": next_run,
        "monitor_events_window": mon_filtered[-BACKGROUND_MONITOR_LOG_UI_MAX:],
        "monitor_events_display_limit": BACKGROUND_MONITOR_LOG_UI_MAX,
        "quick_memo_transcribe_events_window": qm_events[-200:],
        "monitor_file": str(_background_monitor_path()),
        "metrics_enabled": mp is not None,
    }


@app.post("/api/save-note")
async def api_save_note(body: SaveNoteRequest, background_tasks: BackgroundTasks):
    """Save the same Markdown file to every configured ideas directory (iCloud + local mirror, etc.)."""
    dirs = _ideas_dirs()
    if not dirs:
        raise HTTPException(
            status_code=503,
            detail=(
                "Server-side save is not configured. Set AHA_CATCHER_IDEAS_DIR in .env to an "
                "existing folder (e.g. iCloud Drive/Ideas). Optionally set AHA_CATCHER_IDEAS_MIRROR_DIR "
                "to a second folder (e.g. ~/Documents/Ideas) for the same file written twice. Restart uvicorn."
            ),
        )
    paths_ok: list[str] = []
    errors: list[str] = []
    for base in dirs:
        target = _safe_note_path(base, body.filename)
        try:
            target.write_text(body.content, encoding="utf-8")
            paths_ok.append(str(target))
        except OSError as e:
            msg = f"{target}: {e}"
            errors.append(msg)
            print(f"[ahacatcher] save-note failed: {msg}", file=sys.stderr)
    if not paths_ok:
        raise HTTPException(
            status_code=500,
            detail="Could not write to any folder: " + "; ".join(errors),
        )
    background_tasks.add_task(_notify_ntfy_after_save, body.filename, body.content, list(paths_ok))
    return {
        "ok": True,
        "paths": paths_ok,
        "path": paths_ok[0],
        "warnings": errors or None,
    }


@app.post("/api/note-category")
async def api_note_category(
    body: NoteCategoryUpdate,
    x_aha_push_token: Optional[str] = Header(default=None, alias="X-Aha-Push-Token"),
):
    """Update `category:` in frontmatter (phone / notification follow-up). Requires X-Aha-Push-Token matching .env."""
    secret = (os.getenv("AHA_CATCHER_PUSH_ACTION_SECRET") or "").strip()
    if not secret:
        raise HTTPException(
            status_code=503,
            detail="Set AHA_CATCHER_PUSH_ACTION_SECRET in .env to enable category updates from the phone.",
        )
    if (x_aha_push_token or "").strip() != secret:
        raise HTTPException(status_code=403, detail="Invalid or missing X-Aha-Push-Token header.")

    cat = body.category.strip()
    if cat not in NOTE_CATEGORIES:
        raise HTTPException(
            status_code=400,
            detail="category must be one of: " + ", ".join(NOTE_CATEGORIES_LIST),
        )

    dirs = _ideas_dirs()
    if not dirs:
        raise HTTPException(status_code=503, detail="No ideas directories configured.")

    safe_name = _safe_note_path(dirs[0], body.filename).name

    updated: list[str] = []
    errs: list[str] = []
    for base in dirs:
        target = _safe_note_path(base, safe_name)
        if not target.is_file():
            continue
        try:
            raw = target.read_text(encoding="utf-8")
            new_raw = _replace_frontmatter_category(raw, cat)
            target.write_text(new_raw, encoding="utf-8")
            updated.append(str(target))
        except ValueError as e:
            errs.append(f"{target}: {e}")
        except OSError as e:
            errs.append(f"{target}: {e}")

    if not updated:
        raise HTTPException(
            status_code=404,
            detail="Note not found or frontmatter missing: "
            + ("; ".join(errs) if errs else "no matching .md in configured folders"),
        )
    print(
        f"[ahacatcher] note-category: updated {len(updated)} path(s) file={safe_name!r} category={cat!r}",
        file=sys.stderr,
    )
    return {"ok": True, "paths": updated, "category": cat, "warnings": errs or None}


@app.post("/api/quick-memo")
async def api_quick_memo(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    fingerprint: Optional[str] = Form(default=None),
    x_aha_quick_memo_token: Optional[str] = Header(default=None, alias="X-Aha-Quick-Memo-Token"),
):
    """
    Multipart audio → trim → transcribe → classify → ntfy.
    If AHA_CATCHER_IDEAS_DIR (and optional mirror) are set and AHA_CATCHER_QUICK_MEMO_SAVE_IDEAS is on (default),
    writes the same Markdown shape as the web app to every ideas folder (iCloud + local mirror).
    Auth: X-Aha-Quick-Memo-Token must match AHA_CATCHER_QUICK_MEMO_SECRET.
    """
    secret = (os.getenv("AHA_CATCHER_QUICK_MEMO_SECRET") or "").strip()
    if not secret:
        raise HTTPException(
            status_code=503,
            detail="Set AHA_CATCHER_QUICK_MEMO_SECRET in .env to enable /api/quick-memo.",
        )
    if (x_aha_quick_memo_token or "").strip() != secret:
        raise HTTPException(status_code=403, detail="Invalid or missing X-Aha-Quick-Memo-Token header.")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty upload")
    if len(raw) > QUICK_MEMO_MAX_RAW_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large (max {QUICK_MEMO_MAX_RAW_MB} MB before trim)",
        )

    orig_name = (file.filename or "memo.m4a").strip() or "memo.m4a"
    clip, up_name = _trim_audio_first_seconds(raw, orig_name, QUICK_MEMO_TRIM_SEC)
    if len(clip) > QUICK_MEMO_MAX_CLIP_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"Processed audio too large (max {QUICK_MEMO_MAX_CLIP_BYTES // (1024 * 1024)} MB); "
                "install ffmpeg or raise AHA_CATCHER_QUICK_MEMO_MAX_MB."
            ),
        )
    ctype = _guess_audio_content_type(up_name)

    tr = await _forward_transcribe_to_ai_builder(clip, up_name, ctype)
    if isinstance(tr, JSONResponse):
        return tr
    transcript = ""
    if isinstance(tr, dict):
        tx = tr.get("text")
        if isinstance(tx, str):
            transcript = tx.strip()

    _need_key()
    cls = await _classify_transcript_core(transcript)

    fp = (fingerprint or "").strip()[:500]
    tags_raw = cls.get("tags")
    tags_list: list[str] = []
    if isinstance(tags_raw, list):
        tags_list = [str(t) for t in tags_raw[:16] if str(t).strip()]

    cat_str = str(cls.get("category") or "unsorted")
    rs_val = cls.get("reading_source")
    reading_src: Optional[str] = str(rs_val).strip() if isinstance(rs_val, str) else None

    created_iso = datetime.now(timezone.utc).isoformat()
    md_filename = _quick_memo_md_filename(orig_name, transcript)
    md_content = _build_quick_memo_markdown(
        transcript=transcript,
        category=cat_str,
        extra_tags=tags_list,
        reading_source=reading_src,
        created_iso=created_iso,
    )

    saved_paths: list[str] = []
    ideas_save_warnings: list[str] | None = None
    if _env_flag_true("AHA_CATCHER_QUICK_MEMO_SAVE_IDEAS", "1"):
        ok, bad = _quick_memo_save_to_ideas_dirs(filename=md_filename, content=md_content)
        saved_paths = ok
        if bad:
            ideas_save_warnings = bad
        if ok:
            print(f"[ahacatcher] quick-memo: saved to ideas: {ok}", file=sys.stderr)
            _metrics_append(
                {
                    "event": "quick_memo_saved",
                    "paths_n": len(ok),
                    "md_filename": md_filename,
                },
            )
        elif not _ideas_dirs():
            print(
                "[ahacatcher] quick-memo: ideas save skipped (set AHA_CATCHER_IDEAS_DIR to enable)",
                file=sys.stderr,
            )

    background_tasks.add_task(
        _notify_ntfy_quick_memo,
        category=cat_str,
        tags=tags_list,
        why=cls.get("why") if isinstance(cls.get("why"), str) else None,
        transcript_preview=transcript[:2000],
        fingerprint=fp,
    )
    _metrics_append(
        {
            "event": "quick_memo",
            "text_chars": len(transcript),
            "category": cls.get("category"),
            "model": cls.get("model"),
        },
    )

    print(
        f"[ahacatcher] quick-memo: bytes_in={len(raw)} clip={len(clip)} "
        f"cat={cls.get('category')!r} fp={fp!r}",
        file=sys.stderr,
    )

    out: dict[str, Any] = {
        "ok": True,
        "category": cls.get("category"),
        "tags": cls.get("tags"),
        "why": cls.get("why"),
        "reading_source": cls.get("reading_source"),
        "transcript": transcript,
        "trim_seconds": QUICK_MEMO_TRIM_SEC,
        "bytes_uploaded": len(raw),
        "bytes_transcribed": len(clip),
        "classify_model": cls.get("model"),
        "fingerprint": fp or None,
        "md_filename": md_filename,
        "saved_to_ideas": saved_paths if saved_paths else None,
        "ideas_save_warnings": ideas_save_warnings,
    }
    return out


app.mount("/", StaticFiles(directory=str(HERE), html=True), name="static")
