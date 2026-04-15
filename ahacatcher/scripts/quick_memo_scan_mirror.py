#!/usr/bin/env python3
"""
Scan Voice Memos mirror folder(s) for new/changed audio files and POST each to
ahacatcher POST /api/quick-memo (same as iPhone Shortcuts).

Use with launchd/cron *after* ahacatcher/scripts/sync_voice_memos_mirror.py so files exist locally first.

Env (cursor-project/.env):
  AHA_CATCHER_QUICK_MEMO_SECRET — required
  AHA_CATCHER_QUICK_MEMO_LOCAL_URL — default http://127.0.0.1:8765
  AHA_CATCHER_VOICE_MEMOS_MIRROR_DIR — optional; default ~/Documents/VoiceMemosMirror
  AHA_CATCHER_VOICE_MEMOS_DIRS — optional comma-separated extra roots (same as ahacatcher)

State: ahacatcher/data/quick_memo_mirror_state.json (paths already successfully posted).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

# Repo root: ahacatcher/scripts/<this>.py → parents[2]
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
from background_monitor_log import append_event as _bg_append

GLOBS = ("*.m4a", "*.mp4", "*.qta", "*.caf", "*.wav")
STATE_PATH = ROOT / "ahacatcher" / "data" / "quick_memo_mirror_state.json"


def _eprint(*a: object) -> None:
    print(*a, file=sys.stderr)


def _mirror_roots() -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    raw_m = (os.getenv("AHA_CATCHER_VOICE_MEMOS_MIRROR_DIR") or "").strip()
    if raw_m:
        p = Path(raw_m).expanduser().resolve()
        if p.is_dir():
            key = str(p)
            if key not in seen:
                seen.add(key)
                out.append(p)
    else:
        d = Path.home() / "Documents" / "VoiceMemosMirror"
        if d.is_dir():
            key = str(d.resolve())
            if key not in seen:
                seen.add(key)
                out.append(d)
    extra = (os.getenv("AHA_CATCHER_VOICE_MEMOS_DIRS") or "").strip()
    if extra:
        for part in extra.split(","):
            p = Path(part.strip()).expanduser().resolve()
            if p.is_dir():
                key = str(p)
                if key not in seen:
                    seen.add(key)
                    out.append(p)
    return out


def _guess_ct(name: str) -> str:
    ext = Path(name).suffix.lower()
    if ext == ".wav":
        return "audio/wav"
    if ext in (".m4a", ".mp4", ".m4v"):
        return "audio/mp4"
    if ext == ".caf":
        return "audio/x-caf"
    if ext == ".qta":
        return "video/quicktime"
    return "application/octet-stream"


def _load_state() -> dict[str, dict]:
    if not STATE_PATH.is_file():
        return {}
    try:
        raw = STATE_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as e:
        _eprint(f"[quick-memo-scan] state read failed: {e}")
        return {}


def _save_state(state: dict[str, dict]) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=0) + "\n", encoding="utf-8")
    except OSError as e:
        _eprint(f"[quick-memo-scan] state write failed: {e}")


def _collect_audio_files(roots: list[Path]) -> list[Path]:
    found: list[Path] = []
    for root in roots:
        try:
            for pat in GLOBS:
                for p in root.rglob(pat):
                    if p.is_file():
                        found.append(p)
        except OSError as e:
            _eprint(f"[quick-memo-scan] scan error under {root}: {e}")
    # newest first (process recent recordings first)
    try:
        found.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    except OSError:
        pass
    return found


def main() -> int:
    ap = argparse.ArgumentParser(description="POST new mirror audio files to /api/quick-memo")
    ap.add_argument("--dry-run", action="store_true", help="List files that would be posted, no HTTP")
    args = ap.parse_args()

    secret = (os.getenv("AHA_CATCHER_QUICK_MEMO_SECRET") or "").strip()
    if not secret:
        _eprint("[quick-memo-scan] Set AHA_CATCHER_QUICK_MEMO_SECRET in .env")
        return 2

    base = (os.getenv("AHA_CATCHER_QUICK_MEMO_LOCAL_URL") or "http://127.0.0.1:8765").rstrip("/")
    url = f"{base}/api/quick-memo"

    roots = _mirror_roots()
    if not roots:
        _eprint("[quick-memo-scan] No mirror directory found (VoiceMemosMirror or AHA_CATCHER_* env).")
        return 1

    state = _load_state()
    files = _collect_audio_files(roots)
    posted = 0
    skipped = 0

    with httpx.Client(timeout=300.0) as client:
        for path in files:
            try:
                st = path.stat()
            except OSError:
                continue
            key = str(path.resolve())
            fp = f"{path.name}|{int(st.st_mtime)}|{st.st_size}"
            prev = state.get(key)
            if prev and prev.get("mtime") == st.st_mtime and prev.get("size") == st.st_size:
                skipped += 1
                continue

            if args.dry_run:
                _eprint(f"[dry-run] would POST {key}")
                continue

            ctype = _guess_ct(path.name)
            try:
                body = path.read_bytes()
            except OSError as e:
                _eprint(f"[quick-memo-scan] skip read {path}: {e}")
                continue

            try:
                resp = client.post(
                    url,
                    headers={"X-Aha-Quick-Memo-Token": secret},
                    files={"file": (path.name, body, ctype)},
                    data={"fingerprint": fp},
                )
            except httpx.RequestError as e:
                _eprint(f"[quick-memo-scan] HTTP error {path.name}: {e}")
                continue

            if resp.status_code not in (200, 201):
                preview = (resp.text or "").replace("\n", " ")[:400]
                _eprint(f"[quick-memo-scan] HTTP {resp.status_code} {path.name}: {preview}")
                continue

            state[key] = {"mtime": st.st_mtime, "size": st.st_size, "fingerprint": fp}
            _save_state(state)
            posted += 1
            _eprint(f"[quick-memo-scan] ok {path.name} -> quick-memo")

    if args.dry_run:
        _eprint(f"[dry-run] done roots={len(roots)} files={len(files)}")
        _bg_append(
            "quick_memo_scan",
            dry_run=True,
            posted=0,
            skipped_already_done=skipped,
            scanned_files=len(files),
            roots=[str(r) for r in roots],
        )
        return 0

    _eprint(
        f"[quick-memo-scan] done posted={posted} skipped_already_done={skipped} "
        f"scanned_files={len(files)} roots={[str(r) for r in roots]}",
    )
    _bg_append(
        "quick_memo_scan",
        dry_run=False,
        posted=posted,
        skipped_already_done=skipped,
        scanned_files=len(files),
        roots=[str(r) for r in roots],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
