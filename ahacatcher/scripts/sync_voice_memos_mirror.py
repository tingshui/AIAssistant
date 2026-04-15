#!/usr/bin/env python3
"""
Mirror Apple Voice Memos (macOS) audio into a folder you own (two-way hygiene on the mirror).

- Copies new/changed recordings into <dest>/<label>/ relative paths (incremental by size + mtime).
- By default **prunes** mirror files under each successfully scanned source tree when the source file
  is gone (e.g. you deleted the recording in Voice Memos). Use --no-prune for copy-only behavior.
- Does not modify source files or ahacatcher.
- Requires Full Disk Access for the Python binary that runs this script, or copies will fail on
  ~/Library/Group Containers/... (TCC).

Usage:
  python3 ahacatcher/scripts/sync_voice_memos_mirror.py
  python3 ahacatcher/scripts/sync_voice_memos_mirror.py --no-prune
  VOICE_MEMOS_MIRROR_NO_PRUNE=1 python3 ahacatcher/scripts/sync_voice_memos_mirror.py

Default mirror folder: ~/Documents/VoiceMemosMirror (override with --dest or VOICE_MEMOS_MIRROR_DEST).

Cron/launchd: run every N minutes. ahacatcher Import defaults to the same mirror path (see README).
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
from background_monitor_log import append_event as _bg_append


def _eprint(*a: object) -> None:
    print(*a, file=sys.stderr)


def _executable_symlink_chain() -> tuple[str, str]:
    """(argv0 style path, realpath). venv/bin/python3 is a symlink — FDA picker greys those out."""
    exe = sys.executable
    try:
        real = os.path.realpath(exe)
    except OSError:
        real = exe
    return exe, real


def _likely_ide_terminal() -> str:
    """Best-effort: IDE-integrated shells often need the host .app in FDA too."""
    tp = (os.environ.get("TERM_PROGRAM") or "").strip().lower()
    if tp == "vscode":
        return "vscode"  # Cursor / VS Code family
    if "cursor" in (os.environ.get("GIT_ASKPASS") or "").lower():
        return "cursor"
    return ""


def _print_fda_help() -> None:
    exe, real = _executable_symlink_chain()
    host = _likely_ide_terminal()
    _eprint("[voice-memos-mirror] 完全磁盘访问权限 — 若 bin 里的 python3 呈灰色无法选取：")
    _eprint("[voice-memos-mirror] 它是符号链接。在系统设置点「+」打开文件框后按 ⌘⇧G（前往文件夹），粘贴：")
    _eprint(f"[voice-memos-mirror]   {real}")
    _eprint("[voice-memos-mirror] 选对文件后「打开」，再勾选列表中的该项（开关必须为**打开**状态）。")
    if real != exe:
        _eprint(f"[voice-memos-mirror] （venv 入口为 {exe}，请以解析后的真实文件为准。）")
    _eprint(
        "[voice-memos-mirror] **仍在报 PermissionError 时：** 往往要同时添加 **宿主 App** + **上面这个 Python**，"
        "两者列表里都要勾选。"
    )
    if host == "vscode":
        _eprint(
            "[voice-memos-mirror] 检测到 TERM_PROGRAM=vscode（多为 Cursor / VS Code 集成终端）："
            "请务必添加 **Cursor.app**："
        )
        _eprint("[voice-memos-mirror]   /Applications/Cursor.app")
        _eprint(
            "[voice-memos-mirror] 然后使用 **Command+Q 完全退出 Cursor**（只关窗口不算），再重新打开后运行本脚本。"
        )
    else:
        _eprint("[voice-memos-mirror] 若你当前在 **Cursor** 里跑命令：仍请添加：")
        _eprint("[voice-memos-mirror]   /Applications/Cursor.app")
        _eprint("[voice-memos-mirror] 若在系统「终端.app」里跑：请添加：")
        _eprint("[voice-memos-mirror]   /System/Applications/Utilities/Terminal.app")


# .qta = QuickTime Audio (Voice Memos on newer macOS / iOS alongside .m4a)
VOICE_SUFFIXES = frozenset({".m4a", ".mp4", ".m4v", ".wav", ".caf", ".qta"})


def _is_strict_descendant(path: Path, ancestor: Path) -> bool:
    if path == ancestor:
        return False
    try:
        path.relative_to(ancestor)
        return True
    except ValueError:
        return False


def _prune_redundant_roots(roots: list[Path]) -> list[Path]:
    """Drop roots that are subfolders of another root (scan parent once)."""
    resolved: list[Path] = []
    for r in roots:
        try:
            resolved.append(r.resolve())
        except OSError:
            continue
    kept: list[Path] = []
    for r in resolved:
        if any(_is_strict_descendant(r, k) for k in resolved if k != r):
            continue
        if r not in kept:
            kept.append(r)
    return kept


def _default_source_roots(extra: list[Path]) -> list[Path]:
    home = Path.home()
    roots: list[Path] = [
        home / "Library/Group Containers/group.com.apple.VoiceMemos.shared",
        home / "Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings",
        home / "Library/Application Support/com.apple.voicememos",
        home / "Library/Application Support/com.apple.voicememos/Recordings",
        home / "Music/iTunes/iTunes Media/Voice Memos",
    ]
    raw = (os.environ.get("VOICE_MEMOS_MIRROR_EXTRA_SOURCES") or "").strip()
    if raw:
        for part in raw.split(","):
            p = Path(part.strip()).expanduser()
            if str(p):
                roots.append(p)
    roots.extend(extra)
    out: list[Path] = []
    seen: set[str] = set()
    for r in roots:
        try:
            rp = r.resolve()
        except OSError:
            continue
        k = str(rp)
        if k in seen:
            continue
        seen.add(k)
        out.append(rp)
    return _prune_redundant_roots(out)


def _label_for_root(root: Path) -> str:
    s = str(root.resolve()).lower()
    if "voicememos.shared" in s:
        return "VoiceMemos.shared"
    if "com.apple.voicememos" in s:
        return "legacy.voicememos"
    if "voice memos" in s and "itunes" in s:
        return "iTunes.VoiceMemos"
    h = hashlib.sha1(str(root.resolve()).encode("utf-8")).hexdigest()[:10]
    return f"src_{h}"


def _should_skip_copy(src: Path, dst: Path) -> bool:
    if not dst.is_file():
        return False
    try:
        sa = src.stat()
        sb = dst.stat()
        return sa.st_size == sb.st_size and int(sa.st_mtime_ns) == int(sb.st_mtime_ns)
    except OSError:
        return False


def _remove_empty_parents_up_to(file_path: Path, mirror_label_dir: Path) -> None:
    """Remove empty directories from file_path.parent upward until mirror_label_dir (exclusive)."""
    try:
        mirror_res = mirror_label_dir.resolve()
    except OSError:
        return
    cur = file_path.parent
    while True:
        try:
            cur_res = cur.resolve()
        except OSError:
            break
        if cur_res == mirror_res:
            break
        try:
            cur_res.relative_to(mirror_res)
        except ValueError:
            break
        try:
            next(cur.iterdir())
            break
        except StopIteration:
            try:
                cur.rmdir()
            except OSError:
                break
            cur = cur.parent


def run_mirror(
    dest_root: Path,
    source_roots: list[Path],
    dry_run: bool,
    verbose: bool,
    prune: bool,
) -> int:
    dest_root = dest_root.expanduser().resolve()
    copied = skipped = removed = errors = 0
    permission_hint = False
    roots_synced_ok: list[tuple[Path, str]] = []

    if not dry_run:
        try:
            dest_root.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            _eprint(f"[voice-memos-mirror] cannot create dest {dest_root}: {e}")
            _bg_append(
                "voice_memos_sync",
                ok=False,
                errors=1,
                copied=0,
                skipped=0,
                removed=0,
                dry_run=dry_run,
                dest=str(dest_root),
                reason="cannot_create_dest",
                detail=str(e),
            )
            return 2

    for root in source_roots:
        label = _label_for_root(root)
        try:
            if not root.exists() or not root.is_dir():
                if verbose:
                    _eprint(f"[voice-memos-mirror] skip (missing or not dir): {root}")
                continue
        except OSError as e:
            _eprint(f"[voice-memos-mirror] skip root {root}: {e}")
            continue

        try:
            with os.scandir(root) as probe:
                next(probe, None)
        except PermissionError:
            permission_hint = True
            _eprint(f"[voice-memos-mirror] PermissionError: cannot read {root}")
            errors += 1
            continue
        except OSError as e:
            _eprint(f"[voice-memos-mirror] cannot list {root}: {e}")
            errors += 1
            continue

        root_base = root.resolve()
        scan_ok = False
        try:
            for src in root.rglob("*"):
                if not src.is_file():
                    continue
                if src.suffix.lower() not in VOICE_SUFFIXES:
                    continue
                try:
                    rel = src.relative_to(root_base)
                except ValueError:
                    rel = Path(src.name)
                dst = dest_root / label / rel
                if _should_skip_copy(src, dst):
                    skipped += 1
                    continue
                if dry_run:
                    _eprint(f"would copy -> {dst}")
                    copied += 1
                    continue
                try:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                    copied += 1
                    if verbose:
                        _eprint(f"copied {src.name} -> {dst}")
                except PermissionError:
                    permission_hint = True
                    _eprint(f"[voice-memos-mirror] PermissionError reading {src}")
                    errors += 1
                except OSError as e:
                    _eprint(f"[voice-memos-mirror] copy failed {src} -> {dst}: {e}")
                    errors += 1
            scan_ok = True
        except PermissionError:
            permission_hint = True
            _eprint(f"[voice-memos-mirror] PermissionError during scan under {root}")
            errors += 1
        if scan_ok:
            roots_synced_ok.append((root_base, label))

    if prune and roots_synced_ok:
        for root_base, label in roots_synced_ok:
            mirror_sub = dest_root / label
            if not mirror_sub.is_dir():
                continue
            try:
                for mir in mirror_sub.rglob("*"):
                    if not mir.is_file():
                        continue
                    if mir.suffix.lower() not in VOICE_SUFFIXES:
                        continue
                    try:
                        rel = mir.relative_to(mirror_sub)
                    except ValueError:
                        continue
                    src_expected = root_base / rel
                    if src_expected.is_file():
                        continue
                    if dry_run:
                        _eprint(f"would remove (missing in source) -> {mir}")
                        removed += 1
                        continue
                    try:
                        mir.unlink()
                        removed += 1
                        if verbose:
                            _eprint(f"removed {mir.name} (source gone)")
                        _remove_empty_parents_up_to(mir, mirror_sub)
                    except OSError as e:
                        _eprint(f"[voice-memos-mirror] remove failed {mir}: {e}")
                        errors += 1
            except OSError as e:
                _eprint(f"[voice-memos-mirror] prune scan failed under {mirror_sub}: {e}")
                errors += 1

    _eprint(
        f"[voice-memos-mirror] done: copied={copied} skipped={skipped} removed={removed} "
        f"errors={errors} dest={dest_root}"
    )
    if permission_hint:
        _eprint(
            "[voice-memos-mirror] 请到：系统设置 → 隐私与安全性 → 完全磁盘访问权限 → 点「+」"
        )
        _print_fda_help()
        _eprint(
            "[voice-memos-mirror] 勾选后**完全退出**终端（或 Cursor）再打开，重新运行本脚本。"
        )
    _bg_append(
        "voice_memos_sync",
        ok=(errors == 0),
        errors=errors,
        copied=copied,
        skipped=skipped,
        removed=removed,
        dry_run=dry_run,
        dest=str(dest_root),
        permission_hint=permission_hint,
    )
    return 1 if errors else 0


def main() -> int:
    p = argparse.ArgumentParser(
        description="Sync Voice Memos audio into a mirror folder (copy + optional prune when source is gone).",
    )
    _env_dest = (os.environ.get("VOICE_MEMOS_MIRROR_DEST") or "").strip()
    _default_mirror = _env_dest or str(Path.home() / "Documents" / "VoiceMemosMirror")
    p.add_argument(
        "--dest",
        type=str,
        default=_default_mirror,
        help=f"Mirror folder (default: {_default_mirror}; or set VOICE_MEMOS_MIRROR_DEST).",
    )
    p.add_argument(
        "--extra-source",
        action="append",
        default=[],
        metavar="DIR",
        help="Additional source directory (repeatable). Also see VOICE_MEMOS_MIRROR_EXTRA_SOURCES.",
    )
    p.add_argument("--dry-run", action="store_true", help="Print actions only; no writes.")
    p.add_argument(
        "--print-fda-paths",
        action="store_true",
        help="Print paths for macOS Full Disk Access (symlink-resolved); then exit.",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument(
        "--no-prune",
        action="store_true",
        help="Do not delete mirror files when the source recording is gone (copy-only).",
    )
    args = p.parse_args()
    if args.print_fda_paths:
        _print_fda_help()
        return 0
    dest = Path(args.dest)
    extra = [Path(x).expanduser() for x in (args.extra_source or []) if str(x).strip()]
    roots = _default_source_roots(extra)
    env_no_prune = (os.environ.get("VOICE_MEMOS_MIRROR_NO_PRUNE") or "").strip().lower()
    no_prune_env = env_no_prune in ("1", "true", "yes", "on")
    prune = not args.no_prune and not no_prune_env
    return run_mirror(dest, roots, args.dry_run, args.verbose, prune)


if __name__ == "__main__":
    raise SystemExit(main())
