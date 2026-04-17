"""
Layered extraction definitions (domain → source_kind → document_family → intent).

Loads `AI.assistant/config/extraction_profiles.json` and resolves which profile applies
to a record, including chunk-selection for LLM extraction text.
"""
from __future__ import annotations

import json
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _config_path() -> Path:
    # .../AI.assistant/src/ai_assistant/extraction_profiles.py -> parents[2] == AI.assistant
    return Path(__file__).resolve().parents[2] / "config" / "extraction_profiles.json"


@lru_cache(maxsize=1)
def load_catalog() -> Dict[str, Any]:
    p = _config_path()
    if not p.is_file():
        raise FileNotFoundError(f"extraction profile catalog missing: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def reset_catalog_cache() -> None:
    load_catalog.cache_clear()


def _row_to_record_meta(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    keys = row.keys() if hasattr(row, "keys") else []
    return {str(k): row[k] for k in keys}


def infer_source_kind(meta: Dict[str, Any]) -> str:
    sk = (meta.get("source_kind") or "").strip()
    if sk:
        return sk
    dom = (meta.get("domain") or "").strip().lower()
    if dom == "health":
        return "clinical_document"
    if dom == "travel":
        return "generic_blob"
    return "generic_blob"


def infer_document_family(meta: Dict[str, Any]) -> str:
    df = (meta.get("document_family") or "").strip()
    if df:
        return df
    if infer_source_kind(meta) != "clinical_document":
        return "unknown"
    notes = (meta.get("notes") or "").lower()
    if "[encounter]" in notes:
        return "yearly_longitudinal_summary"
    rk = (meta.get("record_kind") or "").lower()
    if "summary" in rk or "annual" in rk or "yearly" in rk:
        return "yearly_longitudinal_summary"
    return "single_visit_note"


def infer_extraction_intent(meta: Dict[str, Any]) -> str:
    ei = (meta.get("extraction_intent") or "").strip()
    if ei:
        return ei
    if infer_source_kind(meta) != "clinical_document":
        return "default"
    notes = (meta.get("notes") or "").lower()
    if "[encounter]" in notes:
        return "visits_only"
    fam = infer_document_family(meta)
    if fam == "yearly_longitudinal_summary":
        return "visits_only"
    return "default"


def resolve_profile_id(meta: Dict[str, Any]) -> str:
    pid = (meta.get("extraction_profile_id") or "").strip()
    if pid:
        profiles = load_catalog().get("profiles") or {}
        if pid in profiles:
            return pid
    domain = (meta.get("domain") or "health").strip().lower()
    sk = infer_source_kind(meta)
    fam = infer_document_family(meta)
    intent = infer_extraction_intent(meta)
    defaults = (load_catalog().get("defaults") or {}).get(domain, {})
    d1 = defaults.get(sk) or {}
    d2 = d1.get(fam) or {}
    resolved = (d2.get(intent) or d2.get("default") or "").strip()
    if resolved:
        return resolved
    return "fallback.all_chunks"


def get_profile(meta: Dict[str, Any]) -> Dict[str, Any]:
    pid = resolve_profile_id(meta)
    profiles = load_catalog().get("profiles") or {}
    prof = profiles.get(pid)
    if not isinstance(prof, dict):
        profiles = load_catalog().get("profiles") or {}
        prof = profiles.get("fallback.all_chunks") or {"chunk_strategy": {"type": "all_chunks"}}
    out = dict(prof)
    out["_resolved_profile_id"] = pid
    return out


def compute_stored_profile_fields(
    *,
    domain: str,
    record_kind: str,
    notes: Optional[str],
    extract_strategy: Optional[str] = None,
    source_kind: Optional[str] = None,
    document_family: Optional[str] = None,
    extraction_intent: Optional[str] = None,
    extraction_profile_id: Optional[str] = None,
) -> Tuple[str, str, str, str]:
    """
    Returns (source_kind, document_family, extraction_intent, extraction_profile_id) to persist on `records`.
    """
    meta: Dict[str, Any] = {
        "domain": (domain or "").strip() or "health",
        "record_kind": record_kind or "",
        "notes": notes or "",
        "source_kind": (source_kind or "").strip() or None,
        "document_family": (document_family or "").strip() or None,
        "extraction_intent": (extraction_intent or "").strip() or None,
        "extraction_profile_id": (extraction_profile_id or "").strip() or None,
    }
    if extract_strategy == "encounters":
        if not (meta.get("document_family") or "").strip():
            meta["document_family"] = "yearly_longitudinal_summary"
        if not (meta.get("extraction_intent") or "").strip():
            meta["extraction_intent"] = "visits_only"
    sk = infer_source_kind(meta)
    fam = infer_document_family({**meta, "source_kind": sk})
    ei = infer_extraction_intent({**meta, "source_kind": sk, "document_family": fam})
    meta2 = {**meta, "source_kind": sk, "document_family": fam, "extraction_intent": ei}
    pid = resolve_profile_id(meta2)
    return sk, fam, ei, pid


def _norm(s: str, case_insensitive: bool) -> str:
    return s.lower() if case_insensitive else s


def _find_span_in_text(
    full_text: str,
    *,
    start_markers: List[str],
    end_markers: List[str],
    case_insensitive: bool,
) -> Optional[Tuple[int, int]]:
    if not full_text.strip():
        return None
    hay = _norm(full_text, case_insensitive) if case_insensitive else full_text
    starts = [m for m in start_markers if m]
    ends = [m for m in end_markers if m]
    if not starts:
        return None
    best_lo: Optional[int] = None
    for sm in starts:
        needle = _norm(sm, case_insensitive) if case_insensitive else sm
        pos = hay.find(needle) if case_insensitive else full_text.find(sm)
        if pos < 0:
            continue
        if best_lo is None or pos < best_lo:
            best_lo = pos
    if best_lo is None:
        return None
    hi = len(full_text)
    best_hi: Optional[int] = None
    search_from = best_lo + 1
    for em in ends:
        needle = _norm(em, case_insensitive) if case_insensitive else em
        sub = hay[search_from:] if case_insensitive else full_text[search_from:]
        pos = sub.find(needle) if case_insensitive else full_text.find(em, search_from)
        if pos < 0:
            continue
        abs_pos = search_from + pos
        if best_hi is None or abs_pos < best_hi:
            best_hi = abs_pos
    if best_hi is None:
        best_hi = hi
    return best_lo, best_hi


def select_chunk_texts_for_extraction(
    *,
    chunk_rows: List[Dict[str, Any]],
    record_meta: Dict[str, Any],
) -> Tuple[str, str]:
    """
    Returns (joined_text, debug_note).

    chunk_rows: rows dicts with at least chunk_index, text (ordered by caller).
    """
    rows = sorted(chunk_rows, key=lambda r: int(r.get("chunk_index") or 0))
    texts = [str(r.get("text") or "") for r in rows]
    prof = get_profile(record_meta)
    pid = str(prof.get("_resolved_profile_id") or "")
    strat = prof.get("chunk_strategy") or {}
    stype = str(strat.get("type") or "all_chunks").strip()

    if stype == "all_chunks" or not texts:
        joined = "\n\n".join(t for t in texts if t).strip()
        return joined, f"profile={pid} strategy=all_chunks chunks={len(texts)}"

    if stype == "section_between_markers":
        sep = "\n\n"
        parts: List[str] = []
        offsets: List[Tuple[int, int, int]] = []  # chunk_index, lo, hi exclusive hi in full_text
        pos = 0
        for r in rows:
            idx = int(r.get("chunk_index") or 0)
            t = str(r.get("text") or "")
            if parts:
                pos += len(sep)
                parts.append(sep)
            lo = pos
            parts.append(t)
            pos += len(t)
            offsets.append((idx, lo, pos))
        full_text = "".join(parts)
        start_markers = list(strat.get("start_markers") or [])
        end_markers = list(strat.get("end_markers") or [])
        ci = bool(strat.get("case_insensitive", True))
        span = _find_span_in_text(
            full_text,
            start_markers=start_markers,
            end_markers=end_markers,
            case_insensitive=ci,
        )
        if not span:
            mode = str(strat.get("if_no_match") or "use_all_chunks")
            if mode == "use_all_chunks":
                joined = "\n\n".join(t for t in texts if t).strip()
                return joined, f"profile={pid} strategy=section_between_markers no_marker_match fallback=all chunks={len(texts)}"
            return "", f"profile={pid} strategy=section_between_markers no_marker_match fallback=empty"

        span_lo, span_hi = span
        picked: List[str] = []
        picked_idx: List[int] = []
        for r in rows:
            idx = int(r.get("chunk_index") or 0)
            t = str(r.get("text") or "")
            span_row = next((c for c in offsets if c[0] == idx), None)
            if not span_row:
                continue
            _, a, b = span_row
            if b <= span_lo or a >= span_hi:
                continue
            picked.append(t)
            picked_idx.append(idx)
        joined = sep.join(picked).strip()
        return joined, f"profile={pid} strategy=section_between_markers span={span_lo}:{span_hi} picked_chunks={picked_idx}"

    joined = "\n\n".join(t for t in texts if t).strip()
    return joined, f"profile={pid} unknown_strategy={stype!r} fallback=all_chunks"


def select_chunk_texts_for_extraction_safe(
    *,
    chunk_rows: List[Dict[str, Any]],
    record_meta: Dict[str, Any],
) -> Tuple[str, str]:
    try:
        return select_chunk_texts_for_extraction(chunk_rows=chunk_rows, record_meta=record_meta)
    except Exception as e:  # noqa: BLE001
        print(f"[extraction_profiles] select failed: {e}", file=sys.stderr)
        rows = sorted(chunk_rows, key=lambda r: int(r.get("chunk_index") or 0))
        joined = "\n\n".join(str(r.get("text") or "") for r in rows).strip()
        return joined, f"error_fallback all_chunks err={e!s}"
