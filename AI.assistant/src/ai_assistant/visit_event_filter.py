from __future__ import annotations

import json
import os
import re
import sys
import concurrent.futures
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _ensure_llm_path() -> None:
    shared = _project_root() / "tools" / "shared"
    s = str(shared)
    if s not in sys.path:
        sys.path.insert(0, s)


def _timeout_seconds_for_provider(provider: str) -> float:
    if provider == "local":
        try:
            return float(os.getenv("LOCAL_LLM_TIMEOUT_SECONDS", "45") or "45")
        except Exception:
            return 45.0
    try:
        return float(os.getenv("LLM_TIMEOUT_SECONDS", "90") or "90")
    except Exception:
        return 90.0


def _query_llm_with_timeout(*, prompt: str, client: Any, provider: str, model: Optional[str]) -> str:
    from llm_api import query_llm  # type: ignore

    timeout_s = _timeout_seconds_for_provider(provider)
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(lambda: query_llm(prompt, client=client, model=model, provider=provider, raise_on_error=True))
    try:
        out = fut.result(timeout=timeout_s)
        return str(out or "")
    finally:
        ex.shutdown(wait=False, cancel_futures=True)


def _strip_code_fences(s: str) -> str:
    t = (s or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\n?", "", t)
        t = re.sub(r"\n?```\s*$", "", t)
    return t.strip()


def _extract_first_json_object(raw: str) -> str:
    s = _strip_code_fences(raw)
    start = s.find("{")
    end = s.rfind("}")
    if start >= 0 and end > start:
        return s[start : end + 1]
    return s


def _json_loads_best_effort(raw: str) -> Dict[str, Any]:
    s = _extract_first_json_object(raw)
    try:
        return json.loads(s)
    except json.JSONDecodeError as e:
        if "Invalid control character" in str(e):
            cleaned = "".join(ch if ord(ch) >= 32 else " " for ch in s)
            return json.loads(cleaned)
        raise


def _numbered_excerpt(lines: List[str]) -> str:
    return "\n".join(f"{i + 1}|{line}" for i, line in enumerate(lines))


def _build_vitals_strip_prompt(*, numbered: str, n_lines: int) -> str:
    return f"""You are helping filter text before clinical-field extraction.

The excerpt has {n_lines} lines. Each line starts with "LINE_NUMBER|" followed by the original line text.

Task: find contiguous line ranges that are PRIMARILY vital signs / measurements blocks, for example:
- Blood pressure, heart rate, pulse, temperature, SpO2, respiratory rate
- Height, weight, BMI
- A block that is mostly these measurements (often repeated dates per measurement)

Do NOT mark as vitals:
- Encounter / office visit narrative, chief complaint, assessment, plan, diagnoses, orders
- A line that only mentions a date of service without being a vitals table

Return ONLY JSON (no markdown, no commentary) with this exact shape:
{{
  "vitals_ranges": [
    {{"line_start": 1, "line_end": 12}}
  ]
}}

Rules:
- line_start and line_end are inclusive, 1-based, referring to the number before "|".
- Ranges must be within 1..{n_lines}.
- Use an empty list if there are no vitals blocks.

Numbered excerpt:
---
{numbered}
---
"""


def _parse_vitals_ranges(d: Dict[str, Any], *, n_lines: int) -> List[Tuple[int, int]]:
    raw = d.get("vitals_ranges")
    out: List[Tuple[int, int]] = []
    if not isinstance(raw, list):
        return out
    for it in raw:
        if not isinstance(it, dict):
            continue
        try:
            a = int(it.get("line_start"))
            b = int(it.get("line_end"))
        except (TypeError, ValueError):
            continue
        if a < 1 or b < a or b > n_lines:
            continue
        out.append((a, b))
    out.sort(key=lambda x: (x[0], x[1]))
    return out


def _merge_ranges(ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not ranges:
        return []
    merged: List[Tuple[int, int]] = []
    cur_s, cur_e = ranges[0]
    for s, e in ranges[1:]:
        if s <= cur_e + 1:
            cur_e = max(cur_e, e)
        else:
            merged.append((cur_s, cur_e))
            cur_s, cur_e = s, e
    merged.append((cur_s, cur_e))
    return merged


def _strip_lines_by_ranges(lines: List[str], ranges: List[Tuple[int, int]]) -> Tuple[str, Dict[str, Any]]:
    n = len(lines)
    kill = [False] * n
    merged = _merge_ranges(ranges)
    for s, e in merged:
        for i in range(s - 1, e):
            if 0 <= i < n:
                kill[i] = True
    kept = [ln for i, ln in enumerate(lines) if not kill[i]]
    dbg = {
        "vitals_ranges_in": len(ranges),
        "vitals_ranges_merged": len(merged),
        "lines_total": n,
        "lines_removed": sum(kill),
        "lines_kept": len(kept),
    }
    return "\n".join(kept), dbg


def filter_text_to_visit_events(
    text: str,
    *,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Best-effort: ask LLM for vitals line ranges only (compact JSON), then remove those lines from the excerpt.

    Falls back to original `text` if anything fails.
    """
    prov = (provider or os.getenv("AI_ASSISTANT_VISITS_ONLY_SPAN_FILTER_PROVIDER", "local") or "local").strip() or "local"
    m = model or (os.getenv("AI_ASSISTANT_VISITS_ONLY_SPAN_FILTER_MODEL") or "").strip() or None
    dbg: Dict[str, Any] = {"provider": prov, "model": m or "(default)", "mode": "vitals_line_strip"}
    if not (text or "").strip():
        dbg.update({"len_in": 0, "len_out": 0})
        return text, dbg

    lines = text.splitlines()
    n = len(lines)
    dbg["len_in"] = len(text)
    if n == 0:
        dbg["len_out"] = 0
        return text, dbg

    numbered = _numbered_excerpt(lines)
    prompt = _build_vitals_strip_prompt(numbered=numbered, n_lines=n)

    _ensure_llm_path()
    from llm_api import create_llm_client  # type: ignore

    client = create_llm_client(prov)
    try:
        raw1 = _query_llm_with_timeout(prompt=prompt, client=client, provider=prov, model=m)
        d1 = _json_loads_best_effort(raw1)
        ranges = _parse_vitals_ranges(d1, n_lines=n)
    except Exception as e1:  # noqa: BLE001
        retry = prompt + (
            "\n\nIMPORTANT: Reply with ONLY one JSON object. "
            "Use only integers in line_start/line_end. No markdown fences. No extra text."
        )
        try:
            raw2 = _query_llm_with_timeout(prompt=retry, client=client, provider=prov, model=m)
            d1 = _json_loads_best_effort(raw2)
            ranges = _parse_vitals_ranges(d1, n_lines=n)
        except Exception as e2:  # noqa: BLE001
            dbg.update({"error": f"{type(e1).__name__}: {e1}; retry {type(e2).__name__}: {e2}"})
            dbg["len_out"] = len(text)
            return text, dbg

    if not ranges:
        dbg.update({"vitals_ranges_in": 0, "lines_removed": 0, "lines_kept": n, "len_out": len(text)})
        return text, dbg

    filtered, strip_dbg = _strip_lines_by_ranges(lines, ranges)
    dbg.update(strip_dbg)
    dbg["len_out"] = len(filtered)
    return filtered, dbg
