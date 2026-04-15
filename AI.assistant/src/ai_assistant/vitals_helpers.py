"""
Aggregate vitals_json from multiple records for Chart.js (age on x-axis).
Age uses visit_age_years/text (visit age), not current age.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple


def parse_age_years_fallback(age_text: Optional[str]) -> Optional[float]:
    if not age_text:
        return None
    s = str(age_text).strip().lower()
    if not s:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*(year|yr|y|岁|years?)", s)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+(?:\.\d+)?)\s*(month|mo|个月)", s)
    if m:
        return float(m.group(1)) / 12.0
    m = re.search(r"^(\d+(?:\.\d+)?)\s*$", s)
    if m:
        return float(m.group(1))
    return None


_VITAL_LABELS: Dict[str, str] = {
    "height_cm": "身高 (cm)",
    "weight_kg": "体重 (kg)",
    "heart_rate_bpm": "心率 (bpm)",
    "spo2_percent": "血氧 (%)",
    "temperature_c": "体温 (°C)",
    "bp_systolic": "收缩压 (mmHg)",
    "bp_diastolic": "舒张压 (mmHg)",
    "respiratory_rate": "呼吸频率",
    "other": "其它",
}


def chart_series_from_records(rows: List[Any]) -> Tuple[List[Dict[str, Any]], bool]:
    by_name: Dict[str, List[Dict[str, Any]]] = {}

    for r in rows:
        vj = r["vitals_json"] if hasattr(r, "keys") else r.get("vitals_json")
        if not vj:
            continue
        try:
            arr = json.loads(str(vj))
        except json.JSONDecodeError:
            continue
        if not isinstance(arr, list):
            continue

        vy = r["visit_age_years"] if hasattr(r, "keys") else r.get("visit_age_years")
        vt = r["visit_age_text"] if hasattr(r, "keys") else r.get("visit_age_text")
        fallback_age: Optional[float] = None
        if vy is not None and str(vy).strip() != "":
            try:
                fallback_age = float(vy)
            except (TypeError, ValueError):
                fallback_age = None
        if fallback_age is None:
            fallback_age = parse_age_years_fallback(str(vt) if vt else None)

        rid = str(r["record_id"] if hasattr(r, "keys") else r.get("record_id") or "")

        for item in arr:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            try:
                val = float(item.get("value"))
            except (TypeError, ValueError):
                continue
            unit = str(item.get("unit") or "").strip()
            age_y = item.get("age_years")
            x: Optional[float] = None
            if age_y is not None and str(age_y).strip() != "":
                try:
                    x = float(age_y)
                except (TypeError, ValueError):
                    x = None
            if x is None:
                x = fallback_age
            if x is None:
                continue
            by_name.setdefault(name, []).append({"x": x, "y": val, "unit": unit, "record_id": rid})

    out: List[Dict[str, Any]] = []
    for key in sorted(by_name.keys()):
        pts = sorted(by_name[key], key=lambda p: p["x"])
        out.append({"key": key, "label": _VITAL_LABELS.get(key, key), "points": pts})
    return out, len(out) > 0

