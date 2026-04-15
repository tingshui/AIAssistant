"""
Local extraction + summarization helpers.

Uses repo `tools/shared/llm_api.py` (loads .env from cwd — run web server from repo root).
Web UI re-extract defaults to local (Ollama) via provider="local".
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _ensure_llm_path() -> None:
    shared = _project_root() / "tools" / "shared"
    s = str(shared)
    if s not in sys.path:
        sys.path.insert(0, s)


def _parse_json_object(raw: str) -> Dict[str, Any]:
    if not raw or not raw.strip():
        raise ValueError("模型返回为空")
    s = raw.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\n?", "", s)
        s = re.sub(r"\n?```\s*$", "", s)
        s = s.strip()
    def _loads_with_sanitize(text: str) -> Dict[str, Any]:
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            # Local models sometimes emit raw control chars inside strings; make best-effort parse.
            if "Invalid control character" in str(e):
                cleaned = "".join(ch if ord(ch) >= 32 else " " for ch in text)
                return json.loads(cleaned)
            raise

    try:
        return _loads_with_sanitize(s)
    except json.JSONDecodeError:
        start = s.find("{")
        end = s.rfind("}")
        if start >= 0 and end > start:
            return _loads_with_sanitize(s[start : end + 1])
        raise


def build_extraction_prompt(document_text: str, person_key_hint: str) -> str:
    return f"""You are a clinical document information extractor. Extract fields ONLY from the text below. If unknown, use an empty string \"\". Do not diagnose. Do not invent facts not supported by the text.

The database row is associated with family member profile key \"{person_key_hint}\" (may help disambiguate patient name).

Return ONLY a single JSON object with these keys:
- \"domain\": usually \"health\"
- \"subdomain\": one of: pediatrics, dermatology, primary_care, mental_health, labs, cardiology, orthopedics, ent, urgent_care, ob_gyn, dentistry, ophthalmology, other
- \"patient_name\"
- \"patient_age\" (e.g. \"35 years\", \"8 months\")
- \"doctor_name\"
- \"facility_name\" (hospital or clinic)
- \"visit_date\" (ISO YYYY-MM-DD if found in document, else \"\")
- \"visit_reason\" (chief complaint / reason for visit, concise)
- \"symptoms\"
- \"prescriptions\" (medications / prescriptions mentioned)
- \"clinical_detail\" (narrative: assessment, plan, key findings — plain text)
- \"vitals\": JSON array of objects, each with:
  - \"name\": one of height_cm, weight_kg, heart_rate_bpm, spo2_percent, temperature_c, bp_systolic, bp_diastolic, respiratory_rate, other
  - \"value\": number
  - \"unit\": short string (e.g. cm, kg, bpm)
  - \"age_years\": number or null

Document:
---
{document_text}
---
"""


def extract_health_metadata(
    document_text: str,
    *,
    person_key_hint: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    _ensure_llm_path()
    from llm_api import create_llm_client, query_llm  # type: ignore

    prov = provider or os.getenv("AI_ASSISTANT_EXTRACT_PROVIDER", "aibuilder")
    prompt = build_extraction_prompt(document_text, person_key_hint)
    client = create_llm_client(prov)
    raw = query_llm(prompt, client=client, model=model, provider=prov)
    if raw is None:
        raise RuntimeError("模型调用失败（无返回）。请检查本机 Ollama / LOCAL_LLM_BASE_URL 或云端 API配置。")
    return _parse_json_object(raw)


def normalize_extraction(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    domain = str(d.get("domain") or "health").strip().lower() or "health"
    sub = d.get("subdomain") or ""
    sub_s = str(sub).strip().lower().replace(" ", "_").replace("-", "_") if sub else ""
    out["domain"] = domain
    out["subdomain"] = sub_s or None
    for k in (
        "patient_name",
        "patient_age",
        "doctor_name",
        "facility_name",
        "visit_date",
        "visit_reason",
        "symptoms",
        "prescriptions",
        "clinical_detail",
    ):
        v = d.get(k)
        out[k] = (str(v).strip() if v is not None else "") or None

    vitals = d.get("vitals")
    if vitals is None:
        out["vitals_json"] = None
    elif isinstance(vitals, list):
        cleaned: List[Dict[str, Any]] = []
        for item in vitals:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            try:
                val = float(item.get("value"))
            except (TypeError, ValueError):
                continue
            unit = str(item.get("unit") or "").strip() or None
            age_y = item.get("age_years")
            age_f: Optional[float] = None
            if age_y is not None and str(age_y).strip() != "":
                try:
                    age_f = float(age_y)
                except (TypeError, ValueError):
                    age_f = None
            cleaned.append({"name": name, "value": val, "unit": unit, "age_years": age_f})
        out["vitals_json"] = json.dumps(cleaned, ensure_ascii=False) if cleaned else "[]"
    else:
        out["vitals_json"] = None

    return out


def build_health_summary_prompt(*, person_key: str, evidence_text: str) -> str:
    return f"""You are helping the user summarize THEIR OWN medical documents for personal record-keeping.
Write ONE concise paragraph summarizing the person's health history from the evidence below.

Rules:
- Use ONLY the evidence; do not invent.
- Do NOT diagnose; do not provide medical advice; do not suggest treatments.
- DO summarize what is documented (conditions/concerns, allergies, chronic issues, surgeries, long‑term meds, notable labs/vitals, follow-ups).
- If the evidence is a single visit note, summarize that visit briefly.
- If something is not present in evidence, say \"not mentioned\" (do not refuse).
- Do not ask questions; do not add disclaimers beyond the rules.
- Keep it short (<= 120 words).

Person key: {person_key}

Evidence:
---
{evidence_text}
---
"""


def generate_health_summary(
    *,
    person_key: str,
    evidence_text: str,
    provider: str = "local",
    model: Optional[str] = None,
) -> str:
    _ensure_llm_path()
    from llm_api import create_llm_client, query_llm  # type: ignore

    prompt = build_health_summary_prompt(person_key=person_key, evidence_text=evidence_text)
    client = create_llm_client(provider)
    raw = query_llm(prompt, client=client, model=model, provider=provider)
    if raw is None or not str(raw).strip():
        raise RuntimeError("本地摘要生成失败（无返回）。请检查 Ollama / LOCAL_LLM_BASE_URL 与模型。")
    return str(raw).strip()

