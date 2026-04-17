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


def _extraction_print_detail_enabled(explicit: Optional[bool]) -> bool:
    """True when caller asks, or when AI_ASSISTANT_PRINT_EXTRACTION_DETAIL is truthy (1/true/yes/on/debug)."""
    if explicit is not None:
        return bool(explicit)
    v = (os.getenv("AI_ASSISTANT_PRINT_EXTRACTION_DETAIL") or "").strip().lower()
    return v in {"1", "true", "yes", "on", "debug"}


def _detail_raw_max_chars() -> int:
    """0 = do not truncate. Default caps huge PDF outputs. Env: AI_ASSISTANT_PRINT_EXTRACTION_DETAIL_MAX_CHARS."""
    raw = (os.getenv("AI_ASSISTANT_PRINT_EXTRACTION_DETAIL_MAX_CHARS") or "").strip()
    if not raw:
        return 80000
    try:
        n = int(raw)
    except ValueError:
        return 80000
    return 0 if n < 0 else n


def _clip_detail_text(s: str) -> tuple[str, bool]:
    cap = _detail_raw_max_chars()
    if cap <= 0 or len(s) <= cap:
        return s, False
    return s[:cap], True


def _emit_extraction_detail(
    *,
    phase: str,
    person_key_hint: str,
    provider: str,
    model: Optional[str],
    detail_context: str,
    raw_response: str,
    parsed: Optional[Dict[str, Any]] = None,
    extra_line: str = "",
) -> None:
    m = (model or "").strip() or "(default)"
    ctx = detail_context.strip()
    banner = (
        f"\n=== AI.assistant extraction detail [{phase}] "
        f"person={person_key_hint!r} provider={provider!r} model={m!r}"
    )
    if ctx:
        banner += f" {ctx}"
    banner += " ==="
    print(banner, file=sys.stderr)
    if extra_line:
        print(extra_line, file=sys.stderr)
    clipped, truncated = _clip_detail_text(raw_response or "")
    suffix = " (truncated; raise AI_ASSISTANT_PRINT_EXTRACTION_DETAIL_MAX_CHARS or set 0 for full)" if truncated else ""
    print(f"[llm raw response]{suffix}", file=sys.stderr)
    print(clipped, file=sys.stderr)
    if parsed is not None:
        print(f"[parsed visit_date] {parsed.get('visit_date')!r}", file=sys.stderr)
        print("[parsed JSON]", file=sys.stderr)
        print(json.dumps(parsed, ensure_ascii=False, indent=2), file=sys.stderr)


def _query_llm_with_timeout(
    *,
    prompt: str,
    client: Any,
    model: Optional[str],
    provider: str,
    raise_on_error: bool,
) -> Optional[str]:
    """
    Enforce an application-level timeout. This protects us even when the SDK/network layer hangs.
    """
    # Import here so module can be imported without `tools/shared` on path.
    from llm_api import query_llm  # type: ignore

    timeout_s = _timeout_seconds_for_provider(provider)
    display_model: Optional[str] = model
    if not display_model:
        if provider == "local":
            display_model = (os.getenv("LOCAL_LLM_MODEL") or "").strip() or None
        else:
            display_model = None
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(lambda: query_llm(prompt, client=client, model=model, provider=provider, raise_on_error=raise_on_error))
    try:
        return fut.result(timeout=timeout_s)
    except concurrent.futures.TimeoutError as e:
        raise RuntimeError(f"LLM timeout after {timeout_s:.0f}s (provider={provider}, model={display_model!r})") from e
    finally:
        # Do NOT wait for a hung worker thread.
        ex.shutdown(wait=False, cancel_futures=True)


def extraction_env_canonical(person_key: str, base: str) -> Optional[str]:
    """
    Read optional per-person overrides, e.g. AI_ASSISTANT_EXTRACT_PATIENT_NAME_QIANYING.
    `base` is the env prefix without trailing underscore (e.g. AI_ASSISTANT_EXTRACT_PATIENT_NAME).
    """
    pk = (person_key or "").strip().upper().replace("-", "_")
    if not pk:
        return None
    raw = (os.getenv(f"{base}_{pk}") or "").strip()
    return raw or None


def resolve_canonical_identities_from_profile_and_env(
    profile: Optional[Dict[str, Any]],
    person_key: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Authoritative patient / PCP names for this person (profile wins, then env)."""
    prof = profile or {}
    p = str(prof.get("legal_name") or "").strip() or None
    d = str(prof.get("primary_doctor_name") or "").strip() or None
    if not p:
        p = extraction_env_canonical(person_key, "AI_ASSISTANT_EXTRACT_PATIENT_NAME")
    if not d:
        d = extraction_env_canonical(person_key, "AI_ASSISTANT_EXTRACT_DOCTOR_NAME")
    return p, d


def apply_canonical_identities(norm: Dict[str, Any], *, patient: Optional[str], doctor: Optional[str]) -> None:
    """When profile/env supply canonical names, prefer them for stable UI rows."""
    if patient:
        norm["patient_name"] = patient
    if doctor:
        norm["doctor_name"] = doctor


def build_extraction_prompt(
    document_text: str,
    person_key_hint: str,
    *,
    canonical_patient_name: Optional[str] = None,
    canonical_doctor_name: Optional[str] = None,
) -> str:
    canon_p = ""
    if (canonical_patient_name or "").strip():
        canon_p = (
            f"\nAuthoritative patient name for this profile (use EXACTLY for \"patient_name\" unless the excerpt "
            f"clearly documents a different patient for this encounter): \"{canonical_patient_name.strip()}\".\n"
        )
    canon_d = ""
    if (canonical_doctor_name or "").strip():
        canon_d = (
            f"\nAuthoritative primary/treating clinician for this profile when the excerpt only shows a short name "
            f'(use for \"doctor_name\" if you cannot determine a fuller name): \"{canonical_doctor_name.strip()}\".\n'
        )
    return f"""You are a clinical document information extractor. Extract fields ONLY from the text below. If unknown, use an empty string \"\". Do not diagnose. Do not invent facts not supported by the text.

The database row is associated with family member profile key \"{person_key_hint}\" (may help disambiguate patient name).
{canon_p}{canon_d}
Lines beginning with \"[\" and labeled ENCOUNTER_ROW_ANCHOR or similar META blocks are NOT part of the medical record. Never copy those instruction lines into \"visit_date_evidence\", \"symptoms\", or \"clinical_detail\".

Role disambiguation (critical for short table fragments):
- \"patient_name\" must be the PATIENT (the person receiving care), not the clinician. Do not put the doctor's name in patient_name.
- \"doctor_name\" must be the treating clinician for THIS encounter (often after Provider / Doctor / signed by), not the patient.

Diagnoses / problems:
- Include ALL diagnosis lines, problem list entries, and ICD-10 codes that appear in the excerpt for this visit — prefer completeness in \"symptoms\" and \"clinical_detail\" over a single-code summary.

The excerpt may contain MULTIPLE sections (e.g. Vitals / Vital Signs vs Encounters / Office Visit). Treat them separately:
- A date next to vital signs (measurement date) is NOT automatically the clinical encounter visit date.
- \"visit_date\" must be the date of THIS office/clinical encounter when the text clearly states it for that visit (e.g. encounter date, date of service for that visit). If you cannot attribute a date to THIS encounter, use \"\".
- Do NOT copy an earlier vitals measurement date into \"visit_date\" for a later encounter block.
- Add \"visit_date_evidence\": a SHORT verbatim quote from the SAME passage that supports \"visit_date\" (or \"\" if visit_date is empty).

Return ONLY a single JSON object with these keys:
- \"domain\": usually \"health\"
- \"subdomain\": one of: pediatrics, dermatology, primary_care, mental_health, labs, cardiology, orthopedics, ent, urgent_care, ob_gyn, dentistry, ophthalmology, other
- \"patient_name\"
- \"patient_age\" (e.g. \"35 years\", \"8 months\")
- \"doctor_name\"
- \"facility_name\" (hospital or clinic)
- \"visit_date\" (ISO YYYY-MM-DD if found in document, else \"\")
- \"visit_date_evidence\" (short verbatim quote supporting visit_date, else \"\")
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


def build_semantic_extraction_prompt(document_text: str, person_key_hint: str) -> str:
    """
    LLM only fills narrative / semantic fields. Patient, doctor, visit date, facility are NOT requested —
    the pipeline fills them from structure (row bind, demographics, profile).
    """
    return f"""You extract ONLY clinical narrative fields from the excerpt below for personal health records.
Do NOT output patient name, doctor/clinician name, visit date, facility name, or patient age — the system fills those from parsers and profile.
If a field is unknown, use an empty string \"\". Do not invent facts. Do not diagnose.

Profile context (for disambiguation only, do not echo as patient_name): family member key \"{person_key_hint}\".

Ignore META blocks such as [ENCOUNTER_ROW_ANCHOR] ... [/ENCOUNTER_ROW_ANCHOR] — do not copy instruction lines into JSON.

Return ONLY a JSON object with these keys:
- \"domain\": usually \"health\"
- \"subdomain\": one of: pediatrics, dermatology, primary_care, mental_health, labs, cardiology, orthopedics, ent, urgent_care, ob_gyn, dentistry, ophthalmology, other
- \"visit_reason\" (chief complaint / reason for visit, concise)
- \"symptoms\" (problems, diagnoses, ICD codes as they appear — prefer completeness)
- \"prescriptions\" (medications mentioned)
- \"clinical_detail\" (assessment, plan, key narrative findings — plain text)
- \"vitals\": JSON array of objects with \"name\", \"value\" (number), \"unit\", \"age_years\" (null ok); omit vitals with unknown values if needed

Document excerpt:
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
    print_detail: Optional[bool] = None,
    detail_context: str = "",
    canonical_patient_name: Optional[str] = None,
    canonical_doctor_name: Optional[str] = None,
    extraction_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """
    When print_detail is True, or env AI_ASSISTANT_PRINT_EXTRACTION_DETAIL is truthy,
    prints LLM raw string + parsed JSON (including visit_date) to stderr — same terminal as uvicorn.

    extraction_mode:
    - \"semantic\" (default via env AI_ASSISTANT_WEB_REEXTRACT_MODE): do not ask LLM for identity/date/facility.
    - \"full\": legacy prompt with all fields (LLM may fill patient/doctor/date).
    """
    _ensure_llm_path()
    from llm_api import create_llm_client  # type: ignore

    mode = (extraction_mode or os.getenv("AI_ASSISTANT_WEB_REEXTRACT_MODE") or "semantic").strip().lower()
    if mode not in {"semantic", "full"}:
        mode = "semantic"

    prov = provider or os.getenv("AI_ASSISTANT_EXTRACT_PROVIDER", "aibuilder")
    if mode == "semantic":
        prompt = build_semantic_extraction_prompt(document_text, person_key_hint)
    else:
        prompt = build_extraction_prompt(
            document_text,
            person_key_hint,
            canonical_patient_name=canonical_patient_name,
            canonical_doctor_name=canonical_doctor_name,
        )
    client = create_llm_client(prov)
    show = _extraction_print_detail_enabled(print_detail)
    raw = _query_llm_with_timeout(prompt=prompt, client=client, model=model, provider=prov, raise_on_error=True)
    if raw is None:
        raise RuntimeError("模型调用失败（无返回）。请检查本机 Ollama / LOCAL_LLM_BASE_URL 或云端 API配置。")
    try:
        parsed = _parse_json_object(raw)
        if show:
            _emit_extraction_detail(
                phase="attempt_1_ok",
                person_key_hint=person_key_hint,
                provider=prov,
                model=model,
                detail_context=detail_context,
                raw_response=str(raw),
                parsed=parsed,
            )
        return parsed
    except Exception as e:
        if show:
            _emit_extraction_detail(
                phase="attempt_1_parse_fail",
                person_key_hint=person_key_hint,
                provider=prov,
                model=model,
                detail_context=detail_context,
                raw_response=str(raw),
                parsed=None,
                extra_line=f"parse_error={e!r}",
            )
        # Retry once with a stricter instruction (local models sometimes ignore JSON-only requirement)
        retry_prompt = prompt + "\n\nIMPORTANT: Output MUST be ONLY JSON and MUST start with '{' and end with '}'."
        raw2 = _query_llm_with_timeout(prompt=retry_prompt, client=client, model=model, provider=prov, raise_on_error=True)
        if raw2 is None:
            head = (str(raw) or "")[:240].replace("\n", "\\n")
            raise RuntimeError(f"模型返回无法解析为 JSON（且重试无返回）：{e}. raw_head={head}") from e
        try:
            parsed2 = _parse_json_object(raw2)
            if show:
                _emit_extraction_detail(
                    phase="attempt_2_ok",
                    person_key_hint=person_key_hint,
                    provider=prov,
                    model=model,
                    detail_context=detail_context,
                    raw_response=str(raw2),
                    parsed=parsed2,
                )
            return parsed2
        except Exception as e2:
            if show:
                _emit_extraction_detail(
                    phase="attempt_2_parse_fail",
                    person_key_hint=person_key_hint,
                    provider=prov,
                    model=model,
                    detail_context=detail_context,
                    raw_response=str(raw2),
                    parsed=None,
                    extra_line=f"parse_error={e2!r}",
                )
            head1 = (str(raw) or "")[:240].replace("\n", "\\n")
            head2 = (str(raw2) or "")[:240].replace("\n", "\\n")
            raise RuntimeError(f"模型返回无法解析为 JSON：{e2}. raw_head={head2}. first_raw_head={head1}") from e2


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


def normalize_semantic_extraction(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Same as normalize_extraction for allowed keys, then clears identity / date / facility so the pipeline
    (encounter bind, demographics, profile) owns those fields — LLM output cannot dominate them.
    """
    out = normalize_extraction(d)
    for k in ("patient_name", "patient_age", "doctor_name", "facility_name", "visit_date"):
        out[k] = None
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
    raw = _query_llm_with_timeout(prompt=prompt, client=client, model=model, provider=provider, raise_on_error=True)
    if raw is None or not str(raw).strip():
        raise RuntimeError("本地摘要生成失败（无返回）。请检查 Ollama / LOCAL_LLM_BASE_URL 与模型。")
    return str(raw).strip()
