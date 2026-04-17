from __future__ import annotations

import json
import os
import sys
import traceback
import threading
from uuid import uuid4
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.ai_assistant import db
from src.ai_assistant.extraction_profiles import reset_catalog_cache
from src.ai_assistant.health_extract import (
    apply_canonical_identities,
    extract_health_metadata,
    generate_health_summary,
    normalize_extraction,
    normalize_semantic_extraction,
    resolve_canonical_identities_from_profile_and_env,
)
from src.ai_assistant.paths import get_paths_for_person
from src.ai_assistant.subjects import known_person_keys, normalize_person_key
from src.ai_assistant.travel_planner import TravelPlannerInputs, build_travel_context, bundle_debug_text, run_travel_planner
from src.ai_assistant.vitals_helpers import chart_series_from_records


APP_ROOT = Path(__file__).resolve().parent
TEMPLATES_DIR = APP_ROOT / "templates"
EXTRACTION_CATALOG_PATH = APP_ROOT / "config" / "extraction_profiles.json"

app = FastAPI()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app.mount("/static", StaticFiles(directory=str(APP_ROOT / "static"), check_dir=False), name="static")


_WEB_REEXTRACT_PROVIDER = os.getenv("AI_ASSISTANT_WEB_REEXTRACT_PROVIDER", "local")
_WEB_SUMMARY_PROVIDER = os.getenv("AI_ASSISTANT_WEB_SUMMARY_PROVIDER", "local")


def _web_reextract_mode() -> str:
    """Default semantic: LLM does not fill patient/doctor/date/facility. Use \"full\" for legacy all-field prompt."""
    m = (os.getenv("AI_ASSISTANT_WEB_REEXTRACT_MODE") or "semantic").strip().lower()
    return m if m in {"semantic", "full"} else "semantic"


def _current_web_model(env_key: str) -> Optional[str]:
    v = (os.getenv(env_key) or "").strip()
    if v:
        return v
    v2 = (os.getenv("LOCAL_LLM_MODEL") or "").strip()
    return v2 or None

_JOBS_LOCK = threading.Lock()
_JOBS: Dict[str, Dict[str, Any]] = {}


def _job_set(job_id: str, patch: Dict[str, Any]) -> None:
    with _JOBS_LOCK:
        base = _JOBS.get(job_id, {})
        base.update(patch)
        _JOBS[job_id] = base


def _job_get(job_id: str) -> Dict[str, Any]:
    with _JOBS_LOCK:
        return dict(_JOBS.get(job_id, {}))


def _default_access_controls() -> Dict[str, Any]:
    """
    Access controls are stored per-person in profiles.profile_json["access_controls"].
    Shape is intentionally simple and forward-compatible (nested dict of booleans).
    """
    return {
        "health": {
            "enabled": True,
            "records": True,
            "profile": True,
            "memories": True,
            "vitals": True,
        },
        "travel": {
            "enabled": True,
            "planner": True,
            "preferences": True,
            "memories": True,
            "include_health_constraints": True,
        },
        # Placeholders for future modules (can be wired later)
        "module3": {"enabled": True},
        "module4": {"enabled": True},
        "module5": {"enabled": True},
        "module6": {"enabled": True},
    }


def _merge_access_controls(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            merged = dict(out[k])
            merged.update(v)
            out[k] = merged
        else:
            out[k] = v
    return out


def _get_access_controls(db_path: Path, *, person_key: str) -> Dict[str, Any]:
    db.init_db(db_path)
    with db.connect(db_path) as conn:
        prof = db.get_profile(conn, person_key=person_key)
    raw = prof.get("access_controls") if isinstance(prof, dict) else None
    if isinstance(raw, dict):
        return _merge_access_controls(_default_access_controls(), raw)
    return _default_access_controls()


def _require_access(access: Dict[str, Any], module: str, feature: Optional[str] = None) -> None:
    m = access.get(module) if isinstance(access, dict) else None
    enabled = bool(m.get("enabled")) if isinstance(m, dict) else False
    if not enabled:
        raise HTTPException(status_code=403, detail=f"Module disabled: {module}")
    if feature:
        if not bool(m.get(feature)):
            raise HTTPException(status_code=403, detail=f"Feature disabled: {module}.{feature}")


def _person_db_path(person: str) -> Path:
    p = get_paths_for_person(person)
    db.maybe_migrate_legacy_sqlite(person=p.person_key, db_path=p.db_path)
    db.init_db(p.db_path)
    # One-time migration: .env birthdate -> profiles.birthdate, so visit_age can be computed.
    with db.connect(p.db_path) as conn:
        prof = db.get_profile(conn, person_key=p.person_key)
        if not str(prof.get("birthdate") or "").strip():
            env_key = f"AI_ASSISTANT_BIRTHDATE_{p.person_key.upper()}"
            bday = os.getenv(env_key, "").strip()
            if bday:
                db.upsert_profile_fields(conn, person_key=p.person_key, patch={"birthdate": bday})
                conn.commit()
    return p.db_path


def _flash_from_query(request: Request) -> Dict[str, str]:
    q = dict(request.query_params)
    return {k: str(v) for k, v in q.items() if k in {"sync", "reextract", "importnew", "saved", "job", "job_person"}}


def _read_extraction_catalog() -> Dict[str, Any]:
    if not EXTRACTION_CATALOG_PATH.is_file():
        raise FileNotFoundError(str(EXTRACTION_CATALOG_PATH))
    return json.loads(EXTRACTION_CATALOG_PATH.read_text(encoding="utf-8"))


def _write_extraction_catalog(catalog: Dict[str, Any]) -> None:
    EXTRACTION_CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = EXTRACTION_CATALOG_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(catalog, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(EXTRACTION_CATALOG_PATH)
    reset_catalog_cache()


def _with_query(url: str, params: Dict[str, str]) -> str:
    """
    Safely append/override query params without producing `...?a=1?b=2`.
    Keeps existing path and existing query parameters.
    """
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q.update({k: v for k, v in params.items() if v is not None})
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q, doseq=True), p.fragment))


def _fetch_records(*, db_path: Path, person_key: str, domain: Optional[str]) -> List[Dict[str, Any]]:
    db.init_db(db_path)
    sql = """
    SELECT
      record_id, person_key, domain, subdomain, record_kind, record_type,
      layer, source_system,
      observed_at, imported_at,
      original_path, stored_path, sensitivity_tier,
      patient_name, patient_age, doctor_name, facility_name,
      visit_date_extracted, visit_reason, symptoms, prescriptions, clinical_detail,
      extracted_at, extraction_error,
      vitals_json, visit_age_years, visit_age_text
    FROM records
    WHERE person_key = ?
    """
    params: List[Any] = [person_key]
    if domain and domain != "all":
        sql += " AND domain = ?"
        params.append(domain)
    sql += " ORDER BY COALESCE(observed_at, imported_at) DESC"
    with db.connect(db_path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({k: r[k] for k in r.keys()})
    return out


def _distinct_domains(*, db_path: Path, person_key: str) -> List[str]:
    try:
        return db.list_distinct_domains(db_path=db_path, person_key=person_key)
    except Exception:
        return []


def _re_extract_one(*, db_path: Path, record_id: str, person_key: str) -> Optional[str]:
    # Fetch text first, then close DB while calling LLM (avoid long-held sqlite locks).
    mode = _web_reextract_mode()
    max_chars = int(os.getenv("AI_ASSISTANT_WEB_REEXTRACT_MAX_CHARS", "6000") or "6000")
    demo: Dict[str, Any] = {}
    with db.connect(db_path) as conn:
        text, bound_visit_iso, row_det = db.get_record_text_for_extraction(
            conn,
            record_id,
            max_chars=max_chars,
            include_encounter_row_anchor=(mode != "semantic"),
        )
        profile = db.get_profile(conn, person_key=person_key)
        sp_row = conn.execute("SELECT stored_path FROM records WHERE record_id = ?", (record_id,)).fetchone()
    if sp_row and sp_row["stored_path"]:
        sp = Path(str(sp_row["stored_path"]))
        if sp.suffix.lower() == ".pdf" and sp.is_file():
            try:
                npg = int(os.getenv("AI_ASSISTANT_DEMOGRAPHICS_MAX_PAGES", "3") or "3")
                preview = db.read_pdf_text_first_pages(sp, max_pages=max(1, npg))
                demo = db.parse_demographics_from_text(preview)
                if demo:
                    print(
                        f"[pdf_demographics] record_id={record_id} patient={demo.get('patient_name')!r} "
                        f"pcp={demo.get('pcp_name')!r}",
                        file=sys.stderr,
                    )
            except Exception:
                demo = {}
    canon_patient, canon_doctor = resolve_canonical_identities_from_profile_and_env(profile, person_key)
    if not text.strip():
        with db.connect(db_path) as conn:
            db.set_extraction_error(conn, record_id, "No text extracted from PDF/chunks")
            conn.commit()
        return "无可抽取文本"
    try:
        raw = extract_health_metadata(
            text,
            person_key_hint=person_key,
            provider=_WEB_REEXTRACT_PROVIDER,
            model=_current_web_model("AI_ASSISTANT_WEB_REEXTRACT_MODEL"),
            detail_context=f"record_id={record_id}",
            canonical_patient_name=canon_patient if mode == "full" else None,
            canonical_doctor_name=canon_doctor if mode == "full" else None,
            extraction_mode=mode,
        )
        if mode == "semantic":
            norm = normalize_semantic_extraction(raw)
        else:
            norm = normalize_extraction(raw)
        if bound_visit_iso and not str(norm.get("visit_date") or "").strip():
            norm["visit_date"] = bound_visit_iso
        db.merge_deterministic_encounter_into_normalized(norm, row_det)
        db.merge_demographics_into_normalized(norm, demo)
        apply_canonical_identities(norm, patient=canon_patient, doctor=canon_doctor)
        with db.connect(db_path) as conn:
            db.update_record_from_extraction(conn, record_id, norm)
            conn.commit()
        return None
    except Exception as e:  # noqa: BLE001
        msg = str(e).strip() or "unknown error"
        # Add targeted hints for common setup issues
        hint = ""
        if "401" in msg or "invalid_api_key" in msg:
            hint = "（401：请检查 .env 的 AI_BUILDER_TOKEN / OPENAI_API_KEY，并重启 uvicorn）"
        if "Connection refused" in msg or "ECONNREFUSED" in msg or "127.0.0.1:11434" in msg:
            hint = "（本地推理连不上：请确认 Ollama 正在运行，或检查 LOCAL_LLM_BASE_URL）"
        with db.connect(db_path) as conn:
            db.set_extraction_error(conn, record_id, msg + hint)
            conn.commit()
        return msg


def _run_reextract_all_job(*, job_id: str, db_path: Path, person_key: str, record_ids: List[str]) -> None:
    _job_set(job_id, {"state": "running", "total": len(record_ids), "ok": 0, "fail": 0})
    ok = 0
    fail = 0
    first_err = ""
    for rid in record_ids:
        _job_set(job_id, {"current_record_id": rid})
        try:
            err = _re_extract_one(db_path=db_path, record_id=rid, person_key=person_key)
        except Exception as e:  # noqa: BLE001
            err = str(e).strip() or "unknown error"
        if err:
            fail += 1
            if not first_err:
                first_err = err
        else:
            ok += 1
        _job_set(job_id, {"ok": ok, "fail": fail, "first_err": first_err, "current_record_id": rid})
    _job_set(job_id, {"state": "done"})


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    people: List[Dict[str, Any]] = []
    for k in known_person_keys():
        person_key = normalize_person_key(k)
        db_path = _person_db_path(person_key)
        access = _get_access_controls(db_path, person_key=person_key)
        people.append({"key": person_key, "access": access})
    catalog = _read_extraction_catalog()
    defaults = catalog.get("defaults") if isinstance(catalog, dict) else {}
    defaults_lines: List[str] = []
    if isinstance(defaults, dict):
        # Flatten domain → source_kind → family → intent → profile_id into readable one-liners.
        for domain, d1 in defaults.items():
            if not isinstance(d1, dict):
                continue
            for source_kind, d2 in d1.items():
                if not isinstance(d2, dict):
                    continue
                for family, d3 in d2.items():
                    if not isinstance(d3, dict):
                        continue
                    for intent, pid in d3.items():
                        if not isinstance(pid, str):
                            continue
                        defaults_lines.append(f"{domain}/{source_kind}/{family}/{intent} → {pid}")
    defaults_lines.sort()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "people": people,
            "extraction_defaults": defaults if isinstance(defaults, dict) else {},
            "extraction_defaults_lines": defaults_lines,
            "extraction_catalog_path": str(EXTRACTION_CATALOG_PATH),
        },
    )


@app.get("/admin/extraction-profiles", response_class=HTMLResponse)
def admin_extraction_profiles(request: Request) -> HTMLResponse:
    catalog = _read_extraction_catalog()
    profiles = catalog.get("profiles") or {}
    rows: List[Dict[str, Any]] = []
    for pid, p in profiles.items():
        if not isinstance(p, dict):
            continue
        rows.append(
            {
                "id": str(pid),
                "label": str(p.get("label") or ""),
                "domain": str(p.get("domain") or ""),
                "source_kind": str(p.get("source_kind") or ""),
                "document_family": str(p.get("document_family") or ""),
                "extraction_intent": str(p.get("extraction_intent") or ""),
            }
        )
    rows.sort(key=lambda r: (r["domain"], r["source_kind"], r["document_family"], r["id"]))
    flash = _flash_from_query(request)
    return templates.TemplateResponse(
        "admin_extraction_profiles.html",
        {"request": request, "rows": rows, "flash": flash, "catalog_path": str(EXTRACTION_CATALOG_PATH)},
    )


@app.post("/admin/extraction-profiles/save")
async def admin_extraction_profiles_save(request: Request, next: str = Form("/admin/extraction-profiles")) -> RedirectResponse:  # noqa: A002
    form = dict(await request.form())
    catalog = _read_extraction_catalog()
    profiles = catalog.get("profiles")
    if not isinstance(profiles, dict):
        raise HTTPException(status_code=500, detail="Invalid extraction_profiles.json: missing profiles")

    changed = 0
    for k, v in form.items():
        ks = str(k)
        if not ks.startswith("intent__"):
            continue
        pid = ks[len("intent__") :]
        if pid not in profiles or not isinstance(profiles.get(pid), dict):
            continue
        new_intent = str(v or "").strip()
        if profiles[pid].get("extraction_intent") != new_intent:
            profiles[pid]["extraction_intent"] = new_intent
            changed += 1

    catalog["profiles"] = profiles
    _write_extraction_catalog(catalog)
    msg = f"已保存：更新 {changed} 条 profile intent" if changed else "已保存：无变更"
    return RedirectResponse(url=_with_query(next, {"saved": msg}), status_code=303)


@app.post("/person/{person}/access-controls")
def update_access_controls(
    person: str,
    next: str = Form("/"),  # noqa: A002
    # Health
    health_enabled: str = Form("0"),
    health_records: str = Form("0"),
    health_profile: str = Form("0"),
    health_memories: str = Form("0"),
    health_vitals: str = Form("0"),
    # Travel
    travel_enabled: str = Form("0"),
    travel_planner: str = Form("0"),
    travel_preferences: str = Form("0"),
    travel_memories: str = Form("0"),
    travel_include_health_constraints: str = Form("0"),
    # Future modules (enable only)
    module3_enabled: str = Form("0"),
    module4_enabled: str = Form("0"),
    module5_enabled: str = Form("0"),
    module6_enabled: str = Form("0"),
) -> RedirectResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    patch = {
        "health": {
            "enabled": health_enabled == "1",
            "records": health_records == "1",
            "profile": health_profile == "1",
            "memories": health_memories == "1",
            "vitals": health_vitals == "1",
        },
        "travel": {
            "enabled": travel_enabled == "1",
            "planner": travel_planner == "1",
            "preferences": travel_preferences == "1",
            "memories": travel_memories == "1",
            "include_health_constraints": travel_include_health_constraints == "1",
        },
        "module3": {"enabled": module3_enabled == "1"},
        "module4": {"enabled": module4_enabled == "1"},
        "module5": {"enabled": module5_enabled == "1"},
        "module6": {"enabled": module6_enabled == "1"},
    }
    with db.connect(db_path) as conn:
        prof = db.get_profile(conn, person_key=person_key)
        current = prof.get("access_controls") if isinstance(prof, dict) else None
        merged = _merge_access_controls(_default_access_controls(), current if isinstance(current, dict) else {})
        merged = _merge_access_controls(merged, patch)
        db.upsert_profile_fields(conn, person_key=person_key, patch={"access_controls": merged})
        conn.commit()
    return RedirectResponse(url=_with_query(next, {"saved": f"{person_key} Access level 已保存"}), status_code=303)


@app.get("/person/{person}")
def person_home(person: str) -> RedirectResponse:
    """
    Backward-compatible entrypoint.
    Older versions used `/person/<person>` as the landing page.
    """
    person_key = normalize_person_key(person)
    return RedirectResponse(url=f"/person/{person_key}/records?domain=all", status_code=303)


@app.get("/person/{person}/records", response_class=HTMLResponse)
def person_records(request: Request, person: str, domain: str = "all") -> HTMLResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    records = _fetch_records(db_path=db_path, person_key=person_key, domain=domain)
    domains = ["all"] + _distinct_domains(db_path=db_path, person_key=person_key)
    flash = _flash_from_query(request)
    return templates.TemplateResponse(
        "records.html",
        {
            "request": request,
            "person": person_key,
            "records": records,
            "domains": domains,
            "domain_filter": domain,
            "flash": flash,
        },
    )


@app.get("/person/{person}/record/{record_id}", response_class=HTMLResponse)
def record_detail(request: Request, person: str, record_id: str) -> HTMLResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    with db.connect(db_path) as conn:
        row = conn.execute("SELECT * FROM records WHERE person_key = ? AND record_id = ?", (person_key, record_id)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="record not found")
        chunks = conn.execute(
            "SELECT chunk_index, text FROM chunks WHERE record_id = ? ORDER BY chunk_index",
            (record_id,),
        ).fetchall()
    rec = {k: row[k] for k in row.keys()}
    return templates.TemplateResponse(
        "record_detail.html",
        {"request": request, "person": person_key, "record": rec, "chunks": chunks},
    )


@app.get("/person/{person}/record/{record_id}/file")
def record_file(person: str, record_id: str) -> FileResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    with db.connect(db_path) as conn:
        row = conn.execute(
            "SELECT stored_path FROM records WHERE person_key = ? AND record_id = ?",
            (person_key, record_id),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="record not found")
    p = Path(str(row["stored_path"]))
    if not p.is_file():
        raise HTTPException(status_code=404, detail="file missing on disk")
    return FileResponse(path=str(p), filename=p.name, media_type="application/pdf")


@app.post("/person/{person}/actions/sync-files")
def action_sync_files(person: str, next: str = Form("/")) -> RedirectResponse:  # noqa: A002
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    removed = db.delete_orphan_records(db_path)
    return RedirectResponse(url=_with_query(next, {"sync": f"同步完成：删除 {removed} 条孤儿记录"}), status_code=303)


@app.post("/person/{person}/actions/reextract-all")
def action_reextract_all(
    background_tasks: BackgroundTasks,
    person: str,
    domain: str = Form("all"),
    next: str = Form("/"),  # noqa: A002
) -> RedirectResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    ids = db.list_record_ids_for_person(db_path, person_key, domain=domain)
    job_id = uuid4().hex[:16]
    _job_set(job_id, {"state": "queued", "person": person_key, "domain": domain})
    background_tasks.add_task(_run_reextract_all_job, job_id=job_id, db_path=db_path, person_key=person_key, record_ids=ids)
    msg = (
        f"已在后台启动本地重新提取（job={job_id}，共 {len(ids)} 条）。"
        f" 可访问 /person/{person_key}/jobs/{job_id} 查看进度。"
    )
    return RedirectResponse(url=_with_query(next, {"reextract": msg, "job": job_id, "job_person": person_key}), status_code=303)


@app.get("/person/{person}/jobs/{job_id}")
def job_status(person: str, job_id: str) -> Dict[str, Any]:
    person_key = normalize_person_key(person)
    j = _job_get(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="job not found")
    if j.get("person") and j.get("person") != person_key:
        raise HTTPException(status_code=404, detail="job not found")
    return j


@app.get("/person/{person}/person/{person2}/jobs/{job_id}")
def job_status_compat(person: str, person2: str, job_id: str) -> RedirectResponse:
    # Compat: some browsers/users may open a relative URL and end up with duplicated prefix.
    p1 = normalize_person_key(person)
    p2 = normalize_person_key(person2)
    if p1 != p2:
        raise HTTPException(status_code=404, detail="job not found")
    return RedirectResponse(url=f"/person/{p1}/jobs/{job_id}", status_code=307)


@app.post("/person/{person}/actions/reextract-one")
def action_reextract_one(person: str, record_id: str = Form(...), next: str = Form("/")) -> RedirectResponse:  # noqa: A002
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    err = _re_extract_one(db_path=db_path, record_id=record_id, person_key=person_key)
    msg = "本地重新提取本条成功" if not err else f"本地重新提取本条失败：{err}"
    return RedirectResponse(url=_with_query(next, {"reextract": msg}), status_code=303)


@app.post("/person/{person}/actions/import-new-files")
def action_import_new_files(person: str, next: str = Form("/")) -> RedirectResponse:  # noqa: A002
    person_key = normalize_person_key(person)
    paths = get_paths_for_person(person_key)
    db_path = _person_db_path(person_key)
    res = db.scan_import_new_pdfs(person=person_key, db_path=db_path, raw_root=paths.raw_dir)
    msg = f"本地提取新文件：导入 {res.imported} 个，跳过 {res.skipped_already_in_db} 个。"
    if res.errors:
        msg += f" 错误 {len(res.errors)} 个（见控制台/稍后可加详情页）：{res.errors[0]}"
    return RedirectResponse(url=_with_query(next, {"importnew": msg}), status_code=303)


@app.get("/person/{person}/vitals", response_class=HTMLResponse)
def vitals_page(request: Request, person: str) -> HTMLResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    with db.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT record_id, vitals_json, visit_age_years, visit_age_text
              FROM records
             WHERE person_key = ? AND domain = 'health'
             ORDER BY COALESCE(observed_at, imported_at) DESC
            """,
            (person_key,),
        ).fetchall()
    series, has_any = chart_series_from_records(rows)
    return templates.TemplateResponse(
        "vitals_charts.html",
        {
            "request": request,
            "person": person_key,
            "has_any": has_any,
            "vitals_series_json": json.dumps(series, ensure_ascii=False),
        },
    )


@app.get("/person/{person}/profile", response_class=HTMLResponse)
def person_profile(request: Request, person: str) -> HTMLResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    with db.connect(db_path) as conn:
        profile = db.get_profile(conn, person_key=person_key)
    flash = _flash_from_query(request)
    return templates.TemplateResponse(
        "profile.html",
        {"request": request, "person": person_key, "profile": profile, "flash": flash},
    )


_ALLOWED_PROFILE_FIELDS = {
    "birthdate",
    "legal_name",
    "primary_doctor_name",
    "location",
    "health_summary",
    "hotel_preference",
    "airline_preference",
    "travel_place_preference",
    "things_to_do_preference",
}


@app.post("/person/{person}/profile/field")
def profile_update_field(
    person: str,
    field: str = Form(...),
    value: str = Form(""),
    next: str = Form("/"),  # noqa: A002
) -> RedirectResponse:
    person_key = normalize_person_key(person)
    if field not in _ALLOWED_PROFILE_FIELDS:
        raise HTTPException(status_code=400, detail="Unsupported profile field")
    db_path = _person_db_path(person_key)
    with db.connect(db_path) as conn:
        db.upsert_profile_fields(conn, person_key=person_key, patch={field: value})
        conn.commit()
    return RedirectResponse(url=_with_query(next, {"saved": "已保存"}), status_code=303)


def _health_evidence_for_summary(*, db_path: Path, person_key: str, max_chars: int = 12_000) -> str:
    with db.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT record_id
              FROM records
             WHERE person_key = ? AND domain = 'health'
             ORDER BY COALESCE(observed_at, imported_at) DESC
             LIMIT 30
            """,
            (person_key,),
        ).fetchall()
        parts: List[str] = []
        for r in rows:
            rid = str(r["record_id"])
            text, _, _ = db.get_record_text_for_extraction(
                conn, rid, max_chars=3000, include_encounter_row_anchor=False
            )
            if text.strip():
                parts.append(f"[record {rid}]\n{text}\n")
        evidence = "\n\n".join(parts).strip()
    if len(evidence) > max_chars:
        evidence = evidence[:max_chars] + "\n\n[truncated]"
    return evidence


@app.post("/person/{person}/profile/health-summary/generate")
def profile_generate_health_summary(person: str) -> Dict[str, Any]:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    evidence = _health_evidence_for_summary(db_path=db_path, person_key=person_key)
    if not evidence.strip():
        return {"summary": "", "error": "没有找到可用于摘要的健康记录文本"}
    try:
        summary = generate_health_summary(
            person_key=person_key,
            evidence_text=evidence,
            provider=_WEB_SUMMARY_PROVIDER,
            model=_current_web_model("AI_ASSISTANT_WEB_SUMMARY_MODEL"),
        )
        return {"summary": summary, "error": ""}
    except Exception as e:  # noqa: BLE001
        return {"summary": "", "error": str(e)}


@app.get("/person/{person}/memories", response_class=HTMLResponse)
def person_memories(request: Request, person: str) -> HTMLResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    with db.connect(db_path) as conn:
        mem_rows = db.list_memories(conn, person_key=person_key)
        profile = db.get_profile(conn, person_key=person_key)
    memories = [{k: r[k] for k in r.keys()} for r in mem_rows]
    flash = _flash_from_query(request)
    return templates.TemplateResponse(
        "memories.html",
        {"request": request, "person": person_key, "memories": memories, "profile": profile, "flash": flash},
    )


@app.post("/person/{person}/memories/{memory_id}/status")
def memory_set_status(person: str, memory_id: str, status: str = Form(...), next: str = Form("/")) -> RedirectResponse:  # noqa: A002
    person_key = normalize_person_key(person)
    if status not in {"tentative", "active", "outdated", "archived"}:
        raise HTTPException(status_code=400, detail="Unsupported memory status")
    db_path = _person_db_path(person_key)
    with db.connect(db_path) as conn:
        db.set_memory_status(conn, person_key=person_key, memory_id=memory_id, status=status)
        conn.commit()
    return RedirectResponse(url=_with_query(next, {"saved": "已更新"}), status_code=303)


@app.get("/person/{person}/travel-planner", response_class=HTMLResponse)
def travel_planner_get(request: Request, person: str) -> HTMLResponse:
    person_key = normalize_person_key(person)
    return templates.TemplateResponse(
        "travel_planner.html",
        {"request": request, "person": person_key, "result": "", "bundle": "", "bundle_chars": 0},
    )


@app.post("/person/{person}/travel-planner", response_class=HTMLResponse)
def travel_planner_post(
    request: Request,
    person: str,
    destination: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    travelers: str = Form(""),
    budget: str = Form(""),
    preferences: str = Form(""),
    include_health_constraints: str = Form("0"),
    debug: str = Form("0"),
) -> HTMLResponse:
    person_key = normalize_person_key(person)
    db_path = _person_db_path(person_key)
    with db.connect(db_path) as conn:
        profile = db.get_profile(conn, person_key=person_key)
        mem_rows = db.list_memories(conn, person_key=person_key)
    travel_memories = [
        {k: r[k] for k in r.keys()}
        for r in mem_rows
        if (r["domain"] in (None, "", "travel")) and (r["status"] in ("active", "tentative"))
    ]

    health_constraints: Optional[str] = None
    if include_health_constraints == "1":
        hs = str(profile.get("health_summary") or "").strip()
        health_constraints = hs or "(no health_summary set)"

    inputs = TravelPlannerInputs(
        destination=destination,
        start_date=start_date,
        end_date=end_date,
        travelers=travelers,
        budget=budget,
        preferences=preferences,
        include_health_constraints=(include_health_constraints == "1"),
    )
    bundle = build_travel_context(profile=profile, travel_memories=travel_memories, health_constraints=health_constraints)
    try:
        result = run_travel_planner(inputs=inputs, bundle=bundle, provider=os.getenv("AI_ASSISTANT_TRAVEL_PROVIDER", "local"))
        bundle_text, bundle_chars = bundle_debug_text(bundle) if debug == "1" else ("", 0)
        return templates.TemplateResponse(
            "travel_planner.html",
            {
                "request": request,
                "person": person_key,
                "result": result,
                "bundle": bundle_text,
                "bundle_chars": bundle_chars,
                "inputs": inputs,
                "debug": debug,
            },
        )
    except Exception as e:  # noqa: BLE001
        tb = traceback.format_exc(limit=8)
        raise HTTPException(status_code=500, detail=f"{e}\n\n{tb}")
