from __future__ import annotations

import hashlib
import os
import json
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from .paths import legacy_db_path
from .subjects import normalize_person_key


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


DDL_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS records (
  record_id TEXT PRIMARY KEY,
  person_key TEXT NOT NULL,
  domain TEXT NOT NULL,
  subdomain TEXT,
  record_kind TEXT NOT NULL,
  record_type TEXT NOT NULL,
  layer TEXT NOT NULL DEFAULT 'raw',
  source_system TEXT,
  observed_at TEXT,
  imported_at TEXT NOT NULL,
  source TEXT NOT NULL,
  original_path TEXT NOT NULL,
  stored_path TEXT NOT NULL,
  content_hash_sha256 TEXT NOT NULL,
  sensitivity_tier TEXT NOT NULL,
  notes TEXT,

  patient_name TEXT,
  patient_age TEXT,
  doctor_name TEXT,
  facility_name TEXT,
  visit_date_extracted TEXT,
  visit_reason TEXT,
  symptoms TEXT,
  prescriptions TEXT,
  clinical_detail TEXT,
  extracted_at TEXT,
  extraction_error TEXT,

  vitals_json TEXT,
  visit_age_years REAL,
  visit_age_text TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
  chunk_id TEXT PRIMARY KEY,
  record_id TEXT NOT NULL REFERENCES records(record_id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL,
  text TEXT NOT NULL,
  location TEXT,
  sensitivity_tier TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS profiles (
  person_key TEXT PRIMARY KEY,
  profile_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memories (
  memory_id TEXT PRIMARY KEY,
  person_key TEXT NOT NULL,
  memory_type TEXT NOT NULL,
  content TEXT NOT NULL,
  confidence TEXT NOT NULL DEFAULT 'medium',
  status TEXT NOT NULL DEFAULT 'tentative',
  domain TEXT,
  sensitivity_tier TEXT,
  source_record_id TEXT,
  source_chunk_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_records_person_domain_observed_at
  ON records(person_key, domain, observed_at);

CREATE INDEX IF NOT EXISTS idx_records_person_domain_subdomain
  ON records(person_key, domain, subdomain);

CREATE INDEX IF NOT EXISTS idx_records_hash
  ON records(content_hash_sha256);

CREATE INDEX IF NOT EXISTS idx_chunks_record
  ON chunks(record_id, chunk_index);

CREATE INDEX IF NOT EXISTS idx_memories_person_domain_status
  ON memories(person_key, domain, status);
"""


@dataclass(frozen=True)
class ImportResult:
    record_id: str
    stored_path: Path
    num_chars: int
    num_chunks: int
    content_hash_sha256: str


@dataclass(frozen=True)
class ScanImportResult:
    imported: int
    skipped_already_in_db: int
    errors: List[str]


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # Ensure ON DELETE CASCADE works for chunks when deleting records.
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r["name"]) for r in rows}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


_EXTRACTION_COLUMN_DDL: List[Tuple[str, str]] = [
    ("patient_name", "TEXT"),
    ("patient_age", "TEXT"),
    ("doctor_name", "TEXT"),
    ("facility_name", "TEXT"),
    ("visit_date_extracted", "TEXT"),
    ("visit_reason", "TEXT"),
    ("symptoms", "TEXT"),
    ("prescriptions", "TEXT"),
    ("clinical_detail", "TEXT"),
    ("extracted_at", "TEXT"),
    ("extraction_error", "TEXT"),
    ("vitals_json", "TEXT"),
    ("visit_age_years", "REAL"),
    ("visit_age_text", "TEXT"),
]


def _ensure_extraction_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "records"):
        return
    cols = _table_columns(conn, "records")
    for name, typ in _EXTRACTION_COLUMN_DDL:
        if name not in cols:
            conn.execute(f"ALTER TABLE records ADD COLUMN {name} {typ}")


def _infer_default_person_key(db_path: Path) -> str:
    name = db_path.name
    if name.endswith(".sqlite3"):
        stem = name[: -len(".sqlite3")]
        if stem in {"qianying", "evelyn", "lucas"}:
            return stem
    if name == "assistant.sqlite3":
        return "qianying"
    return "unknown"


def _birthdate_env_key(person_key: str) -> str:
    return f"AI_ASSISTANT_BIRTHDATE_{person_key.upper()}"


def _migrate_birthdate_from_env_into_profile(conn: sqlite3.Connection, *, person_key: str) -> None:
    """
    One-time migration: move birthdate from .env into profiles.profile_json.birthdate.
    Keep env as optional fallback during transition, but prefer DB afterwards.
    """
    prof = get_profile(conn, person_key=person_key)
    if str(prof.get("birthdate") or "").strip():
        return
    bday = (os.getenv(_birthdate_env_key(person_key)) or "").strip()
    if not bday:
        return
    upsert_profile_fields(conn, person_key=person_key, patch={"birthdate": bday})


def migrate_schema(conn: sqlite3.Connection, db_path: Path) -> None:
    conn.executescript(DDL_SQL)
    _ensure_extraction_columns(conn)
    conn.executescript(INDEX_SQL)
    person_key = _infer_default_person_key(db_path)
    if person_key in {"qianying", "evelyn", "lucas"}:
        _migrate_birthdate_from_env_into_profile(conn, person_key=person_key)
    conn.commit()


def init_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        migrate_schema(conn, db_path=db_path)


def maybe_migrate_legacy_sqlite(*, person: str, db_path: Path) -> bool:
    person_key = normalize_person_key(person)
    if person_key != "qianying":
        return False
    old = legacy_db_path()
    if not old.exists() or db_path.exists():
        return False
    db_path.parent.mkdir(parents=True, exist_ok=True)
    old.rename(db_path)
    return True


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def stable_record_id(*, person_key: str, domain: str, record_kind: str, content_hash_sha256: str) -> str:
    base = f"{person_key}:{domain}:{record_kind}:{content_hash_sha256}".encode("utf-8")
    return hashlib.sha256(base).hexdigest()[:24]


def read_pdf_text(pdf_path: Path) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Missing dependency: pypdf. Please install it in the venv.") from e

    reader = PdfReader(str(pdf_path))
    texts: List[str] = []
    for i, page in enumerate(reader.pages):
        try:
            page_text = page.extract_text() or ""
        except Exception:
            page_text = ""
        if page_text.strip():
            texts.append(f"[page {i + 1}]\n{page_text.strip()}\n")
    return "\n".join(texts).strip()


def chunk_text(text: str, *, chunk_size: int = 1200, overlap: int = 150) -> List[str]:
    text = " ".join((text or "").split())
    if not text:
        return []
    if overlap >= chunk_size:
        overlap = max(0, chunk_size // 8)
    chunks: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(n, start + chunk_size)
        chunks.append(text[start:end])
        if end == n:
            break
        start = max(0, end - overlap)
    return chunks


def ensure_raw_destination(
    raw_root: Path,
    *,
    domain: str,
    record_kind: str,
    content_hash_sha256: str,
    src_path: Path,
) -> Path:
    ext = src_path.suffix.lower() or ".bin"
    dest_dir = raw_root / domain / record_kind
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{content_hash_sha256[:16]}{ext}"
    return dest_dir / filename


def import_health_pdf(
    *,
    person: str,
    db_path: Path,
    raw_root: Path,
    pdf_path: Path,
    domain: str,
    subdomain: Optional[str],
    record_kind: Optional[str],
    record_type: Optional[str],
    layer: str,
    source_system: Optional[str],
    observed_at: Optional[str],
    sensitivity_tier: str,
    notes: Optional[str],
    chunk_size: int,
    chunk_overlap: int,
) -> ImportResult:
    if not pdf_path.exists():
        raise FileNotFoundError(str(pdf_path))
    if pdf_path.suffix.lower() != ".pdf":
        raise ValueError("Only .pdf is supported for import-health-pdf")

    rk = (record_kind or record_type or "").strip()
    if not rk:
        raise ValueError("record_kind is required (or pass legacy --record-type)")
    person_key = normalize_person_key(person)
    domain = domain.strip() or "health"

    content_hash = sha256_file(pdf_path)
    record_id = stable_record_id(person_key=person_key, domain=domain, record_kind=rk, content_hash_sha256=content_hash)
    dest_path = ensure_raw_destination(
        raw_root,
        domain=domain,
        record_kind=rk,
        content_hash_sha256=content_hash,
        src_path=pdf_path,
    )
    if not dest_path.exists():
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(pdf_path, dest_path)

    imported_at = utc_now_iso()
    pdf_text = read_pdf_text(dest_path)
    chunks = chunk_text(pdf_text, chunk_size=chunk_size, overlap=chunk_overlap)

    init_db(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO records(
              record_id, person_key, domain, subdomain, record_kind, record_type, layer, source_system,
              observed_at, imported_at, source, original_path, stored_path, content_hash_sha256,
              sensitivity_tier, notes,
              patient_name, patient_age, doctor_name, facility_name, visit_date_extracted,
              visit_reason, symptoms, prescriptions, clinical_detail, extracted_at, extraction_error,
              vitals_json, visit_age_years, visit_age_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
              person_key=excluded.person_key,
              domain=excluded.domain,
              subdomain=excluded.subdomain,
              record_kind=excluded.record_kind,
              record_type=excluded.record_type,
              layer=excluded.layer,
              source_system=excluded.source_system,
              observed_at=COALESCE(excluded.observed_at, records.observed_at),
              imported_at=excluded.imported_at,
              source=excluded.source,
              original_path=excluded.original_path,
              stored_path=COALESCE(records.stored_path, excluded.stored_path),
              content_hash_sha256=excluded.content_hash_sha256,
              sensitivity_tier=excluded.sensitivity_tier,
              notes=COALESCE(excluded.notes, records.notes)
            """,
            (
                record_id,
                person_key,
                domain,
                subdomain,
                rk,
                rk,
                layer,
                source_system,
                observed_at,
                imported_at,
                "import",
                str(pdf_path),
                str(dest_path),
                content_hash,
                sensitivity_tier,
                notes,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        )
        conn.execute("DELETE FROM chunks WHERE record_id = ?", (record_id,))
        created_at = utc_now_iso()
        for idx, chunk in enumerate(chunks):
            chunk_id = hashlib.sha256(f"{record_id}:{idx}".encode("utf-8")).hexdigest()[:24]
            conn.execute(
                """
                INSERT INTO chunks(chunk_id, record_id, chunk_index, text, location, sensitivity_tier, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (chunk_id, record_id, idx, chunk, None, sensitivity_tier, created_at),
            )
        conn.commit()

    return ImportResult(
        record_id=record_id,
        stored_path=dest_path,
        num_chars=len(pdf_text),
        num_chunks=len(chunks),
        content_hash_sha256=content_hash,
    )


def list_records(
    *,
    db_path: Path,
    domain: Optional[str] = None,
    subdomain: Optional[str] = None,
    record_kind: Optional[str] = None,
    limit: int = 20,
) -> List[sqlite3.Row]:
    init_db(db_path)
    where: List[str] = []
    params: List[Any] = []
    if domain:
        where.append("domain = ?")
        params.append(domain)
    if subdomain:
        where.append("subdomain = ?")
        params.append(subdomain)
    if record_kind:
        where.append("record_kind = ?")
        params.append(record_kind)
    sql = "SELECT * FROM records"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY COALESCE(observed_at, imported_at) DESC LIMIT ?"
    params.append(limit)
    with connect(db_path) as conn:
        return conn.execute(sql, tuple(params)).fetchall()


MAX_EXTRACTION_INPUT_CHARS = 14_000


def delete_orphan_records(db_path: Path) -> int:
    init_db(db_path)
    removed = 0
    with connect(db_path) as conn:
        rows = conn.execute("SELECT record_id, stored_path FROM records").fetchall()
        for r in rows:
            p = Path(str(r["stored_path"]))
            if not p.is_file():
                conn.execute("DELETE FROM records WHERE record_id = ?", (str(r["record_id"]),))
                removed += 1
        conn.commit()
    return removed


def get_record_text_for_extraction(conn: sqlite3.Connection, record_id: str, *, max_chars: int = MAX_EXTRACTION_INPUT_CHARS) -> str:
    rows = conn.execute(
        "SELECT text FROM chunks WHERE record_id = ? ORDER BY chunk_index",
        (record_id,),
    ).fetchall()
    text = "\n\n".join([str(r["text"]) for r in rows if r["text"]]).strip()
    if not text:
        row = conn.execute("SELECT stored_path FROM records WHERE record_id = ?", (record_id,)).fetchone()
        if row:
            sp = Path(str(row["stored_path"]))
            if sp.suffix.lower() == ".pdf" and sp.is_file():
                text = read_pdf_text(sp)
    if max_chars > 0 and len(text) > max_chars:
        text = text[:max_chars] + "\n\n[truncated]"
    return text


def set_extraction_error(conn: sqlite3.Connection, record_id: str, message: str) -> None:
    msg = (message or "").strip()[:2000]
    conn.execute(
        "UPDATE records SET extraction_error = ?, extracted_at = ? WHERE record_id = ?",
        (msg or "unknown error", utc_now_iso(), record_id),
    )


def _parse_iso_date_ymd(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    t = str(s).strip()
    try:
        return datetime.strptime(t[:10], "%Y-%m-%d")
    except Exception:
        return None


def get_profile(conn: sqlite3.Connection, *, person_key: str) -> Dict[str, Any]:
    if not _table_exists(conn, "profiles"):
        return {}
    row = conn.execute("SELECT profile_json FROM profiles WHERE person_key = ?", (person_key,)).fetchone()
    if not row or not row["profile_json"]:
        return {}
    try:
        d = json.loads(str(row["profile_json"]))
        return dict(d) if isinstance(d, dict) else {}
    except Exception:
        return {}


def upsert_profile_fields(conn: sqlite3.Connection, *, person_key: str, patch: Dict[str, Any]) -> None:
    now = utc_now_iso()
    base = get_profile(conn, person_key=person_key)
    base.update(patch)
    conn.execute(
        """
        INSERT INTO profiles(person_key, profile_json, updated_at)
        VALUES(?, ?, ?)
        ON CONFLICT(person_key) DO UPDATE SET
          profile_json=excluded.profile_json,
          updated_at=excluded.updated_at
        """,
        (person_key, json.dumps(base, ensure_ascii=False), now),
    )


def update_record_from_extraction(conn: sqlite3.Connection, record_id: str, normalized: Dict[str, Any]) -> None:
    visit_date_extracted = normalized.get("visit_date")
    if visit_date_extracted is not None:
        visit_date_extracted = str(visit_date_extracted).strip() or None

    row = conn.execute("SELECT observed_at, person_key FROM records WHERE record_id = ?", (record_id,)).fetchone()
    current_obs = row["observed_at"] if row else None
    person_key = str(row["person_key"]) if row else ""
    observed_at = str(current_obs) if current_obs else None
    if visit_date_extracted and not observed_at:
        observed_at = visit_date_extracted

    # vitals json passthrough
    vj = normalized.get("vitals_json")
    vitals_out: Optional[str]
    if vj is None:
        vitals_out = None
    elif isinstance(vj, str):
        vitals_out = vj
    else:
        vitals_out = json.dumps(vj, ensure_ascii=False)

    # visit age: use profile birthdate + visit_date_extracted (do NOT use current age)
    prof = get_profile(conn, person_key=person_key) if person_key else {}
    bday_dt = _parse_iso_date_ymd(str(prof.get("birthdate") or "").strip() or None)
    visit_dt = _parse_iso_date_ymd(visit_date_extracted)
    visit_age_years: Optional[float] = None
    visit_age_text: Optional[str] = None
    if bday_dt and visit_dt:
        years = visit_dt.year - bday_dt.year
        if (visit_dt.month, visit_dt.day) < (bday_dt.month, bday_dt.day):
            years -= 1
        if years >= 0:
            visit_age_years = float(years)
            visit_age_text = f"{years}y"

    conn.execute(
        """
        UPDATE records SET
          domain = ?,
          subdomain = ?,
          patient_name = ?,
          patient_age = ?,
          doctor_name = ?,
          facility_name = ?,
          visit_date_extracted = ?,
          visit_reason = ?,
          symptoms = ?,
          prescriptions = ?,
          clinical_detail = ?,
          extracted_at = ?,
          extraction_error = NULL,
          observed_at = ?,
          vitals_json = COALESCE(?, vitals_json),
          visit_age_years = COALESCE(?, visit_age_years),
          visit_age_text = COALESCE(?, visit_age_text)
        WHERE record_id = ?
        """,
        (
            str(normalized.get("domain") or "health"),
            normalized.get("subdomain"),
            normalized.get("patient_name"),
            normalized.get("patient_age"),
            normalized.get("doctor_name"),
            normalized.get("facility_name"),
            visit_date_extracted,
            normalized.get("visit_reason"),
            normalized.get("symptoms"),
            normalized.get("prescriptions"),
            normalized.get("clinical_detail"),
            utc_now_iso(),
            observed_at,
            vitals_out,
            visit_age_years,
            visit_age_text,
            record_id,
        ),
    )


def list_record_ids_for_person(db_path: Path, person_key: str, *, domain: Optional[str] = None) -> List[str]:
    init_db(db_path)
    with connect(db_path) as conn:
        if domain and domain != "all":
            rows = conn.execute(
                "SELECT record_id FROM records WHERE person_key = ? AND domain = ? ORDER BY imported_at DESC",
                (person_key, domain),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT record_id FROM records WHERE person_key = ? ORDER BY imported_at DESC",
                (person_key,),
            ).fetchall()
        return [str(r["record_id"]) for r in rows]


def list_distinct_domains(*, db_path: Path, person_key: str) -> List[str]:
    init_db(db_path)
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT domain FROM records WHERE person_key = ? ORDER BY domain COLLATE NOCASE",
            (person_key,),
        ).fetchall()
        return [str(r["domain"]) for r in rows]


def _infer_domain_record_kind_from_raw_path(
    pdf_path: Path,
    raw_root: Path,
    *,
    default_domain: str,
    default_record_kind: str,
) -> Tuple[str, str]:
    try:
        rel = pdf_path.resolve().relative_to(raw_root.resolve())
    except ValueError:
        return default_domain, default_record_kind
    parts = rel.parts
    if len(parts) >= 3:
        return parts[0], parts[1]
    if len(parts) == 2:
        return parts[0], "imported_pdf"
    return default_domain, default_record_kind


def scan_import_new_pdfs(
    *,
    person: str,
    db_path: Path,
    raw_root: Path,
    default_domain: str = "health",
    default_record_kind: str = "imported_pdf",
    layer: str = "raw",
    source_system: str = "local_folder_scan",
    sensitivity_tier: str = "A",
    chunk_size: int = 1200,
    chunk_overlap: int = 150,
) -> ScanImportResult:
    person_key = normalize_person_key(person)
    init_db(db_path)
    errors: List[str] = []
    imported = 0
    skipped = 0
    with connect(db_path) as conn:
        known_hashes = {
            str(r[0])
            for r in conn.execute(
                "SELECT content_hash_sha256 FROM records WHERE person_key = ?",
                (person_key,),
            ).fetchall()
        }
    if not raw_root.is_dir():
        return ScanImportResult(0, 0, [f"raw 目录不存在: {raw_root}"])

    for pdf_path in sorted(raw_root.rglob("*.pdf")):
        if not pdf_path.is_file():
            continue
        try:
            content_hash = sha256_file(pdf_path)
        except OSError as e:
            errors.append(f"{pdf_path}: {e}")
            continue
        if content_hash in known_hashes:
            skipped += 1
            continue
        dom, rk = _infer_domain_record_kind_from_raw_path(
            pdf_path,
            raw_root,
            default_domain=default_domain,
            default_record_kind=default_record_kind,
        )
        try:
            import_health_pdf(
                person=person,
                db_path=db_path,
                raw_root=raw_root,
                pdf_path=pdf_path,
                domain=dom,
                subdomain=None,
                record_kind=rk,
                record_type=None,
                layer=layer,
                source_system=source_system,
                observed_at=None,
                sensitivity_tier=sensitivity_tier,
                notes=None,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
            )
            imported += 1
            known_hashes.add(content_hash)
        except Exception as e:  # noqa: BLE001
            errors.append(f"{pdf_path.name}: {e}")

    return ScanImportResult(imported=imported, skipped_already_in_db=skipped, errors=errors)


def create_memory(
    conn: sqlite3.Connection,
    *,
    person_key: str,
    memory_type: str,
    content: str,
    confidence: str = "medium",
    status: str = "tentative",
    domain: Optional[str] = None,
    sensitivity_tier: Optional[str] = None,
    source_record_id: Optional[str] = None,
    source_chunk_id: Optional[str] = None,
) -> str:
    mid = uuid4().hex[:24]
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO memories(
          memory_id, person_key, memory_type, content,
          confidence, status, domain, sensitivity_tier,
          source_record_id, source_chunk_id,
          created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mid,
            person_key,
            memory_type,
            content,
            confidence,
            status,
            domain,
            sensitivity_tier,
            source_record_id,
            source_chunk_id,
            now,
            now,
        ),
    )
    return mid


def list_memories(conn: sqlite3.Connection, *, person_key: str, limit: int = 200) -> List[sqlite3.Row]:
    if not _table_exists(conn, "memories"):
        return []
    return conn.execute(
        """
        SELECT memory_id, memory_type, content, confidence, status, domain, sensitivity_tier,
               source_record_id, source_chunk_id, created_at, updated_at
          FROM memories
         WHERE person_key = ?
         ORDER BY updated_at DESC
         LIMIT ?
        """,
        (person_key, limit),
    ).fetchall()


def set_memory_status(conn: sqlite3.Connection, *, person_key: str, memory_id: str, status: str) -> None:
    now = utc_now_iso()
    conn.execute(
        "UPDATE memories SET status = ?, updated_at = ? WHERE person_key = ? AND memory_id = ?",
        (status, now, person_key, memory_id),
    )

