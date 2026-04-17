from __future__ import annotations

import hashlib
import os
import json
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from .extraction_profiles import compute_stored_profile_fields, select_chunk_texts_for_extraction_safe
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


def delete_records_by_ids(db_path: Path, *, person_key: str, record_ids: List[str]) -> int:
    """
    Delete records (and cascading chunks) by record_id for a person.
    """
    if not record_ids:
        return 0
    init_db(db_path)
    ids = [str(rid).strip() for rid in record_ids if str(rid).strip()]
    if not ids:
        return 0
    with connect(db_path) as conn:
        removed = 0
        for rid in ids:
            cur = conn.execute(
                "DELETE FROM records WHERE person_key = ? AND record_id = ?",
                (person_key, rid),
            )
            removed += int(cur.rowcount or 0)
        conn.commit()
        return removed


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


_PROFILE_META_COLUMN_DDL: List[Tuple[str, str]] = [
    ("source_kind", "TEXT"),
    ("document_family", "TEXT"),
    ("extraction_intent", "TEXT"),
    ("extraction_profile_id", "TEXT"),
]


def _ensure_profile_meta_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "records"):
        return
    cols = _table_columns(conn, "records")
    for name, typ in _PROFILE_META_COLUMN_DDL:
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
    _ensure_profile_meta_columns(conn)
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


def stable_record_id_with_salt(
    *,
    person_key: str,
    domain: str,
    record_kind: str,
    content_hash_sha256: str,
    salt: str,
) -> str:
    """
    Create multiple records that refer to the same document by varying `salt`
    (e.g. visit_date, page range, or an ordinal).
    """
    s = (salt or "").strip()
    base = f"{person_key}:{domain}:{record_kind}:{content_hash_sha256}:{s}".encode("utf-8")
    return hashlib.sha256(base).hexdigest()[:24]


def extract_candidate_visit_dates_from_text(text: str, *, include_all: bool = False) -> List[str]:
    """
    Best-effort local extraction of date strings from PDF text.
    Returns normalized YYYY-MM-DD strings, sorted ascending, de-duplicated.
    No LLM is used.
    """
    t = text or ""
    if not t.strip():
        return []

    found: Set[str] = set()

    # YYYY-MM-DD or YYYY/MM/DD (also allow single-digit month/day)
    for m in re.finditer(r"\b(20\d{2}|19\d{2})[/-](\d{1,2})[/-](\d{1,2})\b", t):
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1900 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
            found.add(f"{y:04d}-{mo:02d}-{d:02d}")

    # MM/DD/YYYY (common in US visit summaries)
    for m in re.finditer(r"\b(\d{1,2})/(\d{1,2})/(20\d{2}|19\d{2})\b", t):
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1900 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
            found.add(f"{y:04d}-{mo:02d}-{d:02d}")

    # Month name formats: Jan 2, 2025 / January 2 2025
    month_map = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }
    for m in re.finditer(
        r"\b(?P<mon>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"[ ,]+(?P<day>\d{1,2})(?:st|nd|rd|th)?[ ,]+(?P<year>20\d{2}|19\d{2})\b",
        t,
        flags=re.IGNORECASE,
    ):
        mon_s = str(m.group("mon") or "").strip().lower()
        mon_s = mon_s.replace(".", "")
        mo = month_map.get(mon_s)
        d = int(m.group("day"))
        y = int(m.group("year"))
        if mo and 1900 <= y <= 2100 and 1 <= d <= 31:
            found.add(f"{y:04d}-{mo:02d}-{d:02d}")

    if include_all:
        return sorted(found)

    # Heuristic: keep only plausible "visit date" candidates.
    # - Drop dates too far in the past (often DOBs / historical problems list)
    # - Drop future dates (often appointment reminders)
    # Users can still override by passing explicit --visit-dates or --include-all-visits.
    today = datetime.now().date()
    min_date = datetime(2023, 1, 1).date()
    max_date = today
    kept: List[str] = []
    for s in sorted(found):
        try:
            dt = datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            continue
        if min_date <= dt <= max_date:
            kept.append(s)
    return kept


@dataclass(frozen=True)
class EncounterItem:
    visit_date: str  # YYYY-MM-DD
    time_text: str  # e.g. "12:30 PM" (may be empty)
    encounter_type: str  # e.g. "Office Visit" / "Telephone Encounter"
    salt: str  # stable per-encounter discriminator


def _encounters_section_slice(t: str) -> str:
    """
    Return Encounters-region text. If the word 'Encounters' is missing (e.g. profile already sliced
    to that span), treat the whole string as the section.
    """
    t = t or ""
    low = t.lower()
    start = low.find("encounters")
    if start < 0:
        return t
    end = low.find("assessments", start)
    if end < 0:
        end = low.find("problems", start)
    if end < 0:
        end = min(len(t), start + 8000)
    return t[start:end]


def extract_encounter_row_blocks_from_text(text: str, *, year: int) -> List[Tuple[EncounterItem, str]]:
    """
    Parse Encounters table into (EncounterItem, row_block_text) pairs.
    `row_block_text` includes the date/time line through the next row's date (exclusive), so it is
    suitable as extraction input for a single visit row.
    """
    sec = _encounters_section_slice(text)
    if not sec.strip():
        return []

    row_re = re.compile(
        r"(?P<date>\d{1,2}/\d{1,2}/(20\d{2}))\s*(?P<hm>\d{1,2}:\d{2})\s*(?P<ap>AM|PM)",
        flags=re.IGNORECASE,
    )
    matches = list(row_re.finditer(sec))
    if not matches:
        return []

    out: List[Tuple[EncounterItem, str]] = []
    for i, m in enumerate(matches):
        date_raw = str(m.group("date") or "")
        try:
            mm, dd, yyyy = date_raw.split("/")
        except ValueError:
            continue
        y = int(yyyy)
        if y != int(year):
            continue
        visit_date = f"{y:04d}-{int(mm):02d}-{int(dd):02d}"
        time_text = f"{m.group('hm')} {str(m.group('ap') or '').upper()}".strip()

        row_end = matches[i + 1].start() if i + 1 < len(matches) else len(sec)
        tail = sec[m.end() : row_end]
        etype = ""
        for line in [ln.strip() for ln in tail.splitlines() if ln.strip()]:
            l = line.lower()
            if "office visit" in l:
                etype = "Office Visit"
                break
            if "preventive" in l:
                etype = "Preventive Care"
                break
            if "telephone" in l:
                etype = "Telephone Encounter"
                break
        if not etype:
            etype = "Encounter"

        salt = f"{visit_date}|{time_text}|{etype}|idx{i}"
        item = EncounterItem(visit_date=visit_date, time_text=time_text, encounter_type=etype, salt=salt)
        block_text = sec[m.start() : row_end].strip()
        out.append((item, block_text))
    return out


def extract_encounter_items_from_text(text: str, *, year: int) -> List[EncounterItem]:
    """
    Extract encounters from the 'Encounters' section only.

    Clinical summary PDFs contain many unrelated dates (Problems/Allergies/etc).
    This function focuses on the Encounters table rows and keeps duplicates for the same date
    (e.g. an office visit + a telephone encounter on the same day).
    """
    return [it for it, _ in extract_encounter_row_blocks_from_text(text, year=year)]


_ICD10_RE = re.compile(r"\b[A-TV-Z][0-9]{2}(?:\.[0-9A-Z]{1,4})?\b")


def parse_encounter_row_deterministic(block_text: str, *, encounter_type: str = "") -> Dict[str, Any]:
    """
    Best-effort rule-based parse of a single Encounters row block (after row binding).
    Intended to fill \"structure\" fields so the LLM can focus on narrative/summary.

    Keys (all optional): facility_name, doctor_name, symptoms (multi-line diagnosis text),
    visit_reason (often billing/visit-type lines).
    """
    out: Dict[str, Any] = {}
    lines = [ln.strip() for ln in (block_text or "").splitlines() if ln.strip()]
    if not lines:
        return out

    row_start = re.compile(
        r"^\d{1,2}/\d{1,2}/20\d{2}\s+\d{1,2}:\d{2}\s+(AM|PM)\b",
        re.IGNORECASE,
    )

    def is_billing_line(line: str) -> bool:
        low = line.lower()
        if "992" in line or "est pt" in low or "new pt" in low:
            return True
        if "level" in low and "(" in line and ")" in line:
            return True
        if "telephone encounter" in low and "min" in low:
            return True
        return False

    def looks_facility(line: str) -> bool:
        low = line.lower()
        return any(
            x in low
            for x in (
                "family medicine",
                "medical center",
                "medical group",
                "health system",
                "clinic",
                "hospital",
                " pediatrics",
                "urgent care",
            )
        )

    def looks_complaint_fragment(line: str) -> bool:
        low = line.lower()
        keys = (
            "pain",
            "cough",
            "deficiency",
            "nausea",
            "headache",
            "throat",
            "fever",
            "acute ",
            "chronic",
            "screen",
            "exposure",
            "fatigue",
            "dizziness",
            "vomit",
            "diarrhea",
            "rash",
            "infection",
            "encounter for",
            "routine",
            "physical",
        )
        return any(k in low for k in keys)

    def looks_provider(line: str) -> bool:
        if _ICD10_RE.search(line):
            return False
        if looks_complaint_fragment(line):
            return False
        if is_billing_line(line) or looks_facility(line):
            return False
        low = line.lower()
        if ", md" in low or re.search(r"\bmd\b", low):
            return True
        parts = line.split()
        if 1 <= len(parts) <= 5:
            if all(p and (p[0].isupper() or not p[0].isalpha()) for p in parts):
                letters = sum(1 for p in parts if any(c.isalpha() for c in p))
                return letters >= 1
        return False

    i = 0
    if row_start.match(lines[0]):
        i = 1

    chief_bits: List[str] = []
    diag_lines: List[str] = []
    facility: Optional[str] = None
    doctor: Optional[str] = None

    while i < len(lines):
        line = lines[i]
        if is_billing_line(line):
            chief_bits.append(line)
            i += 1
            continue
        if facility is None and looks_facility(line):
            facility = line
            i += 1
            continue
        if doctor is None and looks_provider(line):
            doctor = line.replace("  ", " ").strip()
            if doctor.lower().startswith("dr."):
                doctor = doctor[3:].strip()
            i += 1
            continue
        diag_lines.append(line)
        i += 1

    if facility:
        out["facility_name"] = facility
    if doctor:
        out["doctor_name"] = doctor
    if diag_lines:
        out["symptoms"] = "\n".join(diag_lines).strip()
    if chief_bits:
        out["visit_reason"] = " | ".join(chief_bits)
    elif (encounter_type or "").strip():
        out["visit_reason"] = encounter_type.strip()
    return out


def merge_deterministic_encounter_into_normalized(norm: Dict[str, Any], det: Optional[Dict[str, Any]]) -> None:
    """Prefer parser output for table-like fields; leave LLM narrative when parser has nothing."""
    if not det:
        return
    if det.get("facility_name"):
        norm["facility_name"] = str(det["facility_name"]).strip()
    if det.get("doctor_name"):
        norm["doctor_name"] = str(det["doctor_name"]).strip()
    if det.get("symptoms"):
        norm["symptoms"] = str(det["symptoms"]).strip()
    if det.get("visit_reason") and not str(norm.get("visit_reason") or "").strip():
        norm["visit_reason"] = str(det["visit_reason"]).strip()


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


def read_pdf_text_first_pages(pdf_path: Path, *, max_pages: int = 3) -> str:
    """First N pages only — for demographics / header parsing (phase B)."""
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Missing dependency: pypdf. Please install it in the venv.") from e

    reader = PdfReader(str(pdf_path))
    texts: List[str] = []
    n = min(max(1, int(max_pages)), len(reader.pages))
    for i in range(n):
        try:
            page_text = reader.pages[i].extract_text() or ""
        except Exception:
            page_text = ""
        if page_text.strip():
            texts.append(f"[page {i + 1}]\n{page_text.strip()}\n")
    return "\n".join(texts).strip()


def _clean_demographics_line(s: str) -> str:
    t = (s or "").strip()
    for sep in ("  MRN", " MR#", " (MRN", " DOB", " Age"):
        j = t.find(sep)
        if j > 0:
            t = t[:j].strip()
            break
    return t.strip(" ,;")[:500]


def parse_demographics_from_text(text: str) -> Dict[str, Optional[str]]:
    """
    Best-effort header parse from PDF front matter (Epic / common US exports).
    Returns optional patient_name and pcp_name (primary care physician).
    """
    out: Dict[str, Optional[str]] = {}
    if not (text or "").strip():
        return out
    t = text

    patient_patterns = [
        re.compile(r"(?:^|[\n\r])\s*Patient\s*Name\s*[:#]\s*([^\n\r]+)", re.IGNORECASE),
        re.compile(r"(?:^|[\n\r])\s*Patient\s*[:#]\s*([^\n\r]+)", re.IGNORECASE),
        re.compile(r"(?:^|[\n\r])\s*Name\s*[:#]\s*([^\n\r]+)", re.IGNORECASE),
    ]
    for pat in patient_patterns:
        m = pat.search(t)
        if m:
            name = _clean_demographics_line(m.group(1))
            low = name.lower()
            if len(name) >= 2 and "date of birth" not in low and "dob" not in low[:20]:
                out["patient_name"] = name
                break

    pcp_patterns = [
        re.compile(
            r"(?:^|[\n\r])\s*Primary\s+Care\s+Physician\s*[:#]\s*([^\n\r]+)", re.IGNORECASE
        ),
        re.compile(r"(?:^|[\n\r])\s*PCP\s*[:#]\s*([^\n\r]+)", re.IGNORECASE),
        re.compile(
            r"(?:^|[\n\r])\s*Primary\s+Care\s+Provider\s*[:#]\s*([^\n\r]+)", re.IGNORECASE
        ),
        re.compile(r"(?:^|[\n\r])\s*Rendering\s+Provider\s*[:#]\s*([^\n\r]+)", re.IGNORECASE),
    ]
    for pat in pcp_patterns:
        m = pat.search(t)
        if m:
            out["pcp_name"] = _clean_demographics_line(m.group(1))
            break

    return out


def merge_demographics_into_normalized(norm: Dict[str, Any], demo: Optional[Dict[str, Any]]) -> None:
    """Fill patient / PCP when still empty after encounter-row merge; profile canonical applies later."""
    if not demo:
        return
    if not str(norm.get("patient_name") or "").strip() and demo.get("patient_name"):
        norm["patient_name"] = str(demo["patient_name"]).strip()
    if not str(norm.get("doctor_name") or "").strip() and demo.get("pcp_name"):
        norm["doctor_name"] = str(demo["pcp_name"]).strip()


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
    source_kind: Optional[str] = None,
    document_family: Optional[str] = None,
    extraction_intent: Optional[str] = None,
    extraction_profile_id: Optional[str] = None,
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

    sk, fam, ei, pid = compute_stored_profile_fields(
        domain=domain,
        record_kind=rk,
        notes=notes,
        extract_strategy=None,
        source_kind=source_kind,
        document_family=document_family,
        extraction_intent=extraction_intent,
        extraction_profile_id=extraction_profile_id,
    )

    init_db(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO records(
              record_id, person_key, domain, subdomain, record_kind, record_type, layer, source_system,
              observed_at, imported_at, source, original_path, stored_path, content_hash_sha256,
              sensitivity_tier, notes,
              source_kind, document_family, extraction_intent, extraction_profile_id,
              patient_name, patient_age, doctor_name, facility_name, visit_date_extracted,
              visit_reason, symptoms, prescriptions, clinical_detail, extracted_at, extraction_error,
              vitals_json, visit_age_years, visit_age_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              notes=COALESCE(excluded.notes, records.notes),
              source_kind=excluded.source_kind,
              document_family=excluded.document_family,
              extraction_intent=excluded.extraction_intent,
              extraction_profile_id=excluded.extraction_profile_id
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
                sk,
                fam,
                ei,
                pid,
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


def import_health_pdf_multi(
    *,
    person: str,
    db_path: Path,
    raw_root: Path,
    pdf_path: Path,
    domain: str,
    subdomain: Optional[str],
    record_kind: str,
    layer: str,
    source_system: Optional[str],
    sensitivity_tier: str,
    notes: Optional[str],
    visit_dates: List[str],
    max_visits: int = 0,
    include_all_visits: bool = False,
    extract_strategy: str = "dates",
    visit_year: int = 0,
    chunk_size: int,
    chunk_overlap: int,
    source_kind: Optional[str] = None,
    document_family: Optional[str] = None,
    extraction_intent: Optional[str] = None,
    extraction_profile_id: Optional[str] = None,
) -> List[ImportResult]:
    """
    Import ONE PDF but create MULTIPLE records that all refer to the same stored PDF file.
    Each visit date becomes a separate record_id (via salt=visit_date).
    """
    if not pdf_path.exists():
        raise FileNotFoundError(str(pdf_path))
    if pdf_path.suffix.lower() != ".pdf":
        raise ValueError("Only .pdf is supported for import-health-pdf-multi")
    # If caller didn't provide explicit dates, try extracting from the PDF text locally.
    # (No LLM is used; this is best-effort.)

    rk = (record_kind or "").strip()
    if not rk:
        raise ValueError("record_kind is required")
    person_key = normalize_person_key(person)
    domain = domain.strip() or "health"

    content_hash = sha256_file(pdf_path)
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
    encounter_items: List[EncounterItem] = []
    if not visit_dates and extract_strategy == "encounters":
        if not visit_year:
            raise ValueError("visit_year is required when extract_strategy='encounters'")
        encounter_items = extract_encounter_items_from_text(pdf_text, year=int(visit_year))
        if max_visits > 0 and len(encounter_items) > max_visits:
            encounter_items = encounter_items[-max_visits:]
        if not encounter_items:
            raise ValueError("No encounters could be extracted from Encounters section")
    elif not visit_dates:
        visit_dates = extract_candidate_visit_dates_from_text(pdf_text, include_all=include_all_visits)
        if max_visits > 0 and len(visit_dates) > max_visits:
            visit_dates = visit_dates[-max_visits:]
        if not visit_dates:
            raise ValueError("No visit dates provided and none could be extracted from PDF text")
    chunks = chunk_text(pdf_text, chunk_size=chunk_size, overlap=chunk_overlap)

    init_db(db_path)
    out: List[ImportResult] = []
    with connect(db_path) as conn:
        created_at = utc_now_iso()
        if encounter_items:
            iterable = [(it.visit_date, it.salt, it.encounter_type, it.time_text) for it in encounter_items]
        else:
            iterable = [(str(vd).strip(), str(vd).strip(), "", "") for vd in visit_dates if str(vd or "").strip()]

        for visit_date, salt, etype, time_text in iterable:
            record_id = stable_record_id_with_salt(
                person_key=person_key,
                domain=domain,
                record_kind=rk,
                content_hash_sha256=content_hash,
                salt=salt,
            )
            notes_out = (notes or "").strip()
            if etype or time_text:
                prefix = f"[encounter] {etype} {time_text}".strip()
                notes_out = (prefix + ("\n" + notes_out if notes_out else "")).strip()
            sk, fam, ei, pid = compute_stored_profile_fields(
                domain=domain,
                record_kind=rk,
                notes=notes_out,
                extract_strategy=extract_strategy,
                source_kind=source_kind,
                document_family=document_family,
                extraction_intent=extraction_intent,
                extraction_profile_id=extraction_profile_id,
            )
            conn.execute(
                """
                INSERT INTO records(
                  record_id, person_key, domain, subdomain, record_kind, record_type, layer, source_system,
                  observed_at, imported_at, source, original_path, stored_path, content_hash_sha256,
                  sensitivity_tier, notes,
                  source_kind, document_family, extraction_intent, extraction_profile_id,
                  patient_name, patient_age, doctor_name, facility_name, visit_date_extracted,
                  visit_reason, symptoms, prescriptions, clinical_detail, extracted_at, extraction_error,
                  vitals_json, visit_age_years, visit_age_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                  notes=COALESCE(excluded.notes, records.notes),
                  source_kind=excluded.source_kind,
                  document_family=excluded.document_family,
                  extraction_intent=excluded.extraction_intent,
                  extraction_profile_id=excluded.extraction_profile_id,
                  visit_date_extracted=COALESCE(excluded.visit_date_extracted, records.visit_date_extracted)
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
                    visit_date,  # observed_at
                    imported_at,
                    "import_multi",
                    str(pdf_path),
                    str(dest_path),
                    content_hash,
                    sensitivity_tier,
                    notes_out or None,
                    sk,
                    fam,
                    ei,
                    pid,
                    None,
                    None,
                    None,
                    None,
                    visit_date,  # visit_date_extracted prefilled
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
            for idx, chunk in enumerate(chunks):
                chunk_id = hashlib.sha256(f"{record_id}:{idx}".encode("utf-8")).hexdigest()[:24]
                conn.execute(
                    """
                    INSERT INTO chunks(chunk_id, record_id, chunk_index, text, location, sensitivity_tier, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (chunk_id, record_id, idx, chunk, None, sensitivity_tier, created_at),
                )
            out.append(
                ImportResult(
                    record_id=record_id,
                    stored_path=dest_path,
                    num_chars=len(pdf_text),
                    num_chunks=len(chunks),
                    content_hash_sha256=content_hash,
                )
            )
        conn.commit()
    return out


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


def _infer_year_for_encounter_parse(visit_date_extracted: Optional[Any], observed_at: Optional[Any]) -> int:
    for s in (visit_date_extracted, observed_at):
        if not s:
            continue
        t = str(s).strip()
        if len(t) >= 4 and t[:4].isdigit():
            try:
                y = int(t[:4])
                if 1990 <= y <= 2100:
                    return y
            except Exception:
                continue
    return datetime.now().year


def _bind_encounter_row_text_for_record(
    *,
    conn: sqlite3.Connection,
    record_id: str,
    text: str,
    dbg: Any,
) -> Tuple[str, str, Optional[str], Optional[Dict[str, Any]]]:
    """
    For yearly_longitudinal_summary + visits_only, narrow extraction input to the single Encounters
    row that matches this record_id (same salt as import_health_pdf_multi).

    Returns (text, status, bound_visit_iso, row_deterministic) where status is logged to stderr.
    When binding hits, `bound_visit_iso` is YYYY-MM-DD and `row_deterministic` is parse_encounter_row_deterministic(...).
    """
    try:
        enabled = (os.getenv("AI_ASSISTANT_ENCOUNTER_ROW_BIND") or "1").strip().lower() in {"", "1", "true", "yes", "on"}
    except Exception:
        enabled = True
    if not enabled:
        return text, "encounter_row_bind=disabled", None, None

    dbg_s = str(dbg or "").lower()
    if ("yearly_longitudinal_summary" not in dbg_s) or ("visits_only" not in dbg_s):
        return text, "encounter_row_bind=skipped_profile", None, None

    row = conn.execute(
        """
        SELECT person_key, domain, record_kind, content_hash_sha256, visit_date_extracted, observed_at, stored_path
          FROM records WHERE record_id = ?
        """,
        (record_id,),
    ).fetchone()
    if not row:
        return text, "encounter_row_bind=no_record", None, None

    person_key = str(row["person_key"] or "")
    domain = str(row["domain"] or "")
    record_kind = str(row["record_kind"] or "")
    chash = str(row["content_hash_sha256"] or "")
    if not (person_key and domain and record_kind and chash):
        return text, "encounter_row_bind=missing_keys", None, None

    year = _infer_year_for_encounter_parse(row["visit_date_extracted"], row["observed_at"])

    def _try_blocks(blocks: List[Tuple[EncounterItem, str]], *, source: str) -> Optional[Tuple[str, str, EncounterItem]]:
        if not blocks:
            return None
        for it, blk in blocks:
            rid = stable_record_id_with_salt(
                person_key=person_key,
                domain=domain,
                record_kind=record_kind,
                content_hash_sha256=chash,
                salt=it.salt,
            )
            if rid == record_id:
                msg = (
                    f"encounter_row_bind=hit source={source} year={year} rows={len(blocks)} "
                    f"visit_date={it.visit_date!r} type={it.encounter_type!r} len={len(text)}->{len(blk)}"
                )
                return blk, msg, it
        return None

    hit = _try_blocks(extract_encounter_row_blocks_from_text(text, year=year), source="chunk_text")
    if hit:
        blk, msg, it = hit
        det = parse_encounter_row_deterministic(blk, encounter_type=it.encounter_type)
        return blk, msg, it.visit_date, det

    # Import-time salt uses idx over FULL-PDF Encounters matches; chunk-only text can desync indices.
    sp = Path(str(row["stored_path"] or ""))
    if sp.suffix.lower() == ".pdf" and sp.is_file():
        try:
            full_txt = read_pdf_text(sp)
            hit2 = _try_blocks(extract_encounter_row_blocks_from_text(full_txt, year=year), source="full_pdf")
            if hit2:
                blk2, msg2, it2 = hit2
                det2 = parse_encounter_row_deterministic(blk2, encounter_type=it2.encounter_type)
                return blk2, msg2, it2.visit_date, det2
        except Exception as e:  # noqa: BLE001
            return text, f"encounter_row_bind=full_pdf_failed {type(e).__name__}: {e}", None, None

    return text, f"encounter_row_bind=no_match year={year} (tried chunk_text + full_pdf)", None, None


def get_record_text_for_extraction(
    conn: sqlite3.Connection,
    record_id: str,
    *,
    max_chars: int = MAX_EXTRACTION_INPUT_CHARS,
    include_encounter_row_anchor: bool = True,
) -> Tuple[str, Optional[str], Optional[Dict[str, Any]]]:
    meta_row = conn.execute(
        """
        SELECT person_key, domain, record_kind, notes, source_kind, document_family, extraction_intent, extraction_profile_id
          FROM records WHERE record_id = ?
        """,
        (record_id,),
    ).fetchone()
    meta = {str(k): meta_row[k] for k in meta_row.keys()} if meta_row else {}
    rows = conn.execute(
        "SELECT chunk_index, text FROM chunks WHERE record_id = ? ORDER BY chunk_index",
        (record_id,),
    ).fetchall()
    chunk_rows = [{str(k): r[k] for k in r.keys()} for r in rows]
    text, dbg = select_chunk_texts_for_extraction_safe(chunk_rows=chunk_rows, record_meta=meta)
    print(f"[get_record_text_for_extraction] record_id={record_id} {dbg}", file=sys.stderr)
    if not text.strip():
        row = conn.execute("SELECT stored_path FROM records WHERE record_id = ?", (record_id,)).fetchone()
        if row:
            sp = Path(str(row["stored_path"]))
            if sp.suffix.lower() == ".pdf" and sp.is_file():
                text = read_pdf_text(sp)

    # Bind yearly visits_only extraction to ONE Encounters row (same salt as import_multi).
    text, bind_msg, bound_visit_iso, row_det = _bind_encounter_row_text_for_record(conn=conn, record_id=record_id, text=text, dbg=dbg)
    print(f"[structure_route] record_id={record_id} {bind_msg}", file=sys.stderr)
    if row_det:
        print(
            f"[encounter_row_det] record_id={record_id} keys={list(row_det.keys())} "
            f"facility={row_det.get('facility_name')!r} doctor={row_det.get('doctor_name')!r} "
            f"sym_len={len(str(row_det.get('symptoms') or ''))}",
            file=sys.stderr,
        )

    # Optional: for yearly summaries, reduce cross-section noise by keeping only visit_event spans.
    # This is a best-effort step (falls back to original text if labeling fails).
    try:
        enable = (os.getenv("AI_ASSISTANT_VISITS_ONLY_SPAN_FILTER") or "").strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        enable = False
    if enable:
        # After encounter-row binding, input is already a single visit row; vitals_line_strip LLM
        # often mis-deletes the date line and nukes visit_date. Skip filter in that case.
        if "encounter_row_bind=hit" in bind_msg:
            print(
                f"[visit_event_filter] record_id={record_id} skipped (encounter_row_bind narrowed input)",
                file=sys.stderr,
            )
        else:
            # Meta fields may be empty for some legacy rows; the chunk selection debug string is more reliable.
            dbg_s = str(dbg or "").lower()
            should_filter = ("yearly_longitudinal_summary" in dbg_s) and ("visits_only" in dbg_s)
            if should_filter:
                try:
                    from .visit_event_filter import filter_text_to_visit_events

                    filtered, fdbg = filter_text_to_visit_events(text)
                    err = str(fdbg.get("error") or "")
                    err_s = f" err={err!r}" if err else ""
                    print(
                        "[visit_event_filter]"
                        f" record_id={record_id}"
                        f" mode={fdbg.get('mode')}"
                        f" vitals_ranges={fdbg.get('vitals_ranges_merged', fdbg.get('vitals_ranges_in'))}"
                        f" lines={fdbg.get('lines_total')}"
                        f" removed={fdbg.get('lines_removed')} kept={fdbg.get('lines_kept')}"
                        f" len={fdbg.get('len_in')}->{fdbg.get('len_out')}"
                        f"{err_s}",
                        file=sys.stderr,
                    )
                    text = filtered
                except Exception as e:  # noqa: BLE001
                    print(f"[visit_event_filter] record_id={record_id} failed: {type(e).__name__}: {e}", file=sys.stderr)
            else:
                print(f"[visit_event_filter] record_id={record_id} skipped (dbg did not indicate yearly+visits_only)", file=sys.stderr)
    if include_encounter_row_anchor and bound_visit_iso:
        anchor = (
            "[ENCOUNTER_ROW_ANCHOR]\n"
            "(META: not part of the medical record — do NOT quote this block in any JSON field.)\n"
            f"Known encounter visit_date (YYYY-MM-DD): {bound_visit_iso}\n"
            'You MUST set JSON field "visit_date" to this exact value.\n'
            'Put a verbatim date/time snippet from the CLINICAL encounter block BELOW into "visit_date_evidence" '
            "(not ICD codes alone, not billing/visit-type lines alone).\n"
            "[/ENCOUNTER_ROW_ANCHOR]\n\n"
        )
        text = anchor + text
        print(
            f"[encounter_row_bind] anchor_injected record_id={record_id} visit_date={bound_visit_iso!r}",
            file=sys.stderr,
        )
    if max_chars > 0 and len(text) > max_chars:
        text = text[:max_chars] + "\n\n[truncated]"
    return text, bound_visit_iso, row_det


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
