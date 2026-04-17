from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

from .db import connect, delete_records_by_ids, import_health_pdf, import_health_pdf_multi, init_db, list_records
from .paths import get_paths_for_person
from .subjects import normalize_person_key


def _ensure_repo_tools_shared_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    shared = repo_root / "tools" / "shared"
    s = str(shared)
    if s not in sys.path:
        sys.path.insert(0, s)


def cmd_init_db(args: argparse.Namespace) -> int:
    person_key = normalize_person_key(args.person)
    paths = get_paths_for_person(person_key)
    init_db(paths.db_path)
    print(f"OK: initialized db at {paths.db_path}")
    return 0


def cmd_import_health_pdf(args: argparse.Namespace) -> int:
    person_key = normalize_person_key(args.person)
    paths = get_paths_for_person(person_key)
    res = import_health_pdf(
        person=person_key,
        db_path=paths.db_path,
        raw_root=paths.raw_dir,
        pdf_path=Path(args.pdf).expanduser(),
        domain=args.domain,
        subdomain=args.subdomain,
        record_kind=args.record_kind,
        record_type=args.record_type,
        layer=args.layer,
        source_system=args.source_system,
        observed_at=args.observed_at,
        sensitivity_tier=args.sensitivity_tier,
        notes=args.notes,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
        source_kind=getattr(args, "source_kind", None),
        document_family=getattr(args, "document_family", None),
        extraction_intent=getattr(args, "extraction_intent", None),
        extraction_profile_id=getattr(args, "extraction_profile_id", None),
    )
    print(f"OK: imported record_id={res.record_id} stored_path={res.stored_path} chunks={res.num_chunks}")
    return 0


def cmd_import_health_pdf_multi(args: argparse.Namespace) -> int:
    person_key = normalize_person_key(args.person)
    paths = get_paths_for_person(person_key)
    if getattr(args, "dry_run", False):
        # Use the DB helper but prevent writes by pointing to a temp in-memory DB is non-trivial here.
        # Instead, we read the PDF text and re-use the extractor indirectly by passing empty visit_dates
        # and a dummy db_path; but import function performs writes, so we implement a lightweight dry-run here.
        from .db import extract_candidate_visit_dates_from_text, extract_encounter_items_from_text, read_pdf_text  # local import

        pdf_text = read_pdf_text(Path(args.pdf).expanduser())
        strategy = str(getattr(args, "extract_strategy", "dates") or "dates")
        if strategy == "encounters":
            year = int(getattr(args, "visit_year", 0) or 0)
            items = extract_encounter_items_from_text(pdf_text, year=year or 2025)
            dates = [f"{it.visit_date} {it.time_text} {it.encounter_type}".strip() for it in items]
        else:
            dates = extract_candidate_visit_dates_from_text(pdf_text, include_all=bool(getattr(args, "include_all_visits", False)))
        max_visits = int(getattr(args, "max_visits", 0) or 0)
        if max_visits > 0 and len(dates) > max_visits:
            dates = dates[-max_visits:]
        print("DRY RUN: extracted visit dates:")
        for d in dates:
            print(f"- {d}")
        print(f"DRY RUN: total {len(dates)} unique dates.")
        return 0
    visit_dates = [s.strip() for s in str(getattr(args, "visit_dates", "") or "").split(",") if s.strip()]
    if getattr(args, "auto_visit_dates", False) and not visit_dates:
        # Leave visit_dates empty; db.import_health_pdf_multi will attempt local extraction from PDF text.
        pass
    res_list = import_health_pdf_multi(
        person=person_key,
        db_path=paths.db_path,
        raw_root=paths.raw_dir,
        pdf_path=Path(args.pdf).expanduser(),
        domain=args.domain,
        subdomain=args.subdomain,
        record_kind=args.record_kind,
        layer=args.layer,
        source_system=args.source_system,
        sensitivity_tier=args.sensitivity_tier,
        notes=args.notes,
        visit_dates=visit_dates,
        max_visits=int(getattr(args, "max_visits", 0) or 0),
        include_all_visits=bool(getattr(args, "include_all_visits", False)),
        extract_strategy=str(getattr(args, "extract_strategy", "dates") or "dates"),
        visit_year=int(getattr(args, "visit_year", 0) or 0),
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
        source_kind=getattr(args, "source_kind", None),
        document_family=getattr(args, "document_family", None),
        extraction_intent=getattr(args, "extraction_intent", None),
        extraction_profile_id=getattr(args, "extraction_profile_id", None),
    )
    for r in res_list:
        print(f"OK: imported record_id={r.record_id} stored_path={r.stored_path} chunks={r.num_chunks}")
    print(f"OK: imported {len(res_list)} records from 1 pdf (shared stored_path).")
    return 0


def cmd_list_records(args: argparse.Namespace) -> int:
    person_key = normalize_person_key(args.person)
    paths = get_paths_for_person(person_key)
    rows = list_records(
        db_path=paths.db_path,
        domain=args.domain,
        subdomain=args.subdomain,
        record_kind=args.record_kind,
        limit=args.limit,
    )
    for r in rows:
        prof = ""
        rk = list(r.keys())
        if "extraction_profile_id" in rk and r["extraction_profile_id"]:
            prof = f" prof={r['extraction_profile_id']}"
        print(
            f"{r['record_id']}  {r['domain']}/{r['subdomain'] or '-'}  {r['record_kind']}  "
            f"obs={r['observed_at'] or '-'}  imp={r['imported_at']}  "
            f"err={'Y' if r['extraction_error'] else 'N'}{prof}"
        )
    return 0


def cmd_cleanup_pdf_visits(args: argparse.Namespace) -> int:
    """
    Cleanup helper for a multi-visit PDF:
    - Keep encounter-based imports (notes starts with "[encounter]")
    - Delete older/incorrect imports for the same original_path + record_kind
    """
    person_key = normalize_person_key(args.person)
    paths = get_paths_for_person(person_key)
    db_path = paths.db_path
    original_path = str(args.original_path)
    record_kind = str(args.record_kind)
    keep_prefix = str(args.keep_notes_prefix or "").strip()
    dry_run = bool(args.dry_run)

    init_db(db_path)
    with connect(db_path) as conn:  # type: ignore[name-defined]
        rows = conn.execute(
            """
            SELECT record_id, observed_at, imported_at, notes
              FROM records
             WHERE person_key = ? AND original_path = ? AND record_kind = ?
             ORDER BY imported_at DESC
            """,
            (person_key, original_path, record_kind),
        ).fetchall()
    to_delete: List[str] = []
    for r in rows:
        notes = str(r["notes"] or "")
        if keep_prefix and notes.startswith(keep_prefix):
            continue
        to_delete.append(str(r["record_id"]))

    print(f"Found {len(rows)} records for original_path={original_path} record_kind={record_kind}")
    print(f"Will delete {len(to_delete)} records (keep_notes_prefix={keep_prefix!r})")
    for rid in to_delete[:50]:
        print(f"- {rid}")
    if len(to_delete) > 50:
        print(f"... and {len(to_delete) - 50} more")

    if dry_run:
        print("DRY RUN: no changes made.")
        return 0

    removed = delete_records_by_ids(db_path, person_key=person_key, record_ids=to_delete)
    print(f"OK: deleted {removed} records.")
    return 0


def cmd_chat(args: argparse.Namespace) -> int:
    _ensure_repo_tools_shared_on_path()
    from llm_api import create_llm_client, query_llm  # type: ignore

    provider = args.provider or os.getenv("AI_ASSISTANT_CHAT_PROVIDER", "aibuilder")
    model: Optional[str] = args.model
    client = create_llm_client(provider)

    print("进入 chat。输入 /exit 退出。")
    while True:
        try:
            user = input("你: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not user:
            continue
        if user in ("/exit", "/quit"):
            return 0
        resp = query_llm(user, client=client, model=model, provider=provider)
        print(f"AI: {resp or '(no response)'}")


def run_cli(parser: argparse.ArgumentParser) -> int:
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-db", help="Init per-person SQLite DB")
    p_init.add_argument("--person", required=True, help="qianying/evelyn/lucas")
    p_init.set_defaults(func=cmd_init_db)

    p_imp = sub.add_parser("import-health-pdf", help="Import a health PDF into DB")
    p_imp.add_argument("--person", required=True)
    p_imp.add_argument("--pdf", required=True)
    p_imp.add_argument("--domain", default="health")
    p_imp.add_argument("--subdomain", default=None)
    p_imp.add_argument("--record-kind", default=None)
    p_imp.add_argument("--record-type", default=None, help="legacy alias of record-kind")
    p_imp.add_argument("--layer", default="raw")
    p_imp.add_argument("--source-system", default="pdf_export")
    p_imp.add_argument("--observed-at", default=None, help="ISO datetime/date; if omitted can be filled by extraction")
    p_imp.add_argument("--sensitivity-tier", default="A")
    p_imp.add_argument("--notes", default=None)
    p_imp.add_argument("--chunk-size", type=int, default=1200)
    p_imp.add_argument("--chunk-overlap", type=int, default=150)
    p_imp.add_argument("--source-kind", default=None, help="e.g. clinical_document (default inferred from domain)")
    p_imp.add_argument("--document-family", default=None, help="e.g. single_visit_note, yearly_longitudinal_summary")
    p_imp.add_argument("--extraction-intent", default=None, help="e.g. default, visits_only")
    p_imp.add_argument("--extraction-profile-id", default=None, help="Override catalog profile id if set")
    p_imp.set_defaults(func=cmd_import_health_pdf)

    p_imp_multi = sub.add_parser(
        "import-health-pdf-multi",
        help="Import ONE health PDF as MULTIPLE visit records (shared stored file). Provide --visit-dates or use --auto-visit-dates.",
    )
    p_imp_multi.add_argument("--person", required=True)
    p_imp_multi.add_argument("--pdf", required=True)
    p_imp_multi.add_argument("--domain", default="health")
    p_imp_multi.add_argument("--subdomain", default=None)
    p_imp_multi.add_argument("--record-kind", required=True)
    p_imp_multi.add_argument("--layer", default="raw")
    p_imp_multi.add_argument("--source-system", default="pdf_export")
    p_imp_multi.add_argument("--sensitivity-tier", default="A")
    p_imp_multi.add_argument("--notes", default=None)
    p_imp_multi.add_argument("--visit-dates", default="", help="Comma-separated dates, e.g. 2025-01-03,2025-03-22")
    p_imp_multi.add_argument("--auto-visit-dates", action="store_true", help="Extract dates from PDF text locally when --visit-dates is empty")
    p_imp_multi.add_argument("--include-all-visits", action="store_true", help="Do not apply date-window heuristic filtering when auto-extracting dates")
    p_imp_multi.add_argument("--extract-strategy", default="dates", choices=["dates", "encounters"], help="Auto-extract strategy: dates (whole doc) or encounters (Encounters section only)")
    p_imp_multi.add_argument("--visit-year", type=int, default=0, help="Required for --extract-strategy encounters, e.g. 2025")
    p_imp_multi.add_argument("--max-visits", type=int, default=0, help="If >0, keep only the most recent N extracted dates")
    p_imp_multi.add_argument("--dry-run", action="store_true", help="Only show extracted dates / would-import count, do not write DB")
    p_imp_multi.add_argument("--chunk-size", type=int, default=1200)
    p_imp_multi.add_argument("--chunk-overlap", type=int, default=150)
    p_imp_multi.add_argument("--source-kind", default=None)
    p_imp_multi.add_argument("--document-family", default=None)
    p_imp_multi.add_argument("--extraction-intent", default=None)
    p_imp_multi.add_argument("--extraction-profile-id", default=None)
    p_imp_multi.set_defaults(func=cmd_import_health_pdf_multi)

    p_list = sub.add_parser("list-records", help="List records")
    p_list.add_argument("--person", required=True)
    p_list.add_argument("--domain", default=None)
    p_list.add_argument("--subdomain", default=None)
    p_list.add_argument("--record-kind", default=None)
    p_list.add_argument("--limit", type=int, default=20)
    p_list.set_defaults(func=cmd_list_records)

    p_clean = sub.add_parser("cleanup-pdf-visits", help="Delete non-encounter records for a given PDF original_path")
    p_clean.add_argument("--person", required=True)
    p_clean.add_argument("--original-path", required=True)
    p_clean.add_argument("--record-kind", default="doctor_visit_notes")
    p_clean.add_argument("--keep-notes-prefix", default="[encounter]")
    p_clean.add_argument("--dry-run", action="store_true")
    p_clean.set_defaults(func=cmd_cleanup_pdf_visits)

    p_chat = sub.add_parser("chat", help="Chat with LLM (provider configurable)")
    p_chat.add_argument("--provider", default=None, choices=["openai", "anthropic", "gemini", "local", "deepseek", "azure", "siliconflow", "aibuilder"])
    p_chat.add_argument("--model", default=None)
    p_chat.set_defaults(func=cmd_chat)

    args = parser.parse_args()
    return int(args.func(args))
