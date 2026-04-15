from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

from .db import import_health_pdf, init_db, list_records
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
    )
    print(f"OK: imported record_id={res.record_id} stored_path={res.stored_path} chunks={res.num_chunks}")
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
        print(
            f"{r['record_id']}  {r['domain']}/{r['subdomain'] or '-'}  {r['record_kind']}  "
            f"obs={r['observed_at'] or '-'}  imp={r['imported_at']}  "
            f"err={'Y' if r['extraction_error'] else 'N'}"
        )
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
    p_imp.set_defaults(func=cmd_import_health_pdf)

    p_list = sub.add_parser("list-records", help="List records")
    p_list.add_argument("--person", required=True)
    p_list.add_argument("--domain", default=None)
    p_list.add_argument("--subdomain", default=None)
    p_list.add_argument("--record-kind", default=None)
    p_list.add_argument("--limit", type=int, default=20)
    p_list.set_defaults(func=cmd_list_records)

    p_chat = sub.add_parser("chat", help="Chat with LLM (provider configurable)")
    p_chat.add_argument("--provider", default=None, choices=["openai", "anthropic", "gemini", "local", "deepseek", "azure", "siliconflow", "aibuilder"])
    p_chat.add_argument("--model", default=None)
    p_chat.set_defaults(func=cmd_chat)

    args = parser.parse_args()
    return int(args.func(args))

