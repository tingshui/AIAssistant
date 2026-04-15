from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .subjects import normalize_person_key


@dataclass(frozen=True)
class AssistantPaths:
    project_root: Path
    person: str
    person_key: str
    data_dir: Path
    db_dir: Path
    db_path: Path
    raw_dir: Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]  # AI.assistant/


def legacy_db_path() -> Path:
    return project_root() / "data" / "assistant.sqlite3"


def get_paths_for_person(person: str) -> AssistantPaths:
    """
    Two-layer storage layout:
    - Layer 1: per-person sqlite + per-person raw root
    - Layer 2: logical taxonomy (domain/subdomain/record_kind/layer/source_system)
    """
    person_key = normalize_person_key(person)
    root = project_root()
    data_dir = root / "data"
    db_dir = data_dir / "db"
    db_path = db_dir / f"{person_key}.sqlite3"
    raw_dir = data_dir / "raw" / person_key
    return AssistantPaths(
        project_root=root,
        person=person,
        person_key=person_key,
        data_dir=data_dir,
        db_dir=db_dir,
        db_path=db_path,
        raw_dir=raw_dir,
    )

