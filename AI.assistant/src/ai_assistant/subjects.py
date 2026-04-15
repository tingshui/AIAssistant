from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class Person:
    key: str
    aliases: tuple[str, ...]


# Canonical person keys are lowercase filesystem-friendly identifiers.
KNOWN_PEOPLE: tuple[Person, ...] = (
    Person(key="qianying", aliases=("qianying", "qy")),
    Person(key="evelyn", aliases=("evelyn",)),
    Person(key="lucas", aliases=("lucas",)),
)


def _alias_map() -> Dict[str, str]:
    m: Dict[str, str] = {}
    for p in KNOWN_PEOPLE:
        for a in p.aliases:
            m[a.lower()] = p.key
        m[p.key.lower()] = p.key
    return m


_ALIAS_TO_KEY = _alias_map()


def normalize_person_key(person: str) -> str:
    if not person:
        raise ValueError("person is required")
    key = person.strip().lower()
    if key not in _ALIAS_TO_KEY:
        allowed = ", ".join(sorted({p.key for p in KNOWN_PEOPLE}))
        raise ValueError(f"Unknown person '{person}'. Known people: {allowed}")
    return _ALIAS_TO_KEY[key]


def known_person_keys() -> tuple[str, ...]:
    return tuple(p.key for p in KNOWN_PEOPLE)

