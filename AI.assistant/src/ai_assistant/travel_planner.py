from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class TravelPlannerInputs:
    destination: str
    start_date: str
    end_date: str
    travelers: str
    budget: str
    preferences: str
    include_health_constraints: bool


def build_travel_context(
    *,
    profile: Dict[str, Any],
    travel_memories: List[Dict[str, Any]],
    health_constraints: Optional[str],
) -> Dict[str, Any]:
    return {
        "profile": profile,
        "travel_memories": travel_memories,
        "health_constraints": health_constraints or "",
    }


def build_travel_planner_prompt(inputs: TravelPlannerInputs, bundle: Dict[str, Any]) -> str:
    return f"""You are a personal travel planner.
Use the context bundle (profile + memories + optional health constraints) to propose an itinerary.

Rules:
- Be concrete (days, activities, transit, reservations, packing).
- Respect preferences from memories.
- If health constraints are present, incorporate them (no medical advice; just planning constraints).
- Provide a short checklist at the end.

Trip:
- destination: {inputs.destination}
- start_date: {inputs.start_date}
- end_date: {inputs.end_date}
- travelers: {inputs.travelers}
- budget: {inputs.budget}
- preferences: {inputs.preferences}
- include_health_constraints: {inputs.include_health_constraints}

Context bundle (JSON):
{bundle}
"""


def run_travel_planner(
    *,
    inputs: TravelPlannerInputs,
    bundle: Dict[str, Any],
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    # late import to keep module light
    from pathlib import Path
    import sys

    project_root = Path(__file__).resolve().parents[3]
    shared = project_root / "tools" / "shared"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))
    from llm_api import create_llm_client, query_llm  # type: ignore

    prov = provider or os.getenv("AI_ASSISTANT_CHAT_PROVIDER", "aibuilder")
    prompt = build_travel_planner_prompt(inputs, bundle)
    client = create_llm_client(prov)
    raw = query_llm(prompt, client=client, model=model, provider=prov)
    if raw is None or not str(raw).strip():
        raise RuntimeError("Travel planner LLM failed (empty response).")
    return str(raw).strip()


def bundle_debug_text(bundle: Dict[str, Any]) -> Tuple[str, int]:
    import json

    s = json.dumps(bundle, ensure_ascii=False, indent=2)
    return s, len(s)

