#!/usr/bin/env python3
from __future__ import annotations

import argparse

from src.ai_assistant.cli import run_cli


def main() -> int:
    parser = argparse.ArgumentParser(prog="ai-assistant", description="AI.assistant CLI")
    return run_cli(parser)


if __name__ == "__main__":
    raise SystemExit(main())

