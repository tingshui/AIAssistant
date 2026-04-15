# Security & Privacy (cursor-project)

## Principles

- **Never commit secrets**: API keys, tokens, passwords, cookies, private URLs.
- **Never commit private data**: health PDFs, raw files, SQLite databases, audio/video.

## What must stay out of git

- `.env`, `.env.local`
- `AI.assistant/data/` (SQLite + raw evidence PDFs)
- `AI.assistant/.tmp_smoke/`
- `venv/`

## Recommended workflow

1. Use local git commits frequently.
2. Push to GitHub **code only** (secrets/data ignored).
3. Back up private data separately via Time Machine / encrypted archive / iCloud Drive.

## Pre-commit secret scan

This repo uses `pre-commit` + `detect-secrets` to prevent accidental secret commits.

Setup (from repo root):

```bash
./venv/bin/python3 -m pip install pre-commit detect-secrets
pre-commit install
detect-secrets scan --exclude-files '^(venv/|AI\\.assistant/data/|AI\\.assistant/\\.tmp_smoke/|\\.cursor/)' > .secrets.baseline
```

