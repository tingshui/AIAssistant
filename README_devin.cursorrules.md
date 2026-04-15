# cursor-project


A Cursor-powered AI development environment with advanced agentic capabilities.


## Quick Start

1. Activate the virtual environment:
   ```bash
   # On Windows
   venv\Scripts\activate
   
   # On macOS/Linux
   source venv/bin/activate
   ```

2. Configure your environment:
   - Copy `.env.example` to `.env` if you haven't already
   - Add your API keys in `.env` (optional)

## Available Tools

Scripts live under **`tools/shared/`** (generic utilities). **Aha! Catcher** helpers live in **`ahacatcher/scripts/`**. See **`tools/README.md`** and **`ahacatcher/README.md`**.

### LLM Integration（`tools/shared/llm_api.py`）
```bash
venv/bin/python3 tools/shared/llm_api.py --prompt "Your question" --provider anthropic
```
To import in Python, add `tools/shared` to `PYTHONPATH` or `sys.path`, then `from llm_api import query_llm`.

### Web Scraping
```bash
venv/bin/python3 tools/shared/web_scraper.py --max-concurrent 3 https://example.com
```

### Search Engine
```bash
venv/bin/python3 tools/shared/search_engine.py "your search keywords"
```

### Screenshot Verification
```bash
venv/bin/python3 tools/shared/screenshot_utils.py https://example.com --output shot.png
```
Combine with `tools/shared/llm_api.py` and `--image` for vision prompts (see `.cursorrules`).

Note: When you first use the screenshot verification feature, Playwright browsers will be installed automatically.


## AI Assistant Configuration


This project uses `.cursorrules` to configure the AI assistant. The assistant can:
- Help with coding tasks
- Verify screenshots
- Perform web searches
- Analyze images and code


## Environment Variables

Configure these in your `.env` file:

- `LLM_API_KEY`: Your LLM API key (optional)
- `AZURE_OPENAI_API_KEY`: Azure OpenAI API key (optional)
- `AZURE_OPENAI_ENDPOINT`: Azure OpenAI endpoint (optional)
- `AZURE_OPENAI_MODEL_DEPLOYMENT`: Azure OpenAI model deployment name (optional)
- `SILICONFLOW_API_KEY`: Siliconflow API key (optional)
Note: Basic functionality works without API keys. Advanced features (like multimodal analysis) require appropriate API keys.

## Development Tools

- `.devcontainer/`: VS Code development container configuration
- `.vscode.example/`: Recommended VS Code settings
- `.github/`: CI/CD workflows

## License

MIT License