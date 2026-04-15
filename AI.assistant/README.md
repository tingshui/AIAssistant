# AI.assistant

本项目是一个 **local-first** 的个人 AI 助理（先做 health 模块，其它模块可扩展），数据默认落在本地 SQLite + 本地 raw 文件夹中。

## Web UI 启动

在仓库根目录执行：

```bash
PYTHONPATH=AI.assistant ./venv/bin/python3 -m uvicorn web_ui:app --host 127.0.0.1 --port 8765 --app-dir AI.assistant
```

浏览器打开：

- `http://127.0.0.1:8765/`

UI 使用 Bootstrap 5.3 CDN。

## CLI（导入/初始化）

```bash
./venv/bin/python3 AI.assistant/main.py init-db --person evelyn
./venv/bin/python3 AI.assistant/main.py import-health-pdf --person evelyn --pdf "/path/to/file.pdf" --domain health --record-kind doctor_visit_summary
./venv/bin/python3 AI.assistant/main.py list-records --person evelyn --limit 20
```

## 数据目录

- `AI.assistant/data/db/<person>.sqlite3`
- `AI.assistant/data/raw/<person>/...`

## 速查

见 `AI.assistant/USE_MANUAL.md`。

