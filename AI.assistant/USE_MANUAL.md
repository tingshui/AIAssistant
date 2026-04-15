# AI.assistant 使用手册（速查）

## 启动 Web

在仓库根目录执行（推荐）：

```bash
PYTHONPATH=AI.assistant ./venv/bin/python3 -m uvicorn web_ui:app --host 127.0.0.1 --port 8765 --app-dir AI.assistant
```

然后浏览器打开 `http://127.0.0.1:8765/`。

## 常用页面

- `/`：选择成员
- `/person/<person>/records`：记录列表（有 domain tabs）
- `/person/<person>/profile`：Profile（可编辑 birthdate/location/health_summary；health_summary 支持“从本地文件自动生成”预览）
- `/person/<person>/memories`：Memories（含 Travel Preferences 可编辑；下方是动态记忆列表）
- `/person/<person>/vitals`：生命体征图（横轴使用**就诊年龄**）
- `/person/<person>/travel-planner`：Travel Planner demo（可选 include_health_constraints + debug bundle）

## 记录页按钮说明

进入某成员记录列表后：

- **本地提取新文件**：扫描 `AI.assistant/data/raw/<person>/` 下新增 PDF 并导入 DB（去重基于文件 SHA256）。
- **同步磁盘**：删除 DB 中 `stored_path` 已不存在的记录（解决“删了文件但网页还在”的问题）。
- **本地重新提取全部**（health tab）：对该成员全部记录做本地 LLM 抽取，填充 domain/subdomain + 临床字段 + vitals_json。
- **本地重新提取本条**（详情页）：仅对单条记录抽取。

## 本地 LLM（Ollama）

Web 端默认 provider=`local`，通常连 `http://127.0.0.1:11434/v1`。

常用环境变量（仓库根 `.env`）：

- `LOCAL_LLM_BASE_URL`：默认 `http://127.0.0.1:11434/v1`
- `LOCAL_LLM_MODEL`：例如 `llama3.2`

如出现“连接失败/Connection refused”，请确认 Ollama 已启动且模型已拉取。

## 生日与就诊年龄（visit age）

- **生日已存入 SQLite 的 `profiles.birthdate`**（每人各自的 DB）。  
- 现在 `.env` 里的 `AI_ASSISTANT_BIRTHDATE_*` 仅作为**迁移兜底来源**：如果某人的 `profiles.birthdate` 为空，系统会在初始化/启动时把 `.env` 的值写入 profile。
- **就诊年龄**用于 records 表格与生命体征横轴，计算方式：`profiles.birthdate` 与 `records.visit_date_extracted` 的差值（不会使用当前年龄）。

