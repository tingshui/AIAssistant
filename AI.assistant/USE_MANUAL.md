# AI.assistant 使用手册（速查）

## 启动 Web

在仓库根目录执行（推荐）：

```bash
PYTHONPATH=AI.assistant ./venv/bin/python3 -m uvicorn web_ui:app --host 127.0.0.1 --port 8765 --app-dir AI.assistant
```

然后浏览器打开 `http://127.0.0.1:8765/`。

## Web 重提取模式（默认 semantic）

- **`AI_ASSISTANT_WEB_REEXTRACT_MODE`**（默认 `semantic`）：LLM **只填** `visit_reason` / `symptoms` / `prescriptions` / `clinical_detail` / `vitals`；**患者名、医生、就诊日、机构**由程序决定（Encounters 行解析、PDF 前几页 demographics、Profile），不再让模型主导身份与日期。
- 需恢复旧行为（整段 JSON 全字段由模型填）：设为 `full` 并重启 uvicorn。
- **PDF 首页 demographics**：读前 `AI_ASSISTANT_DEMOGRAPHICS_MAX_PAGES` 页（默认 3）解析 Patient / PCP 等；stderr 可见 `[pdf_demographics]`。
- `semantic` 模式下**不注入** `[ENCOUNTER_ROW_ANCHOR]`（就诊日由行绑定直接写库）。

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

## 分层抽取定义（domain → source_kind → document_family → intent）

抽取时喂给模型的文本来自 `chunks`，但会按记录在库里的 **profile** 先筛选再拼接（见 `AI.assistant/config/extraction_profiles.json`）。

- 每条 `records` 会写入：`source_kind`、`document_family`、`extraction_intent`、`extraction_profile_id`（导入时自动推断，也可用 CLI 覆盖）。
- **年度汇总 + 仅就诊**：`import-health-pdf-multi` 使用 `--extract-strategy encounters` 时，会默认落到 `yearly_longitudinal_summary` / `visits_only`，并对 chunk 使用「Encounters–Assessments」区间策略（marker 写在 catalog 里，可改配置，不必改代码）。
- **单次就诊 PDF**：默认 `single_visit_note` / `all_chunks`。
- CLI 覆盖示例：`--document-family yearly_longitudinal_summary --extraction-intent visits_only --extraction-profile-id health.clinical_document.yearly_longitudinal_summary.visits_only`

重新提取前会在 stderr 打一行 `[get_record_text_for_extraction] ...`，便于确认当前选用的 profile 与策略。

## 试验功能：Encounters 行级绑定（修复「多条 record 重提取同一段文本」）

默认开启（可用 `AI_ASSISTANT_ENCOUNTER_ROW_BIND=0` 关闭）。

对 `yearly_longitudinal_summary` + `visits_only`：在抽取前把输入**收窄到与当前 `record_id` 对应的那一行 Encounters**（`salt` 与 `import-health-pdf-multi` 一致）。会先尝试 chunk 拼接文本；若无法匹配，会再读 **整份 PDF 文本** 做同一套解析以对齐导入时的 `idx`。

stderr 会打：`[structure_route] record_id=... encounter_row_bind=...`

绑定命中时（`encounter_row_bind=hit`）：会在送入抽取模型的文本前注入 `[ENCOUNTER_ROW_ANCHOR]`（已知该行的 `YYYY-MM-DD`）；若模型 JSON 里 `visit_date` 仍为空，Web「重新提取」在入库前会用该行日期**自动回填** `visit_date_extracted`（避免表格里「就诊日期」列大量空白）。

同一绑定命中下，程序还会对**该行原文**做轻量规则解析（机构 / 医生 / 诊断行合并进 `symptoms` 等），stderr 有 `[encounter_row_det] ...`；入库时在 LLM 结果之后**以解析结果覆盖**这些字段（`visit_reason` 仅在模型为空时用计费/visit 类型行兜底）。

**患者/医生名不稳定**：窄 Encounters 行常缺页眉，本地模型易把患者与医生弄混。可在 Profile 填 `legal_name`、`primary_doctor_name`（或在 `.env` 设 `AI_ASSISTANT_EXTRACT_PATIENT_NAME_QIANYING`、`AI_ASSISTANT_EXTRACT_DOCTOR_NAME_QIANYING`），重新提取时会写入 prompt，并在入库时**覆盖** `patient_name` / `doctor_name`。

## 试验功能：Yearly summary 的 visit_event spans 过滤（减少 vitals 日期污染）

默认关闭。开启后：对 profile 为 `yearly_longitudinal_summary` + `visits_only` 的记录，在喂给抽取模型前会先用 LLM 识别 **vitals 占用的行号区间**（只返回小 JSON：`vitals_ranges`），再从原文中**删除这些行**，降低 vitals 日期污染 encounter 的概率。

启动 Web 时加：

```bash
AI_ASSISTANT_VISITS_ONLY_SPAN_FILTER=1 PYTHONPATH=AI.assistant ./venv/bin/python3 -m uvicorn web_ui:app --host 127.0.0.1 --port 8767 --app-dir AI.assistant
```

可选（默认本地 Ollama）：

- `AI_ASSISTANT_VISITS_ONLY_SPAN_FILTER_PROVIDER=local`
- `AI_ASSISTANT_VISITS_ONLY_SPAN_FILTER_MODEL=llama3.2:latest`（不设则用 provider 默认）

开启后，stderr 会多打印一行 `[visit_event_filter] ... spans=... len=...->...` 便于确认过滤是否生效。

若已启用 **Encounters 行级绑定**（`encounter_row_bind=hit`），会自动 **跳过** 本 vitals 行过滤，避免把单行就诊里的日期行误删导致 `visit_date` 变空。

## 调试：终端打印抽取详情（含模型原始返回与 visit_date）

不设变量即关闭。需要时在**启动 uvicorn 的命令前**加环境变量（等价于「`--print-detail`」类开关；uvicorn 本身不接受自定义参数）：

```bash
AI_ASSISTANT_PRINT_EXTRACTION_DETAIL=1 PYTHONPATH=AI.assistant ./venv/bin/python3 -m uvicorn web_ui:app --host 127.0.0.1 --port 8765 --app-dir AI.assistant
```

随后在网页点「本地重新提取」：运行 uvicorn 的终端 **stderr** 会打印本轮 LLM 的原始字符串和解析后的 JSON（单独一行标出 `[parsed visit_date]`）。原始输出过长时默认最多约 80000 字符；要全文可设 `AI_ASSISTANT_PRINT_EXTRACTION_DETAIL_MAX_CHARS=0`。

## 本地 LLM（Ollama）

Web 端默认 provider=`local`，通常连 `http://127.0.0.1:11434/v1`。

常用环境变量（仓库根 `.env`）：

- `LOCAL_LLM_BASE_URL`：默认 `http://127.0.0.1:11434/v1`
- `LOCAL_LLM_MODEL`：例如 `llama3.2:latest`

如出现“连接失败/Connection refused”，请确认 Ollama 已启动且模型已拉取。

## 生日与就诊年龄（visit age）

- **生日已存入 SQLite 的 `profiles.birthdate`**（每人各自的 DB）。
- 现在 `.env` 里的 `AI_ASSISTANT_BIRTHDATE_*` 仅作为**迁移兜底来源**：如果某人的 `profiles.birthdate` 为空，系统会在初始化/启动时把 `.env` 的值写入 profile。
- **就诊年龄**用于 records 表格与生命体征横轴，计算方式：`profiles.birthdate` 与 `records.visit_date_extracted` 的差值（不会使用当前年龄）。
