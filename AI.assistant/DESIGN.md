# AI.assistant — Design Notes（WIP）

## 核心原则

- **Modular**：shared core + domain modules（health/travel…）
- **本地优先**：SQLite + 本地 raw 文件夹，UI 默认使用本地推理（Ollama / OpenAI-compatible endpoint）
- **证据 vs 解释分层**：raw evidence（records/chunks）与结构化画像（profiles）与动态记忆（memories）分开存
- **可追溯/可控**：任何抽取与总结应可回溯到来源记录；UI 操作显式触发

## 两层隔离（Family members）

- 物理隔离：`data/db/<person>.sqlite3` + `data/raw/<person>/...`
- 逻辑分类：`domain/subdomain/record_kind/layer/source_system/sensitivity_tier`

## 数据层（最低可行）

- **Raw evidence layer**：`records` + `chunks`（PDF 导入、文本抽取、chunk）
- **Structured profile layer**：`profiles`（birthdate/location/health_summary + travel preferences）
- **Dynamic memory layer**：`memories`（memory_type/confidence/status/domain/tier + source evidence）

## 关键 Lesson

- **就诊年龄必须使用 visit date - birthdate**（不要用当前年龄）。用于 vitals 图表与展示。

