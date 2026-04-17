# 复杂 PDF、信息污染与抽取演进（中文详述）

> 英文短题：**Complex PDFs, information pollution, and program-first extraction** — 便于与 `project_definition`、issue 标题对齐。

本文按**时间顺序与假设—验证—修正**写法，记录我们在「年度临床总结 PDF → 本地 SQLite → Web 展示 / LLM 重提取」链路上遇到的主要问题：**根因是什么、我们具体做了什么尝试、如何验证假设、假设被推翻后下一假设是什么**。技术细节以当前 `AI.assistant` 实现为准。

---

## 0. 背景：我们在处理什么样的 PDF？

- **文档形态**：多页、多章节（Problems、Allergies、Medications、**Encounters**、Vitals、Assessments 等），同一页或相邻页上存在**大量日期**，语义角色不同（测量时间、开药日期、历史问题日期、**本次就诊日期**等）。
- **系统目标**：把「与某次就诊相关」的字段写进 `records`（如 `visit_date_extracted`、`symptoms` 等），并在 Web 列表中可读。
- **早期约束**：优先 **本地 LLM（Ollama）** 做抽取，成本与隐私友好，但小模型在**严格 JSON 填槽**上弱于「自由摘要」。

这类文档的核心难点不是「没有文本」，而是**同一窗口内多类信息并存**，若把「找边界 + 认字段 + 填表」全交给模型，极易发生**信息污染**（把 A 类日期写进 B 类字段）或**槽位不稳定**（证据里有日期串，`visit_date` 仍为空）。

---

## 1. 问题一：界面里「就诊日期」不对或为空，但「时间」一列往往有日期

### 1.1 表面现象（可观察事实）

- 列表里 **时间**（通常与 `observed_at` 或展示列相关）整列较完整。
- **就诊日期**（`visit_date_extracted`）多行为空，或与本次就诊不一致。
- 用户直觉：**「PDF 里明明有日期，为什么程序/模型没写进就诊日？」**

### 1.2 第一组根因假设

| 假设 | 含义 |
|------|------|
| **H1** | 模型「没看见」日期。 |
| **H2** | 模型看见了，但**把别类日期**（如 vitals）当成了就诊日。 |
| **H3** | 入库逻辑把 `visit_date` 映射错了，或展示列绑错了字段。 |

### 1.3 我们如何验证

1. **对照 DB**：用 `list-records` / SQLite 看 `visit_date_extracted` 与 `observed_at` 是否一致、是否为空。
2. **打开 `AI_ASSISTANT_PRINT_EXTRACTION_DETAIL=1`**：同一轮抽取在 stderr 打出 **原始 JSON** 与 **`[parsed visit_date]`**。
3. **对照 PDF 文本**：Encounters 行首 `MM/DD/YYYY` 与 JSON 里日期是否同类。

### 1.4 结论（H1–H3 哪些成立）

- **H3** 部分成立过（展示/字段语义要分清），但**不是主因**。
- **H2 强成立**：长总结里 **vitals 段落的日期**与 **encounter 行日期** 同窗口出现时，模型容易把「离它近」或「格式像」的日期填进 `visit_date`。
- **H1 常被推翻**：日志里可出现「证据句里已有 `12/09/2025`」，但 **`visit_date` 仍为空** —— 说明不全是「没看见」，而是**填槽失败 / 角色混乱**。

**下一假设方向**：不能只靠 prompt 说「别混」；需要 **结构层** 先把「本次就诊」对应的文本范围缩小到 encounter 语义单元。

---

## 2. 问题二：同一人多条就诊记录，重提取后「八条结果几乎一样」

### 2.1 表面现象

- 数据库里 8 条 `record` 对应同一份 PDF 的 8 次就诊，但点「全部重提取」后，多条记录的抽取结果**高度雷同**（像对同一段文字抽了八遍）。

### 2.2 根因假设

| 假设 | 含义 |
|------|------|
| **H4** | 模型偷懒，复制粘贴。 |
| **H5** | **喂给模型的输入文本对多条 record 实际是相同的**（上游选 chunk 策略问题）。 |

### 2.3 如何验证 H4 vs H5

- 在 stderr 增加 **`[get_record_text_for_extraction]`** 日志：打印 `record_id`、`picked_chunks`、策略名。
- 若 **不同 `record_id` 的 `picked_chunks` 与拼接 span 完全一致**，则 **H5 成立**，H4 只是表象。
- 实际验证：**八条记录共享同一段 Encounters 区间文本** → 模型当然输出相似。

### 2.4 结论与修复思路

- **主因是 H5**：问题在**数据管线**（record → 文本），不在「多抽几次就会好」。
- **修复**：与导入时一致的 **Encounters 行 salt**（`visit_date|time|type|idx`）把 **`record_id` 绑定到唯一一行**，并把抽取输入**收窄到该行块**；chunk 对不齐时 **回读整份 PDF 文本** 再匹配 salt（避免 chunk 索引与全文索引漂移）。

**如何验证修复生效**：日志出现 **`encounter_row_bind=hit`** 且 **`len=3604->181`** 这类「变短且每条不同」；八条 JSON 开始分化。

---

## 3. 问题三：已绑定到「单行 encounter」，就诊日仍错或仍被 vitals 干扰

### 3.1 根因假设

| 假设 | 含义 |
|------|------|
| **H6** | 绑定行内仍夹有 vitals 行，模型继续误用 vitals 日期。 |
| **H7** | 绑定行已很干净，模型仍填错 —— 纯模型能力问题。 |

### 3.2 具体尝试（H6）

- 增加 **visit-only 过滤**：用本地模型输出 **vitals 行号区间**，从原文删掉这些行再送抽取（`visit_event_filter` / `AI_ASSISTANT_VISITS_ONLY_SPAN_FILTER`）。

### 3.3 如何验证 H6

- 对比过滤前后 **stderr 长度**、以及 `visit_date` 是否仍被 vitals 日期带偏。
- **反例**：绑定后输入已是**极短单行块**时，过滤可能 **误删日期行** → `visit_date` 更空。
- 处理：**`encounter_row_bind=hit` 时跳过 span 过滤**（避免「帮倒忙」）。

### 3.4 结论

- **H6 在「大块混杂文本」上可能成立**；在**已绑定单行**场景下，过滤收益下降、风险上升。
- **H7 在本地小模型上成立**：即使文本短，**严格 JSON 的 `visit_date` 仍可能空**。

**下一假设**：既然程序在绑定阶段已经知道 **`YYYY-MM-DD`**，就不应再让 LLM 成为就诊日的**唯一写入者** → 引入 **程序回填** 与（当时的）**锚定提示**。

---

## 4. 问题四：日志里 `visit_date='2025-12-09'`，JSON 里仍是 `"visit_date": ""`

### 4.1 这一步推翻了什么？

- 彻底推翻 **「程序不知道日期」**：`[structure_route]` 已打印 **`encounter_row_bind=hit ... visit_date='...'`**。
- 也削弱 **「只要再调 prompt 就会稳定抄进 JSON」**：同一轮里 **`[parsed visit_date] ''`** 仍可出现。

### 4.2 根因（更精确）

- **根因**：本地模型在「**窄表格残片 → 严格 JSON 字段**」映射上**不稳定**；`visit_date` 与 `visit_date_evidence` 的职责在模型输出里**漂移**（证据像日期，主字段空；或把 billing/说明句当证据）。

### 4.3 具体工程措施与验证

1. **锚定块**（`[ENCOUNTER_ROW_ANCHOR]`）：明示已知 ISO 日期，要求写入 JSON（并声明 META 勿抄）。
   - **验证**：看 `visit_date` 是否仍大量为空；看 `visit_date_evidence` 是否误抄指令句。
2. **入库前回填**：若绑定 ISO 存在且 JSON 仍空，**用绑定日期写入**再 `update_record_from_extraction`。
   - **验证**：Web 列表「就诊日期」列与 `structure_route` 是否一致。

### 4.4 锚定带来的副作用（新观察）

- 小模型仍可能把 **「You MUST set JSON…」** 抄进 `visit_date_evidence`。
- **对策**：标注 **META**；在 **`semantic` 重提取模式**下 **不再注入锚定块**（就诊日完全不靠 LLM 字段）。

---

## 5. 问题五：患者名、医生名、诊断「飘」— 与日期问题不同类，但同源

### 5.1 根因假设

| 假设 | 含义 |
|------|------|
| **H8** | 行块里缺少首页 demographics，模型乱猜患者/医生。 |
| **H9** | 模型把 **provider 与 patient** 在短行里弄反。 |
| **H10** | 诊断多条，模型倾向压缩成一句。 |

### 5.2 验证方法

- **PRINT_DETAIL** 看 `patient_name` / `doctor_name` / `symptoms` 与行内原文谁对齐。
- Profile 增加 **`legal_name` / `primary_doctor_name`** 或 env 覆盖，看列表是否稳定。

### 5.3 结论与演进

- **H8–H10 均部分成立**。
- **措施链**：
  1. **Prompt 强化**角色与「诊断尽量列全」；
  2. **Profile/env 权威姓名**在入库前覆盖；
  3. **`parse_encounter_row_deterministic`**：对绑定行块做规则解析，**程序优先写入**机构、行内医生、多行诊断文本；
  4. **`parse_demographics_from_text` + 前几页 PDF**：补 **患者名 / PCP**（仅填空）；
  5. **`AI_ASSISTANT_WEB_REEXTRACT_MODE=semantic`**：`normalize_semantic_extraction` **清空** patient/doctor/date/facility，让 LLM **无法主导**这些槽位。

**验证**：对比 merge 前后列；stderr 看 `[pdf_demographics]`、`[encounter_row_det]`。

---

## 6. 当前架构：谁对什么负责（汇总表）

| 信息类型 | 主责任方 | 典型手段 | 如何快速验证 |
|----------|----------|----------|----------------|
| 本次就诊日期 | **程序** | 行绑定 +（`full` 模式下锚定/回填） | `[structure_route]` 与 DB `visit_date_extracted` |
| 行内机构/医生/诊断原文 | **程序** | `parse_encounter_row_deterministic` + merge | `[encounter_row_det]` |
| 首页患者 / PCP | **程序** | 前 N 页 + `parse_demographics_from_text` | `[pdf_demographics]` |
| 法定展示名 / 固定主治 | **配置** | Profile / env + `apply_canonical_identities` | Profile 保存后重提 |
| 就诊原因、长叙述、处方叙述、vitals 语义 | **LLM（默认 semantic）** | `build_semantic_extraction_prompt` | PRINT_DETAIL 中 JSON 仅含允许字段 |

---

## 7. 我们踩过的坑（简短「排雷」）

1. **把 LLM JSON 当真相**：路由已知日期仍可能入库空字符串。
2. **以为「分区了就结束了」**：未解决「多 record 同输入」。
3. **锚定提示被模型当正文抄进 evidence**：需 META 标注 + `semantic` 不注入锚定。
4. **单行仍开 span 过滤**：可能删掉日期行 → 绑定命中后应跳过过滤。
5. **把「输入变短」等同「变简单」**：表格残片对「填槽」更难。

---

## 8. 插图与日志样例（自愿、脱敏）

仓库 **`docs/assets/`** 说明见 `README.md`。不在仓库内强制附带患者截图；若自行配图，请脱敏。

**终端对照示例（结构匿名）：**

```text
[structure_route] record_id=... encounter_row_bind=hit ... visit_date='2025-12-09' ...
[parsed visit_date] ''
```

---

## 9. 一句话原则

**「找」边界、行、列、重复结构 —— 交给程序与解析器；「说」理由、归纳、面向人读的表述 —— 再交给 LLM。不要让 LLM 独自承担从粗糙 PDF 文本到强类型字段的全链路。**

---

## 10. 相关操作入口（便于复现）

- 调试 JSON：`AI_ASSISTANT_PRINT_EXTRACTION_DETAIL=1`（见 `USE_MANUAL.md`）。
- 重提取模式：`AI_ASSISTANT_WEB_REEXTRACT_MODE=semantic`（默认）或 `full`。
- demographics 页数：`AI_ASSISTANT_DEMOGRAPHICS_MAX_PAGES`。

---

## Document history

| 日期 | 说明 |
|------|------|
| 2026-04-16 | 初版（中英混排）。 |
| 2026-04-16 | 删除仓库内 UI 截图文件，文档以文字为主。 |
| 2026-04-16 | **扩写为中文详述**：按根因—尝试—验证—修正链条重写主干。 |
