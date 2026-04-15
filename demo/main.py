#!/usr/bin/env python3
"""
FastAPI 网站 - Hello API + Chat 代理（Agentic）
- Hello: 接受用户输入的名字，返回 "Hello, {name}"
- Chat: Agentic loop，LLM 可决定是否调用 search 工具，仅一轮后强制生成最终回复
"""

import asyncio
import json
import os
import sys
from typing import Any, List, Optional

import httpx
from dotenv import load_dotenv
from pathlib import Path

from fastapi import Body, FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

load_dotenv()


# OpenAI 兼容的 Chat Completion 请求模型（用于 Swagger 文档可编辑）
class ChatMessage(BaseModel):
    role: str = Field(..., description="system | user | assistant | tool")
    content: Any = Field(None, description="消息内容，可为字符串或 multimodal 数组")


class ChatCompletionRequest(BaseModel):
    model: str = Field("gpt-5", description="模型: gpt-5, deepseek, supermind-agent-v1 等")
    messages: list[ChatMessage] = Field(..., description="对话消息列表")
    max_tokens: Optional[int] = Field(1000, description="最大生成 token 数")
    temperature: Optional[float] = Field(None, description="采样温度 0-2")
    stream: bool = Field(False, description="是否流式返回")

    model_config = {
        "extra": "allow",  # 允许额外字段，完整转发
        "json_schema_extra": {
            "example": {
                "model": "gpt-5",
                "messages": [{"role": "user", "content": "你好，用一句话介绍你自己"}],
                "max_tokens": 500,
            }
        },
    }

AI_BUILDER_BASE = os.getenv("AI_BUILDER_BASE_URL", "https://space.ai-builders.com/backend")
AI_BUILDER_KEY = os.getenv("AI_BUILDER_API_KEY", "")

# Search 工具定义（OpenAI function calling 格式）
SEARCH_TOOL = {
    "type": "function",
    "function": {
        "name": "search",
        "description": "在互联网上搜索信息。当用户询问需要实时或最新信息的问题时使用此工具。",
        "parameters": {
            "type": "object",
            "properties": {
                "keywords": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "搜索关键词列表",
                },
                "max_results": {
                    "type": "integer",
                    "description": "每个关键词返回的最大结果数",
                    "default": 6,
                },
            },
            "required": ["keywords"],
        },
    },
}


async def _call_ai_builder_chat(
    client: httpx.AsyncClient,
    messages: List[dict],
    tools: Optional[List[dict]] = None,
    tool_choice: Optional[str] = None,
    model: str = "gpt-5",
    max_tokens: int = 1000,
) -> dict:
    """调用 AI Builder Chat API"""
    url = f"{AI_BUILDER_BASE.rstrip('/')}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {AI_BUILDER_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
    }
    if tools is not None:
        payload["tools"] = tools
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice
    resp = await client.post(url, json=payload, headers=headers)
    return resp.json()


async def _execute_search(keywords: List[str], max_results: int = 6) -> dict:
    """执行搜索并返回结果（供 LLM 使用）"""
    url = f"{AI_BUILDER_BASE.rstrip('/')}/v1/search/"
    headers = {
        "Authorization": f"Bearer {AI_BUILDER_KEY}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=60.0) as c:
        resp = await c.post(
            url,
            json={"keywords": keywords, "max_results": max_results},
            headers=headers,
        )
    return resp.json()

DEMO_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="Hello API",
    description="""
## 简介
这是一个简单的问候 API，接受用户输入的名字，返回 `Hello, {name}` 格式的问候语。

## 调用方式
- **GET** 请求，通过 URL 查询参数 `name` 传入名字
- 示例：`GET /hello?name=张三` → 返回 `{"message": "Hello, 张三"}`

## 使用场景
- 测试 API 连通性
- 学习 FastAPI 基础用法
- 作为微服务的健康检查示例
    """,
    version="1.0.0",
    openapi_tags=[
        {"name": "问候", "description": "Hello 相关接口"},
        {"name": "Chat", "description": "Chat Completion 代理，与 OpenAI SDK 兼容"},
        {"name": "搜索", "description": "AI Builder Tavily 搜索代理"},
        {"name": "系统", "description": "系统信息接口"},
    ],
)


@app.get("/chat", include_in_schema=False)
def chat_page():
    """Chat GUI 页面"""
    return FileResponse(DEMO_DIR / "static" / "chat.html")


@app.get(
    "/hello",
    tags=["问候"],
    summary="获取问候语",
    description="""
根据传入的名字返回问候语。

**参数说明：**
- `name`（必填）：用户的名字，支持中文、英文等任意字符

**返回格式：**
```json
{"message": "Hello, {你输入的名字}"}
```

**调用示例：**
- `GET /hello?name=World` → `{"message": "Hello, World"}`
- `GET /hello?name=小明` → `{"message": "Hello, 小明"}`
    """,
    responses={
        200: {
            "description": "成功返回问候语",
            "content": {
                "application/json": {
                    "examples": {
                        "英文名": {"value": {"message": "Hello, World"}},
                        "中文名": {"value": {"message": "Hello, 小明"}},
                    }
                }
            },
        },
    },
)
def hello(
    name: str = Query(
        ...,
        description="用户的名字，支持中文、英文等任意字符",
        examples={"英文名": {"value": "World"}, "中文名": {"value": "小明"}},
    )
):
    """Hello API: 接受名字，返回问候语"""
    return {"message": f"Hello, {name}"}


@app.get(
    "/",
    tags=["系统"],
    summary="API 信息",
    description="返回 API 的基本信息和可用接口列表，用于快速了解如何调用本服务。",
    responses={
        200: {
            "description": "API 信息",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Welcome to Hello API",
                        "docs": "/docs",
                        "hello": "/hello?name=YourName",
                    }
                }
            },
        },
    },
)
def root():
    """根路径，返回 API 信息"""
    return {
        "message": "Welcome to Hello API",
        "docs": "/docs",
        "chat_gui": "/chat",
        "hello": "/hello?name=YourName",
        "chat": "POST /v1/chat/completions (OpenAI SDK 兼容)",
        "search": "POST /search (AI Builder Tavily 搜索)",
    }


class SearchRequest(BaseModel):
    """AI Builder 搜索请求：keywords 必填，max_results 可选"""
    keywords: List[str] = Field(..., min_length=1, description="搜索关键词列表")
    max_results: Optional[int] = Field(6, ge=1, le=20, description="每个关键词返回的最大结果数")


@app.post(
    "/search",
    tags=["搜索"],
    summary="Web 搜索（AI Builder Tavily）",
    description="""
调用 AI Builder Space 的 Tavily 搜索 API，支持多关键词并发搜索。

**请求格式：**
- `keywords`（必填）：搜索关键词列表，至少 1 个
- `max_results`（可选）：每个关键词返回的最大结果数，默认 6，范围 1-20

**返回：**
- `queries`：每个关键词的详细搜索结果（title, URL, content, score 等）
- `combined_answer`：Tavily 返回的综合摘要（如有）
- `errors`：失败关键词的错误信息（如有）
    """,
    responses={200: {"description": "Tavily 搜索结果"}},
)
async def search(body: SearchRequest = Body(..., example={"keywords": ["OpenAI GPT-5"], "max_results": 6})) -> Any:
    """转发到 AI Builder /v1/search/ API"""
    if not AI_BUILDER_KEY:
        return JSONResponse(
            status_code=500,
            content={"error": "AI_BUILDER_API_KEY 未配置，请在 .env 中设置"},
        )
    url = f"{AI_BUILDER_BASE.rstrip('/')}/v1/search/"
    headers = {
        "Authorization": f"Bearer {AI_BUILDER_KEY}",
        "Content-Type": "application/json",
    }
    payload = body.model_dump(exclude_none=True)
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
    try:
        data = resp.json()
    except Exception:
        data = {"error": resp.text or "Unknown error"}
    if resp.status_code != 200:
        return JSONResponse(status_code=resp.status_code, content=data)
    return data


MAX_TOOL_ROUNDS = 2  # 第 1、2 轮可带工具，第 3 轮强制无工具


async def _execute_single_tool(tc: dict, round_num: int, idx: int) -> dict:
    """执行单个 tool call，返回 tool 消息"""
    name = tc.get("function", {}).get("name", "")
    args_str = tc.get("function", {}).get("arguments", "{}")
    tc_id = tc.get("id", "")
    try:
        args = json.loads(args_str)
    except json.JSONDecodeError:
        args = {}
    print(f"[Agentic] 第 {round_num} 轮 工具 #{idx+1}: {name}", file=sys.stderr)
    print(f"[Agentic]   参数: {args}", file=sys.stderr)
    if name == "search":
        kw = args.get("keywords", [])
        mr = args.get("max_results", 6)
        result = await _execute_search(kw, mr)
        tool_content = json.dumps(result, ensure_ascii=False)
        qs = result.get("queries", [])
        summary = f"queries={len(qs)}条"
        first_content = ""
        if qs:
            first = qs[0].get("response", {}).get("results", [])
            summary += f", 首条{len(first)}个结果"
            if first:
                first_content = str(first[0].get("content", ""))[:200]
        print(f"[Agentic]   结果: {summary}", file=sys.stderr)
        if first_content:
            print(f"[Agentic]   首条摘要: {first_content}...", file=sys.stderr)
    else:
        tool_content = json.dumps({"error": f"Unknown tool: {name}"})
        print(f"[Agentic]   结果: 未知工具", file=sys.stderr)
    return {"role": "tool", "tool_call_id": tc_id, "content": tool_content}


async def _execute_tool_calls(tool_calls: list, round_num: int) -> list:
    """并行执行所有 tool_calls，返回要追加的 tool 消息列表（保持顺序）"""
    tasks = [_execute_single_tool(tc, round_num, i) for i, tc in enumerate(tool_calls)]
    tool_messages = await asyncio.gather(*tasks)
    return list(tool_messages)


@app.post(
    "/v1/chat/completions",
    tags=["Chat"],
    summary="Chat Completion（Agentic，OpenAI 兼容）",
    description="""
Agentic loop：LLM 可决定是否调用 search 工具，最多 2 轮工具调用后强制生成最终回复。

**流程：**
- 最多三轮：第 1、2 轮 LLM 可调用 search 工具，若调用则执行并继续
- 第 3 轮不再提供工具，强制生成最终文本回复

**与 OpenAI SDK 兼容**，可直接用 OpenAI 客户端调用。
    """,
    responses={
        200: {"description": "OpenAI 格式的 Chat Completion 响应"},
    },
)
async def chat_completions(
    body: ChatCompletionRequest = Body(
        ...,
        example={
            "model": "gpt-5",
            "messages": [{"role": "user", "content": "GPT-5 是什么？有什么新特性？"}],
            "max_tokens": 1000,
        },
    ),
) -> Any:
    """Agentic Chat：最多 2 轮工具调用，第 3 轮强制最终回复"""
    if not AI_BUILDER_KEY:
        return JSONResponse(
            status_code=500,
            content={"error": "AI_BUILDER_API_KEY 未配置，请在 .env 中设置"},
        )
    payload = body.model_dump(exclude_none=True)
    messages = list(payload["messages"])
    model = payload.get("model", "gpt-5")
    max_tokens = payload.get("max_tokens", 1000)

    async with httpx.AsyncClient(timeout=180.0) as client:
        tool_round = 0
        while True:
            # 前 2 轮带工具，第 3 轮及以后不带
            use_tools = tool_round < MAX_TOOL_ROUNDS
            print(f"[Agentic] 第 {tool_round + 1} 轮, use_tools={use_tools}", file=sys.stderr)
            resp = await _call_ai_builder_chat(
                client,
                messages=messages,
                tools=[SEARCH_TOOL] if use_tools else None,
                tool_choice="auto" if use_tools else None,
                model=model,
                max_tokens=max_tokens,
            )

            if "error" in resp:
                return JSONResponse(status_code=500, content=resp)

            choice = resp.get("choices", [{}])[0]
            msg = choice.get("message", {})
            tool_calls = msg.get("tool_calls")

            if not tool_calls:
                print(f"[Agentic] 无 tool_calls，返回最终回复", file=sys.stderr)
                return resp

            # 执行工具调用
            print(f"[Agentic] LLM 请求 {len(tool_calls)} 个工具调用", file=sys.stderr)
            messages = messages + [msg]
            tool_messages = await _execute_tool_calls(tool_calls, round_num=tool_round + 1)
            messages = messages + tool_messages
            tool_round += 1

            # 若已达最大工具轮次，下一轮强制无工具（已在 use_tools 中体现）
