#!/usr/bin/env python3
"""
使用 OpenAI SDK 调用本地的 Chat 代理（转发到 AI Builder GPT-5）
"""
from openai import OpenAI

# 指向本地代理，与 OpenAI API 兼容
client = OpenAI(
    base_url="http://127.0.0.1:8000/v1",
    api_key="dummy",  # 代理使用 .env 中的 AI_BUILDER_API_KEY
)

response = client.chat.completions.create(
    model="gpt-5",
    messages=[{"role": "user", "content": "用一句话介绍你自己"}],
    max_tokens=500,
)

print("回复:", response.choices[0].message.content)
