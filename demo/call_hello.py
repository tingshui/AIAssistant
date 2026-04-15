#!/usr/bin/env python3
"""
调用 Hello 接口，查看返回结果
"""
import json
import urllib.parse
import urllib.request

BASE_URL = "http://127.0.0.1:8000"


def call_hello(name: str = "QQ") -> dict:
    """调用 /hello 接口，返回解析后的 JSON"""
    url = f"{BASE_URL}/hello?name={urllib.parse.quote(name)}"
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode())


if __name__ == "__main__":
    result = call_hello("QQ")
    print("Hello 接口返回:", json.dumps(result, ensure_ascii=False, indent=2))
