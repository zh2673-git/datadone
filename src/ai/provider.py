#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""LLM 接入层 - 统一封装 OpenAI / Ollama / Mock 等 Provider。"""

import json
import logging
import os
import re
from typing import Optional

from src.ai.config import AIConfig
from src.ai.models import GenerationResult

logger = logging.getLogger(__name__)


class LLMProvider:
    """LLM Provider 抽象基类。"""

    def __init__(self, config: AIConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    def from_config(cls, config: Optional[AIConfig] = None) -> "LLMProvider":
        """根据配置创建对应 Provider 实例。"""
        cfg = config or AIConfig()
        provider_type = cfg.provider.lower()
        if provider_type == "openai":
            return OpenAIProvider(cfg)
        if provider_type in ("ollama", "local"):
            return OllamaProvider(cfg)
        return MockProvider(cfg)

    def health_check(self) -> bool:
        """检查 Provider 是否可用。子类可覆盖。"""
        return True

    def estimate_tokens(self, prompt: str) -> int:
        """粗略估算输入 Token 数（按中文字符 1:1，英文 1:0.6）。"""
        chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", prompt))
        other_chars = len(prompt) - chinese_chars
        return int(chinese_chars + other_chars * 0.6)

    def generate(self, prompt: str, schema: Optional[dict] = None, temperature: Optional[float] = None) -> GenerationResult:
        """调用 LLM 生成结构化输出。子类必须实现。"""
        raise NotImplementedError

    def _build_messages(self, prompt: str, schema: Optional[dict] = None) -> list[dict]:
        """组装消息列表，附加 schema 约束指令。"""
        content = prompt
        if schema:
            schema_instruction = (
                "\n\n【强制要求】请严格按以下 JSON Schema 输出，不要包含任何解释性文字，"
                "不要包含 markdown 代码块标记，仅返回合法 JSON：\n"
                f"{json.dumps(schema, ensure_ascii=False, indent=2)}"
            )
            content += schema_instruction
        return [{"role": "user", "content": content}]


class OpenAIProvider(LLMProvider):
    """OpenAI 兼容 API Provider（支持 OpenAI / Azure / vLLM 等）。"""

    def __init__(self, config: AIConfig):
        super().__init__(config)
        self._client = None
        try:
            import openai
            api_base = config.api_base or None
            api_key = config.api_key or None
            if api_base:
                self._client = openai.OpenAI(base_url=api_base, api_key=api_key, timeout=config.timeout)
            else:
                self._client = openai.OpenAI(api_key=api_key, timeout=config.timeout)
        except ImportError:
            self.logger.error("未安装 openai 包，请运行: pip install openai")

    def health_check(self) -> bool:
        return self._client is not None

    def generate(self, prompt: str, schema: Optional[dict] = None, temperature: Optional[float] = None) -> GenerationResult:
        if not self._client:
            return GenerationResult(成功=False, 错误信息="openai 包未安装或客户端初始化失败")

        messages = self._build_messages(prompt, schema)
        temp = temperature if temperature is not None else self.config.temperature

        try:
            response = self._client.chat.completions.create(
                model=self.config.model,
                messages=messages,
                temperature=temp,
                max_tokens=self.config.max_tokens,
            )
            raw = response.choices[0].message.content or ""
            return self._parse_raw_output(raw)
        except Exception as e:
            self.logger.error(f"OpenAI 调用失败: {e}")
            return GenerationResult(成功=False, 错误信息=str(e))

    @staticmethod
    def _parse_raw_output(raw: str) -> GenerationResult:
        """尝试从原始输出中提取 JSON。"""
        raw = raw.strip()
        # 移除 markdown 代码块标记
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
        try:
            parsed = json.loads(raw)
            return GenerationResult(成功=True, 原始输出=raw, 结构化输出=parsed)
        except json.JSONDecodeError as e:
            return GenerationResult(成功=False, 原始输出=raw, 错误信息=f"JSON 解析失败: {e}")


class OllamaProvider(LLMProvider):
    """Ollama 本地模型 Provider。"""

    def __init__(self, config: AIConfig):
        super().__init__(config)
        self.api_base = config.api_base or "http://localhost:11434"

    def health_check(self) -> bool:
        try:
            import urllib.request
            req = urllib.request.Request(f"{self.api_base}/api/tags", method="GET")
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception as e:
            self.logger.warning(f"Ollama 健康检查失败: {e}")
            return False

    def generate(self, prompt: str, schema: Optional[dict] = None, temperature: Optional[float] = None) -> GenerationResult:
        try:
            import urllib.request
            messages = self._build_messages(prompt, schema)
            # 取最后一条 user 消息作为 Ollama 的 prompt
            prompt_text = messages[-1]["content"]
            payload = {
                "model": self.config.model,
                "prompt": prompt_text,
                "stream": False,
                "options": {
                    "temperature": temperature if temperature is not None else self.config.temperature,
                },
            }
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                f"{self.api_base}/api/generate",
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                raw = body.get("response", "")
                return OpenAIProvider._parse_raw_output(raw)
        except Exception as e:
            self.logger.error(f"Ollama 调用失败: {e}")
            return GenerationResult(成功=False, 错误信息=str(e))


class MockProvider(LLMProvider):
    """Mock Provider，用于测试和默认降级场景。"""

    def __init__(self, config: AIConfig):
        super().__init__(config)
        self.call_count = 0

    def health_check(self) -> bool:
        return True

    def generate(self, prompt: str, schema: Optional[dict] = None, temperature: Optional[float] = None) -> GenerationResult:
        self.call_count += 1
        self.logger.info(f"MockProvider 被调用第 {self.call_count} 次")

        # 根据 prompt 关键词返回不同 mock 结果
        lower_prompt = prompt.lower()
        if "narrative" in lower_prompt or "叙事" in prompt:
            mock_output = {
                "叙事块列表": [
                    {
                        "标题": "Mock 人员级叙事",
                        "内容": "这是用于测试的 mock 叙事内容。",
                        "级别": "person",
                        "证据引用": [{"证据类型": "anomaly", "证据ID": "mock-001", "说明": "mock"}],
                        "置信度": "中",
                    }
                ]
            }
        elif "hypothesis" in lower_prompt or "假设" in prompt:
            mock_output = {
                "调查假设列表": [
                    {
                        "假设ID": "H-mock-001",
                        "标题": "Mock 假设",
                        "描述": "这是用于测试的 mock 假设。",
                        "验证方向": ["核实 mock 证据"],
                        "证据引用": [{"证据类型": "pattern", "证据ID": "mock-002", "说明": "mock"}],
                        "风险等级": "中",
                    }
                ]
            }
        elif "qa" in lower_prompt or "问答" in prompt:
            mock_output = {
                "回答": "这是 mock 问答回答。",
                "证据引用": [{"证据类型": "timeline", "证据ID": "mock-003", "说明": "mock"}],
                "是否可回答": True,
                "未回答原因": "",
            }
        else:
            mock_output = {"mock": True}

        return GenerationResult(成功=True, 原始输出=json.dumps(mock_output, ensure_ascii=False), 结构化输出=mock_output)
