#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""LLM Provider 单元测试"""

import pytest

from src.ai.config import AIConfig
from src.ai.provider import LLMProvider, MockProvider


class TestMockProvider:
    def test_mock_provider_returns_structured_output(self):
        config = AIConfig({"provider": "mock", "model": "test-model"})
        provider = MockProvider(config)
        result = provider.generate("请生成叙事")
        assert result.成功
        assert result.结构化输出 is not None
        assert "叙事块列表" in result.结构化输出

    def test_provider_factory_routes_to_mock(self):
        config = AIConfig({"provider": "mock"})
        provider = LLMProvider.from_config(config)
        assert isinstance(provider, MockProvider)

    def test_estimate_tokens(self):
        config = AIConfig({"provider": "mock"})
        provider = MockProvider(config)
        tokens = provider.estimate_tokens("你好 world")
        assert tokens > 0
