#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""AI 协同层配置管理"""

import os
from typing import Optional


class AIConfig:
    """AI 层配置，支持从环境变量、config.json 或默认配置读取。"""

    # 默认配置
    DEFAULTS = {
        "enabled": False,
        "provider": "mock",  # openai / ollama / mock
        "model": "qwen2.5:7b",
        "api_base": "",
        "api_key": "",
        "temperature": 0.3,
        "max_tokens": 4096,
        "timeout": 120.0,
        "cache_enabled": True,
        "cache_dir": "cache/ai",
        "max_retries": 1,
        "prompts_dir": "src/ai/prompts",
    }

    def __init__(self, settings: Optional[dict] = None):
        """
        Args:
            settings: 从 AppSettings 或 config.json 读取的 ai 配置字典
        """
        self._raw = settings or {}

    def get(self, key: str, default=None):
        """获取配置项，优先级：显式配置 > 环境变量 > 默认值。"""
        env_key = f"DATADONE_AI_{key.upper()}"
        if env_key in os.environ:
            value = os.environ[env_key]
            # 布尔类型转换
            if isinstance(self.DEFAULTS.get(key), bool):
                return value.lower() in ("true", "1", "yes", "on")
            # 数字类型转换
            if isinstance(self.DEFAULTS.get(key), (int, float)):
                try:
                    return type(self.DEFAULTS[key])(value)
                except ValueError:
                    pass
            return value

        if key in self._raw:
            return self._raw[key]

        return default if default is not None else self.DEFAULTS.get(key)

    @property
    def enabled(self) -> bool:
        return bool(self.get("enabled"))

    @property
    def provider(self) -> str:
        return str(self.get("provider"))

    @property
    def model(self) -> str:
        return str(self.get("model"))

    @property
    def api_base(self) -> str:
        return str(self.get("api_base"))

    @property
    def api_key(self) -> str:
        return str(self.get("api_key"))

    @property
    def temperature(self) -> float:
        return float(self.get("temperature"))

    @property
    def max_tokens(self) -> int:
        return int(self.get("max_tokens"))

    @property
    def timeout(self) -> float:
        return float(self.get("timeout"))

    @property
    def cache_enabled(self) -> bool:
        return bool(self.get("cache_enabled"))

    @property
    def cache_dir(self) -> str:
        return str(self.get("cache_dir"))

    @property
    def max_retries(self) -> int:
        return int(self.get("max_retries"))

    @property
    def prompts_dir(self) -> str:
        return str(self.get("prompts_dir"))

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "provider": self.provider,
            "model": self.model,
            "api_base": self.api_base,
            "api_key": self.api_key[:4] + "****" if self.api_key else "",
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "timeout": self.timeout,
            "cache_enabled": self.cache_enabled,
            "cache_dir": self.cache_dir,
            "max_retries": self.max_retries,
            "prompts_dir": self.prompts_dir,
        }
