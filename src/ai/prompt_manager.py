#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Prompt 管理层 - 版本化 Jinja2 模板管理。"""

import os
import logging
from typing import Optional

from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from src.ai.config import AIConfig

logger = logging.getLogger(__name__)


class PromptManager:
    """管理 AI 层的 Jinja2 Prompt 模板。"""

    VERSION = "1.0.0"

    def __init__(self, config: Optional[AIConfig] = None):
        self.config = config or AIConfig()
        self.template_dir = self.config.prompts_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        self._env: Optional[Environment] = None
        self._load_env()

    def _load_env(self):
        """加载 Jinja2 环境。"""
        if not os.path.isdir(self.template_dir):
            self.logger.warning(f"Prompt 目录不存在: {self.template_dir}，将使用空环境")
            self._env = Environment()
            return
        self._env = Environment(loader=FileSystemLoader(self.template_dir))

    def list_templates(self) -> list[str]:
        """列出可用模板文件名。"""
        if not self._env or not os.path.isdir(self.template_dir):
            return []
        return [f for f in os.listdir(self.template_dir) if f.endswith(".j2")]

    def load(self, name: str):
        """加载模板对象。"""
        if not self._env:
            raise TemplateNotFound(name)
        return self._env.get_template(name)

    def render(self, name: str, context: dict) -> str:
        """渲染指定模板。"""
        try:
            template = self.load(name)
            ctx = {"version": self.VERSION, **context}
            return template.render(ctx)
        except TemplateNotFound:
            self.logger.error(f"模板不存在: {name}")
            # 返回一个带提示的 fallback prompt，避免直接崩溃
            return self._fallback_prompt(name, context)

    @staticmethod
    def _fallback_prompt(name: str, context: dict) -> str:
        """模板缺失时的兜底 Prompt。"""
        import json
        return (
            f"【系统提示：模板 {name} 缺失，以下为原始上下文】\n\n"
            f"{json.dumps(context, ensure_ascii=False, indent=2)}"
        )
