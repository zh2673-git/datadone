#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""失败降级层 - AI 协同层失败时不影响规则分析主流程。"""

import logging
from typing import Optional

from src.ai.models import AIInsight


class FallbackManager:
    """管理 AI 层的失败降级。

    设计原则：
    - AI 层失败属于「增强能力失败」，不能阻塞核心规则分析。
    - 降级时生成空的 AIInsight，保证下游报告生成器无需特殊处理。
    - 所有降级原因写入日志，便于排查。
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.last_reason: Optional[str] = None

    def should_fallback(self, error: Exception) -> bool:
        """判断是否触发降级。当前策略：任何 AI 层异常都触发降级。"""
        return True

    def create_empty_insight(self, person: str) -> AIInsight:
        """生成空 AI 产物，保持接口一致性。"""
        return AIInsight(本方姓名=person)

    def log_fallback(self, reason: str, person: Optional[str] = None) -> None:
        """记录降级原因。"""
        self.last_reason = reason
        prefix = f"[{person}] " if person else ""
        self.logger.warning(f"{prefix}AI 分析已降级: {reason}")
