#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""问答引擎 - 基于规则信号回答自然语言问题。"""

import logging
import re
from typing import Optional

from src.ai.config import AIConfig
from src.ai.models import QAPair
from src.ai.provider import LLMProvider
from src.ai.prompt_manager import PromptManager
from src.ai.symbolic_validator import SymbolicValidator
from src.models.analysis_result import AnalysisResult

logger = logging.getLogger(__name__)


class QAEngine:
    """基于 AnalysisResult 回答自然语言问题。"""

    # 越界问题关键词：AI 不能给出法律定性或超出数据范围的意见
    OUT_OF_SCOPE_KEYWORDS = [
        "是否构成", "是否违法", "是否犯罪", "定罪", "量刑", "逮捕",
        "受贿", "贪污", "挪用公款", "洗钱", "有罪", "无罪",
    ]

    def __init__(
        self,
        provider: Optional[LLMProvider] = None,
        prompt_manager: Optional[PromptManager] = None,
        validator: Optional[SymbolicValidator] = None,
        config: Optional[AIConfig] = None,
    ):
        self.config = config or AIConfig()
        self.provider = provider or LLMProvider.from_config(self.config)
        self.prompt_manager = prompt_manager or PromptManager(self.config)
        self.validator = validator or SymbolicValidator()
        self.logger = logging.getLogger(self.__class__.__name__)

    def is_answerable(self, question: str) -> bool:
        """判断问题是否在系统能力范围内。"""
        lower = question.lower()
        for kw in self.OUT_OF_SCOPE_KEYWORDS:
            if kw in lower:
                return False
        return True

    def answer(
        self,
        question: str,
        analysis_result: AnalysisResult,
        person: Optional[str] = None,
    ) -> QAPair:
        """回答用户问题。"""
        if not self.is_answerable(question):
            return QAPair(
                问题=question,
                回答="根据系统定位，我无法对证据进行法律定性或超出数据范围作出判断。",
                证据引用=[],
                是否可回答=False,
                未回答原因="问题涉及法律定性或超出现有数据能力范围",
            )

        context = self._build_context(question, analysis_result, person)
        prompt = self.prompt_manager.render("qa.j2", context)

        schema = {
            "type": "object",
            "properties": {
                "回答": {"type": "string"},
                "证据引用": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "证据类型": {"type": "string"},
                            "证据ID": {"type": "string"},
                            "说明": {"type": "string"},
                        },
                        "required": ["证据类型", "证据ID"],
                    },
                },
                "是否可回答": {"type": "boolean"},
                "未回答原因": {"type": "string"},
            },
            "required": ["回答", "证据引用", "是否可回答", "未回答原因"],
        }

        gen_result = self.provider.generate(prompt, schema=schema)
        if not gen_result.成功:
            self.logger.error(f"问答生成失败: {gen_result.错误信息}")
            return QAPair(
                问题=question,
                回答="AI 问答生成失败，请稍后重试或查看日志。",
                证据引用=[],
                是否可回答=False,
                未回答原因=gen_result.错误信息,
            )

        output = gen_result.结构化输出 or {}
        val_hallu = self.validator.check_hallucination(output, analysis_result)
        if not val_hallu.通过:
            return QAPair(
                问题=question,
                回答=output.get("回答", ""),
                证据引用=[],
                是否可回答=False,
                未回答原因=f"回答未通过事实校验: {'; '.join(val_hallu.错误列表)}",
            )

        return QAPair(
            问题=question,
            回答=output.get("回答", ""),
            证据引用=output.get("证据引用", []),
            是否可回答=output.get("是否可回答", True),
            未回答原因=output.get("未回答原因", ""),
        )

    def _build_context(
        self,
        question: str,
        analysis_result: AnalysisResult,
        person: Optional[str] = None,
    ) -> dict:
        """构建问答 Prompt 上下文。"""
        target_person = person
        if not target_person and analysis_result.persons:
            target_person = analysis_result.persons[0]

        # 构建信号摘要：只取与问题可能相关的信号
        summary = {
            "人员列表": analysis_result.persons,
            "目标人员": target_person,
            "异常数量": sum(len(v) for v in analysis_result.anomalies.values()),
            "模式数量": sum(len(v) for v in analysis_result.patterns.values()),
            "时序链数量": sum(len(v) for v in analysis_result.timeline_chains.values()),
            "风险研判": {
                p: (r.综合风险等级 if r else "")
                for p, r in analysis_result.risk_assessment.items()
            },
        }

        if target_person:
            summary["目标人员异常"] = [
                a.model_dump() for a in analysis_result.anomalies.get(target_person, [])
            ]
            summary["目标人员模式"] = [
                p.model_dump() for p in analysis_result.patterns.get(target_person, [])
            ]
            summary["目标人员时序链"] = [
                t.model_dump() for t in analysis_result.timeline_chains.get(target_person, [])
            ]
            risk = analysis_result.risk_assessment.get(target_person)
            if risk:
                summary["目标人员风险"] = risk.model_dump()

        return {
            "person": target_person,
            "question": question,
            "summary": summary,
        }
