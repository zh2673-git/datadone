#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""假设生成器 - 基于规则信号生成可验证的调查假设。"""

import logging
from typing import Optional

from src.ai.config import AIConfig
from src.ai.models import Hypothesis
from src.ai.provider import LLMProvider
from src.ai.prompt_manager import PromptManager
from src.ai.symbolic_validator import SymbolicValidator
from src.models.analysis_result import AnalysisResult

logger = logging.getLogger(__name__)


class HypothesisEngine:
    """基于 AnalysisResult 生成调查假设。"""

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

    def generate_hypotheses(self, person: str, analysis_result: AnalysisResult) -> list[Hypothesis]:
        """为单个人员生成调查假设。"""
        context = self._build_context(person, analysis_result)
        prompt = self.prompt_manager.render("hypothesis.j2", context)

        schema = {
            "type": "object",
            "properties": {
                "调查假设列表": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "假设ID": {"type": "string"},
                            "标题": {"type": "string"},
                            "描述": {"type": "string"},
                            "验证方向": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
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
                            "风险等级": {"type": "string"},
                        },
                        "required": ["假设ID", "标题", "描述", "验证方向", "证据引用", "风险等级"],
                    },
                }
            },
            "required": ["调查假设列表"],
        }

        gen_result = self.provider.generate(prompt, schema=schema)
        if not gen_result.成功:
            self.logger.error(f"生成 {person} 假设失败: {gen_result.错误信息}")
            raise RuntimeError(f"AI 假设生成失败: {gen_result.错误信息}")

        output = gen_result.结构化输出 or {}
        val_output = self.validator.validate_output(output)
        val_hallu = self.validator.check_hallucination(output, analysis_result)
        if not val_output.通过 or not val_hallu.通过:
            errors = val_output.错误列表 + val_hallu.错误列表
            raise RuntimeError(f"AI 假设校验失败: {'; '.join(errors)}")

        return self._parse_hypotheses(output)

    @staticmethod
    def _build_context(person: str, analysis_result: AnalysisResult) -> dict:
        """构建假设生成 Prompt 上下文。"""
        anomalies = [a.model_dump() for a in analysis_result.anomalies.get(person, [])]
        patterns = [p.model_dump() for p in analysis_result.patterns.get(person, [])]
        timeline = [t.model_dump() for t in analysis_result.timeline_chains.get(person, [])]
        risk = analysis_result.risk_assessment.get(person)
        key_persons = []
        if risk:
            key_persons = [kp.model_dump() for kp in risk.重点人员]

        return {
            "person": person,
            "anomalies": anomalies,
            "patterns": patterns,
            "timeline_chains": timeline,
            "key_persons": key_persons,
        }

    @staticmethod
    def _parse_hypotheses(output: dict) -> list[Hypothesis]:
        """解析 LLM 输出为 Hypothesis 列表。"""
        hypotheses = []
        raw_list = output.get("调查假设列表", [])
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            hypotheses.append(
                Hypothesis(
                    假设ID=item.get("假设ID", ""),
                    标题=item.get("标题", ""),
                    描述=item.get("描述", ""),
                    验证方向=item.get("验证方向", []),
                    证据引用=item.get("证据引用", []),
                    风险等级=item.get("风险等级", "中"),
                )
            )
        return hypotheses
