#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""假设引擎单元测试"""

import pytest

from src.ai.config import AIConfig
from src.ai.models import GenerationResult
from src.ai.hypothesis_engine import HypothesisEngine
from src.ai.provider import LLMProvider
from src.models.analysis_result import (
    AnalysisResult, PatternMatch
)


class StubHypothesisProvider(LLMProvider):
    """返回固定假设结果的 Provider，用于单元测试。"""

    def __init__(self, output: dict):
        super().__init__(AIConfig({"provider": "mock"}))
        self.output = output

    def generate(self, prompt: str, schema: dict = None, temperature: float = None) -> GenerationResult:
        import json
        return GenerationResult(
            成功=True,
            原始输出=json.dumps(self.output, ensure_ascii=False),
            结构化输出=self.output,
        )


def make_pattern(person: str) -> PatternMatch:
    return PatternMatch(
        模式编号="P1",
        模式名称="周期性收入模式",
        匹配度=0.85,
        涉及对手="李四",
        关键证据="每月固定日期收到李四转账",
        满足条件=["连续3个月收到固定金额"],
        报告用语="存在周期性收入特征",
    )


class TestHypothesisEngine:
    def test_generate_hypotheses_with_valid_evidence(self):
        person = "张三"
        result = AnalysisResult(
            persons=[person],
            patterns={person: [make_pattern(person)]},
        )

        stub_output = {
            "调查假设列表": [
                {
                    "假设ID": "H-张三-001",
                    "标题": "存在周期性利益输送可能",
                    "描述": "张三连续多个月收到李四固定金额转账，建议进一步核实资金性质。",
                    "验证方向": ["核实李四身份", "调取相关合同或说明"],
                    "证据引用": [{"证据类型": "pattern", "证据ID": "P-张三-0", "说明": "周期性收入模式"}],
                    "风险等级": "中",
                }
            ]
        }

        engine = HypothesisEngine(provider=StubHypothesisProvider(stub_output))
        hypotheses = engine.generate_hypotheses(person, result)

        assert len(hypotheses) == 1
        hypo = hypotheses[0]
        assert hypo.假设ID == "H-张三-001"
        assert hypo.风险等级 == "中"
        assert len(hypo.验证方向) == 2

    def test_generate_hypotheses_detects_hallucination(self):
        person = "张三"
        result = AnalysisResult(
            persons=[person],
            patterns={person: [make_pattern(person)]},
        )

        # 证据引用不存在的 ID，校验应失败
        stub_output = {
            "调查假设列表": [
                {
                    "假设ID": "H-张三-001",
                    "标题": "测试假设",
                    "描述": "引用了一个不存在的证据。",
                    "验证方向": ["核实"],
                    "证据引用": [{"证据类型": "pattern", "证据ID": "P-张三-999", "说明": "不存在"}],
                    "风险等级": "低",
                }
            ]
        }

        engine = HypothesisEngine(provider=StubHypothesisProvider(stub_output))
        with pytest.raises(RuntimeError) as exc_info:
            engine.generate_hypotheses(person, result)

        assert "校验失败" in str(exc_info.value) or "AI 假设校验失败" in str(exc_info.value)
