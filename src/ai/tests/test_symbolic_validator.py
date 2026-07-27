#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""符号校验层单元测试"""

import pytest

from src.ai.symbolic_validator import SymbolicValidator, EvidenceIndex
from src.ai.models import EvidenceRef, NarrativeBlock
from src.models.analysis_result import (
    AnalysisResult, AnomalyItem, AIInsight,
)


class TestEvidenceIndex:
    def test_build_index_for_anomalies(self):
        result = AnalysisResult(
            persons=["张三"],
            anomalies={
                "张三": [
                    AnomalyItem(异常类型="资金异常", 异常子类="月度收入突增", 偏离度=3.5, 严重程度="高",
                                本方姓名="张三", 对方姓名="李四", 交易日期="2024-01-01", 交易金额=100000.0, 基线均值=10000.0, 说明="测试")
                ]
            }
        )
        index = EvidenceIndex(result)
        assert index.exists("anomaly", "A-张三-0")
        assert not index.exists("anomaly", "A-张三-999")


class TestSymbolicValidator:
    def test_validate_input_with_no_signals(self):
        result = AnalysisResult(persons=["张三"])
        validator = SymbolicValidator()
        val = validator.validate_input(result)
        assert val.通过
        assert any("无任何可翻译信号" in w for w in val.警告列表)

    def test_check_hallucination_detects_fake_ref(self):
        result = AnalysisResult(
            persons=["张三"],
            anomalies={
                "张三": [
                    AnomalyItem(异常类型="资金异常", 异常子类="月度收入突增", 偏离度=3.5, 严重程度="高",
                                本方姓名="张三", 对方姓名="李四", 交易日期="2024-01-01", 交易金额=100000.0, 基线均值=10000.0, 说明="测试")
                ]
            }
        )
        ai_output = {
            "叙事块列表": [
                {
                    "标题": "测试",
                    "内容": "测试内容",
                    "级别": "person",
                    "证据引用": [{"证据类型": "anomaly", "证据ID": "A-张三-999", "说明": "不存在的证据"}]
                }
            ]
        }
        validator = SymbolicValidator()
        val = validator.check_hallucination(ai_output, result)
        assert not val.通过
        assert any("A-张三-999" in e for e in val.错误列表)
