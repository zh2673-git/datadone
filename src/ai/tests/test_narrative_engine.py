#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""叙事引擎单元测试"""

import pytest

from src.ai.config import AIConfig
from src.ai.models import GenerationResult, AIInsight, NarrativeBlock
from src.ai.narrative_engine import NarrativeEngine
from src.ai.provider import LLMProvider
from src.models.analysis_result import (
    AnalysisResult, AnomalyItem, PersonBaseline
)


class StubNarrativeProvider(LLMProvider):
    """返回固定叙事结果的 Provider，用于单元测试。"""

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


def make_anomaly(person: str, idx: int = 0) -> AnomalyItem:
    return AnomalyItem(
        异常类型="资金异常",
        异常子类="月度收入突增",
        偏离度=3.5,
        严重程度="高",
        本方姓名=person,
        对方姓名="李四",
        交易日期="2024-01-01",
        交易金额=100000.0,
        基线均值=10000.0,
        说明="测试异常",
    )


def make_baseline(person: str) -> PersonBaseline:
    return PersonBaseline(
        本方姓名=person,
        数据充足度="充足",
        数据月数=12,
        月均收入=50000.0,
        月均支出=30000.0,
    )


class TestNarrativeEngine:
    def test_generate_clue_narratives_returns_blocks(self):
        person = "张三"
        result = AnalysisResult(
            persons=[person],
            anomalies={person: [make_anomaly(person)]},
        )
        engine = NarrativeEngine()
        blocks = engine.generate_clue_narratives(person, result)

        assert len(blocks) == 1
        assert person in blocks[0].内容
        assert "月度收入突增" in blocks[0].标题
        assert blocks[0].级别 == "clue"

    def test_generate_person_narrative_with_valid_evidence(self):
        person = "张三"
        result = AnalysisResult(
            persons=[person],
            baseline={person: make_baseline(person)},
            anomalies={person: [make_anomaly(person)]},
        )

        stub_output = {
            "叙事块列表": [
                {
                    "标题": "测试叙事",
                    "内容": "张三存在月度收入突增情况。",
                    "级别": "person",
                    "本方姓名": person,
                    "证据引用": [{"证据类型": "anomaly", "证据ID": "A-张三-0", "说明": "测试"}],
                    "置信度": "高",
                }
            ]
        }

        engine = NarrativeEngine(provider=StubNarrativeProvider(stub_output))
        insight = engine.generate_person_narrative(person, result)

        assert insight.本方姓名 == person
        assert len(insight.人员级叙事) == 1
        block = insight.人员级叙事[0]
        assert block.标题 == "测试叙事"
        assert block.置信度 == "高"

    def test_generate_case_narrative_aggregates_persons(self):
        persons = ["张三", "李四"]
        result = AnalysisResult(persons=persons)

        engine = NarrativeEngine()

        def _fake_person_narrative(person: str, _result):
            return AIInsight(
                本方姓名=person,
                人员级叙事=[
                    NarrativeBlock(
                        标题=f"{person} 叙事",
                        内容=f"{person} 的测试内容",
                        级别="person",
                        本方姓名=person,
                        置信度="中",
                    )
                ],
            )

        engine.generate_person_narrative = _fake_person_narrative
        insight = engine.generate_case_narrative(result)

        assert insight.本方姓名 == "案件汇总"
        assert len(insight.案件级叙事) == 1
        assert "2 人" in insight.案件级叙事[0].标题
        assert len(insight.人员级叙事) == 2
