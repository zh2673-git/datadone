#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""叙事生成器 - 把规则信号翻译为自然语言叙事。"""

import logging
from typing import Optional

from src.ai.config import AIConfig
from src.ai.models import NarrativeBlock, AIInsight, GenerationResult
from src.ai.provider import LLMProvider
from src.ai.prompt_manager import PromptManager
from src.ai.symbolic_validator import SymbolicValidator
from src.models.analysis_result import AnalysisResult

logger = logging.getLogger(__name__)


class NarrativeEngine:
    """基于 AnalysisResult 生成案件级、人员级、线索级叙事。"""

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

    def generate_case_narrative(self, analysis_result: AnalysisResult) -> AIInsight:
        """生成案件级叙事（跨人员综合）。"""
        # 案件级叙事暂不实现复杂跨人员推理，返回一个汇总性 insight
        blocks = []
        for person in analysis_result.persons:
            person_blocks = self.generate_person_narrative(person, analysis_result)
            blocks.extend(person_blocks.人员级叙事)

        return AIInsight(
            本方姓名="案件汇总",
            案件级叙事=[
                NarrativeBlock(
                    标题=f"案件涉及 {len(analysis_result.persons)} 人",
                    内容=f"本次分析共涉及 {', '.join(analysis_result.persons)}。"
                         f"建议结合各人员叙事进一步研判交叉关系。",
                    级别="case",
                    证据引用=[],
                    置信度="中",
                )
            ],
            人员级叙事=blocks,
        )

    def generate_person_narrative(self, person: str, analysis_result: AnalysisResult) -> AIInsight:
        """生成单人员叙事。"""
        context = self._build_person_context(person, analysis_result)
        prompt = self.prompt_manager.render("narrative_person.j2", context)

        schema = {
            "type": "object",
            "properties": {
                "叙事块列表": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "标题": {"type": "string"},
                            "内容": {"type": "string"},
                            "级别": {"type": "string"},
                            "本方姓名": {"type": "string"},
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
                            "置信度": {"type": "string"},
                        },
                        "required": ["标题", "内容", "级别", "证据引用", "置信度"],
                    },
                }
            },
            "required": ["叙事块列表"],
        }

        gen_result = self.provider.generate(prompt, schema=schema)
        if not gen_result.成功:
            self.logger.error(f"生成 {person} 叙事失败: {gen_result.错误信息}")
            raise RuntimeError(f"AI 叙事生成失败: {gen_result.错误信息}")

        # 校验
        output = gen_result.结构化输出 or {}
        val_output = self.validator.validate_output(output)
        val_hallu = self.validator.check_hallucination(output, analysis_result)
        if not val_output.通过 or not val_hallu.通过:
            errors = val_output.错误列表 + val_hallu.错误列表
            raise RuntimeError(f"AI 叙事校验失败: {'; '.join(errors)}")

        blocks = self._parse_narrative_blocks(output, person)
        return AIInsight(本方姓名=person, 人员级叙事=blocks)

    def generate_clue_narratives(self, person: str, analysis_result: AnalysisResult) -> list[NarrativeBlock]:
        """生成线索级叙事（按单条异常/模式/时序链）。"""
        blocks = []
        anomalies = analysis_result.anomalies.get(person, [])
        for idx, item in enumerate(anomalies):
            blocks.append(
                NarrativeBlock(
                    标题=f"异常线索：{item.异常类型} - {item.异常子类}",
                    内容=f"{person} 在 {item.交易日期} 与 {item.对方姓名} 发生交易，"
                         f"金额 {item.交易金额:,.2f} 元，偏离度 {item.偏离度:.2f}，"
                         f"严重程度 {item.严重程度}。{item.说明}",
                    级别="clue",
                    本方姓名=person,
                    证据引用=[{"证据类型": "anomaly", "证据ID": f"A-{person}-{idx}", "说明": item.异常子类}],
                    置信度=item.严重程度 if item.严重程度 in ("高", "中", "低") else "中",
                )
            )
        return blocks

    def _build_person_context(self, person: str, analysis_result: AnalysisResult) -> dict:
        """构建单人员叙事 Prompt 上下文。"""
        baseline = analysis_result.baseline.get(person)
        baseline_dict = baseline.model_dump() if baseline else {}

        anomalies = [a.model_dump() for a in analysis_result.anomalies.get(person, [])]
        patterns = [p.model_dump() for p in analysis_result.patterns.get(person, [])]
        timeline = [t.model_dump() for t in analysis_result.timeline_chains.get(person, [])]
        risk = analysis_result.risk_assessment.get(person)
        risk_dict = risk.model_dump() if risk else {}

        return {
            "person": person,
            "baseline": baseline_dict,
            "anomalies": anomalies,
            "patterns": patterns,
            "timeline_chains": timeline,
            "risk": risk_dict,
        }

    @staticmethod
    def _parse_narrative_blocks(output: dict, person: str) -> list[NarrativeBlock]:
        """解析 LLM 输出为 NarrativeBlock 列表。"""
        blocks = []
        raw_blocks = output.get("叙事块列表", [])
        for item in raw_blocks:
            if not isinstance(item, dict):
                continue
            block = NarrativeBlock(
                标题=item.get("标题", ""),
                内容=item.get("内容", ""),
                级别=item.get("级别", "person"),
                本方姓名=item.get("本方姓名") or person,
                证据引用=item.get("证据引用", []),
                置信度=item.get("置信度", "中"),
            )
            blocks.append(block)
        return blocks
