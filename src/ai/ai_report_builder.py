#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""AI 报告生成器 - 把 AI 产物注入现有 ReportData。"""

import logging
from typing import Optional

from src.ai.models import AIInsight, NarrativeBlock, Hypothesis
from src.ai.narrative_engine import NarrativeEngine
from src.ai.hypothesis_engine import HypothesisEngine
from src.models.report import ReportData

logger = logging.getLogger(__name__)


class AIReportBuilder:
    """将 AIInsight 注入 ReportData，供 Excel / Word 导出器消费。"""

    def __init__(
        self,
        narrative_engine: Optional[NarrativeEngine] = None,
        hypothesis_engine: Optional[HypothesisEngine] = None,
    ):
        self.narrative_engine = narrative_engine
        self.hypothesis_engine = hypothesis_engine
        self.logger = logging.getLogger(self.__class__.__name__)

    def enrich(self, report_data: ReportData, ai_insights: dict[str, AIInsight]) -> ReportData:
        """把 AI 产物注入 ReportData 的对应人员报告中。"""
        if not ai_insights:
            return report_data

        # 注入每人的 AIInsight
        for person, insight in ai_insights.items():
            if person in report_data.person_reports:
                report_data.person_reports[person].ai_insights = insight
            else:
                self.logger.warning(f"人员 {person} 不在报告数据中，跳过 AI 注入")

        # 生成 Excel AI 摘要工作表数据
        report_data.ai_summary_data = self.build_ai_summary_sheet(ai_insights)
        return report_data

    @staticmethod
    def build_ai_summary_sheet(ai_insights: dict[str, AIInsight]) -> list[dict]:
        """生成 Excel「AI 摘要」工作表数据。"""
        rows = []
        for person, insight in ai_insights.items():
            # 人员级叙事摘要
            for block in insight.人员级叙事:
                rows.append({
                    "本方姓名": person,
                    "类型": "人员级叙事",
                    "标题": block.标题,
                    "内容": block.内容,
                    "置信度": block.置信度,
                    "证据数量": len(block.证据引用),
                })

            # 线索级叙事摘要
            for block in insight.线索级叙事:
                rows.append({
                    "本方姓名": person,
                    "类型": "线索级叙事",
                    "标题": block.标题,
                    "内容": block.内容,
                    "置信度": block.置信度,
                    "证据数量": len(block.证据引用),
                })

            # 调查假设摘要
            for hypo in insight.调查假设:
                rows.append({
                    "本方姓名": person,
                    "类型": "调查假设",
                    "标题": hypo.标题,
                    "内容": hypo.描述,
                    "风险等级": hypo.风险等级,
                    "验证方向": "；".join(hypo.验证方向),
                    "证据数量": len(hypo.证据引用),
                })

        return rows

    @staticmethod
    def build_ai_word_sections(ai_insights: dict[str, AIInsight]) -> dict[str, list[dict]]:
        """生成 Word 报告可用的 AI 章节数据。"""
        sections = {"narratives": [], "hypotheses": []}
        for person, insight in ai_insights.items():
            for block in insight.人员级叙事:
                sections["narratives"].append({
                    "本方姓名": person,
                    "级别": block.级别,
                    "标题": block.标题,
                    "内容": block.内容,
                    "置信度": block.置信度,
                    "证据引用": block.证据引用,
                })
            for hypo in insight.调查假设:
                sections["hypotheses"].append({
                    "本方姓名": person,
                    "假设ID": hypo.假设ID,
                    "标题": hypo.标题,
                    "描述": hypo.描述,
                    "验证方向": hypo.验证方向,
                    "风险等级": hypo.风险等级,
                    "证据引用": hypo.证据引用,
                })
        return sections
