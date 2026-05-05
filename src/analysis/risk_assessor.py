"""风险研判器 - 综合分析结果给出线索级判断"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional

from src.models.analysis_result import (
    PersonBaseline, AnomalyItem, PatternMatch, TimelineChain,
    RiskAssessment, KeyPerson
)


class RiskAssessor:
    """风险研判器 - 综合评分+风险等级+调查建议"""

    def __init__(self, thresholds=None):
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def assess(
        self,
        persons: list[str],
        baseline: dict[str, PersonBaseline],
        anomalies: dict[str, list[AnomalyItem]],
        patterns: dict[str, list[PatternMatch]],
        timeline_chains: dict[str, list[TimelineChain]],
        cross_analysis: dict,
        call_frequency: list = None,
    ) -> dict[str, RiskAssessment]:
        """
        综合风险研判

        Args:
            persons: 人员列表
            baseline: {本方姓名: PersonBaseline}
            anomalies: {本方姓名: [AnomalyItem]}
            patterns: {本方姓名: [PatternMatch]}
            timeline_chains: {本方姓名: [TimelineChain]}
            cross_analysis: 交叉分析结果
            call_frequency: 话单频率分析结果
        Returns:
            {本方姓名: RiskAssessment}
        """
        result: dict[str, RiskAssessment] = {}

        for person in persons:
            bl = baseline.get(person)
            anoms = anomalies.get(person, [])
            pats = patterns.get(person, [])
            tl = timeline_chains.get(person, [])

            assessment = self._assess_person(person, bl, anoms, pats, tl, cross_analysis, call_frequency)
            result[person] = assessment

        return result

    # ------------------------------------------------------------------
    # 单人风险评估
    # ------------------------------------------------------------------
    def _assess_person(
        self,
        person: str,
        baseline: Optional[PersonBaseline],
        anomalies: list[AnomalyItem],
        patterns: list[PatternMatch],
        timeline_chains: list[TimelineChain],
        cross_analysis: dict,
        call_frequency: list = None,
    ) -> RiskAssessment:
        """评估单人风险"""

        # 1. 异常偏离度评分 (0-30)
        dev_score, dev_desc = self._score_anomaly_deviation(anomalies)

        # 2. 行为模式评分 (0-25)
        pattern_score, pattern_desc = self._score_patterns(patterns)

        # 3. 证据链评分 (0-25)
        chain_score, chain_desc = self._score_chains(timeline_chains)

        # 4. 规模评分 (0-20)
        scale_score, scale_desc = self._score_scale(anomalies, baseline)

        # 汇总
        total = dev_score + pattern_score + chain_score + scale_score
        level = "高风险" if total >= 70 else "中风险" if total >= 40 else "低风险"

        # 重点人员
        key_persons = self._rank_key_persons(person, anomalies, patterns, timeline_chains, call_frequency)

        # 证据充分度
        evidence_level, existing, missing = self._assess_evidence(patterns, timeline_chains, anomalies)

        # 调查建议
        suggestions = self._generate_suggestions(missing, key_persons, patterns)

        # 重点时段
        focus_period = self._detect_focus_period(anomalies)

        return RiskAssessment(
            本方姓名=person,
            异常偏离度得分=round(dev_score, 1),
            异常偏离度说明=dev_desc,
            行为模式得分=round(pattern_score, 1),
            行为模式说明=pattern_desc,
            证据链得分=round(chain_score, 1),
            证据链说明=chain_desc,
            规模得分=round(scale_score, 1),
            规模说明=scale_desc,
            综合风险分数=round(total, 1),
            综合风险等级=level,
            重点人员=key_persons,
            证据充分度=evidence_level,
            已有证据=existing,
            待补充证据=missing,
            调查方向建议=suggestions,
            重点时段=focus_period,
        )

    # ------------------------------------------------------------------
    # 评分子方法
    # ------------------------------------------------------------------
    def _score_anomaly_deviation(self, anomalies: list[AnomalyItem]) -> tuple[float, str]:
        """异常偏离度评分 (0-30)"""
        if not anomalies:
            return 0.0, "未发现异常"

        max_sigma = max((a.偏离度 for a in anomalies if a.偏离度 > 0), default=0)
        high_count = sum(1 for a in anomalies if a.严重程度 == '高')
        total_count = len(anomalies)

        # 偏离度维度: 最大σ/5 * 20 + 异常数量/10 * 10
        dev_part = min(max_sigma / 5, 1) * 20
        count_part = min(total_count / 10, 1) * 10
        score = dev_part + count_part

        desc_parts = []
        if max_sigma > 0:
            desc_parts.append(f"最大偏离{max_sigma:.1f}σ")
        if high_count > 0:
            desc_parts.append(f"高风险异常{high_count}项")
        if total_count > 0:
            desc_parts.append(f"共{total_count}项异常")

        return score, "；".join(desc_parts) if desc_parts else "未发现异常"

    def _score_patterns(self, patterns: list[PatternMatch]) -> tuple[float, str]:
        """行为模式评分 (0-25)"""
        if not patterns:
            return 0.0, "未匹配到行为模式"

        pattern_count = len(patterns)
        avg_match = np.mean([p.匹配度 for p in patterns])

        # 模式数量维度: 数量/3 * 15 + 平均匹配度 * 10
        count_part = min(pattern_count / 3, 1) * 15
        match_part = avg_match * 10
        score = count_part + match_part

        pattern_names = [f"{p.模式名称}({p.匹配度:.0%})" for p in patterns]
        return score, "；".join(pattern_names)

    def _score_chains(self, chains: list[TimelineChain]) -> tuple[float, str]:
        """证据链评分 (0-25)"""
        if not chains:
            return 0.0, "未发现时序链"

        three_seg = [c for c in chains if c.链模式 == "通话→资金→通话"]
        two_seg = [c for c in chains if c.链模式 == "通话→资金"]

        # 三段链权重>二段链
        three_part = min(len(three_seg) / 3, 1) * 15
        two_part = min(len(two_seg) / 5, 1) * 10
        score = three_part + two_part

        desc_parts = []
        if three_seg:
            desc_parts.append(f"{len(three_seg)}条完整时序链(通话→资金→通话)")
        if two_seg:
            desc_parts.append(f"{len(two_seg)}条部分时序链(通话→资金)")

        return score, "；".join(desc_parts)

    def _score_scale(self, anomalies: list[AnomalyItem],
                     baseline: Optional[PersonBaseline]) -> tuple[float, str]:
        """规模评分 (0-20)"""
        total_amount = sum(a.交易金额 for a in anomalies if a.交易金额 > 0)
        person_count = len(set(a.对方姓名 for a in anomalies if a.对方姓名))

        # 金额维度: 总额/500万 * 15 + 人员数/5 * 5
        amount_part = min(total_amount / 5000000, 1) * 15
        person_part = min(person_count / 5, 1) * 5
        score = amount_part + person_part

        desc_parts = []
        if total_amount > 0:
            desc_parts.append(f"涉及金额{total_amount:,.0f}元")
        if person_count > 0:
            desc_parts.append(f"涉及{person_count}人")

        return score, "；".join(desc_parts) if desc_parts else "规模较小"

    # ------------------------------------------------------------------
    # 重点人员排序
    # ------------------------------------------------------------------
    def _rank_key_persons(self, person: str, anomalies: list[AnomalyItem],
                           patterns: list[PatternMatch],
                           timeline_chains: list[TimelineChain],
                           call_frequency: list = None) -> list[KeyPerson]:
        """按关联度排序对手"""
        opponent_scores: dict[str, dict] = {}

        # 从异常中收集
        for a in anomalies:
            opp = a.对方姓名
            if not opp:
                continue
            if opp not in opponent_scores:
                opponent_scores[opp] = {'金额': 0, '异常数': 0, '链数': 0, '模式': [], '通话次数': 0}
            opponent_scores[opp]['金额'] += a.交易金额
            opponent_scores[opp]['异常数'] += 1

        # 从模式中收集
        for p in patterns:
            opp = p.涉及对手
            if not opp:
                continue
            if opp not in opponent_scores:
                opponent_scores[opp] = {'金额': 0, '异常数': 0, '链数': 0, '模式': [], '通话次数': 0}
            opponent_scores[opp]['模式'].append(p.模式名称)

        # 从时序链中收集
        for c in timeline_chains:
            opp = c.对方姓名
            if not opp:
                continue
            if opp not in opponent_scores:
                opponent_scores[opp] = {'金额': 0, '异常数': 0, '链数': 0, '模式': [], '通话次数': 0}
            opponent_scores[opp]['链数'] += 1

        # 从话单频率中获取通话次数和对方单位/职务
        opp_unit_map = {}
        opp_title_map = {}
        if call_frequency:
            for item in call_frequency:
                if hasattr(item, '本方姓名') and item.本方姓名 != person:
                    continue
                opp_name = item.对方姓名 if hasattr(item, '对方姓名') else ''
                if not opp_name:
                    continue
                opp_unit_map[opp_name] = item.对方单位名称 or '' if hasattr(item, '对方单位名称') else ''
                opp_title_map[opp_name] = item.对方职务 or '' if hasattr(item, '对方职务') else ''
                if opp_name in opponent_scores:
                    opponent_scores[opp_name]['通话次数'] = item.通话次数 if hasattr(item, '通话次数') else 0

        # 计算关联度评分
        ranked = []
        for opp, info in opponent_scores.items():
            score = info['异常数'] * 2 + info['链数'] * 5 + min(info['金额'] / 100000, 10) + len(info['模式']) * 3
            features = []
            if info['链数'] > 0:
                features.append(f"{info['链数']}条时序链")
            if info['模式']:
                features.append('+'.join(info['模式']))
            if info['异常数'] > 0:
                features.append(f"{info['异常数']}项异常")

            ranked.append((opp, score, info, features))

        ranked.sort(key=lambda x: x[1], reverse=True)

        result = []
        for rank, (opp, score, info, features) in enumerate(ranked[:5], 1):
            result.append(KeyPerson(
                排名=rank,
                对方姓名=opp,
                对方单位=opp_unit_map.get(opp, ''),
                对方职务=opp_title_map.get(opp, ''),
                关联度评分=round(score, 1),
                通话次数=info['通话次数'],
                资金往来金额=info['金额'],
                时序链数=info['链数'],
                关联特征='；'.join(features),
            ))

        return result

    # ------------------------------------------------------------------
    # 证据充分度评估
    # ------------------------------------------------------------------
    def _assess_evidence(self, patterns: list[PatternMatch],
                          chains: list[TimelineChain],
                          anomalies: list[AnomalyItem]) -> tuple[str, list[str], list[str]]:
        """评估证据充分度"""
        existing = []
        missing = []

        # 已有证据
        three_seg = [c for c in chains if c.链模式 == "通话→资金→通话"]
        two_seg = [c for c in chains if c.链模式 == "通话→资金"]

        if three_seg:
            existing.append(f"{len(three_seg)}条完整时序链")
        if two_seg:
            existing.append(f"{len(two_seg)}条部分时序链")
        if patterns:
            existing.append(f"{len(patterns)}种行为模式匹配")
        if anomalies:
            high_anoms = [a for a in anomalies if a.严重程度 == '高']
            if high_anoms:
                existing.append(f"{len(high_anoms)}项高风险异常")

        # 待补充证据
        if not chains:
            missing.append("通讯-资金时序链")
        if not patterns:
            missing.append("行为模式归类的更多数据")
        if not any(p.涉及对手 for p in patterns):
            missing.append("涉及对手的职权关系信息")
        if any(p.模式编号 == 'P2' for p in patterns):
            missing.append("资金最终去向")
        if any(p.模式编号 == 'P1' for p in patterns):
            missing.append("周期性收入来源的合法性")

        # 充分度判定
        if len(existing) >= 3 and three_seg:
            level = "充分"
        elif len(existing) >= 2:
            level = "较充分"
        else:
            level = "待补充"

        return level, existing, missing

    # ------------------------------------------------------------------
    # 调查建议生成
    # ------------------------------------------------------------------
    def _generate_suggestions(self, missing: list[str], key_persons: list[KeyPerson],
                               patterns: list[PatternMatch]) -> list[str]:
        """基于证据薄弱环节生成调查建议"""
        suggestions = []

        # 基于待补充证据
        for m in missing:
            if "职权关系" in m:
                for kp in key_persons[:2]:
                    unit_info = f"（{kp.对方单位}）" if kp.对方单位 else ""
                    suggestions.append(f"优先核查{kp.对方姓名}{unit_info}与被核查人的职权关系")
            elif "资金去向" in m:
                suggestions.append("追踪取现资金的最终去向")
            elif "合法性" in m:
                suggestions.append("核查周期性收入来源的合法性")
            elif "时序链" in m:
                suggestions.append("补充通讯记录以构建时序链")

        # 基于行为模式追加建议
        for p in patterns:
            if p.模式编号 == 'P3' and p.涉及对手:
                suggestions.append(f"核查与{p.涉及对手}通讯后资金往来的决策背景")
            elif p.模式编号 == 'P2':
                suggestions.append("核查资金快速中转的原因和目的")
            elif p.模式编号 == 'P5' and p.涉及对手:
                suggestions.append(f"核实与{p.涉及对手}关系的性质")

        # 去重并限制数量
        seen = set()
        unique = []
        for s in suggestions:
            if s not in seen:
                seen.add(s)
                unique.append(s)

        return unique[:5]

    # ------------------------------------------------------------------
    # 重点时段检测
    # ------------------------------------------------------------------
    def _detect_focus_period(self, anomalies: list[AnomalyItem]) -> str:
        """检测异常最集中的时间段"""
        if not anomalies:
            return ""

        dates = []
        for a in anomalies:
            if a.交易日期:
                try:
                    dates.append(a.交易日期[:7])  # YYYY-MM
                except Exception:
                    pass

        if not dates:
            return ""

        from collections import Counter
        month_counts = Counter(dates)
        if not month_counts:
            return ""

        # 找出异常最密集的月份
        sorted_months = month_counts.most_common()
        if len(sorted_months) == 1:
            return sorted_months[0][0]

        # 找出连续的密集月份
        top_months = [m for m, _ in sorted_months[:5]]
        top_months.sort()

        if len(top_months) >= 2:
            return f"{top_months[0]}至{top_months[-1]}"
        return top_months[0] if top_months else ""
