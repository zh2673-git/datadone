#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""符号校验层 - 保证 AI 输出可溯源、不幻觉、与规则信号一致。"""

import logging
import re
from typing import Any, Optional

from src.ai.models import ValidationResult, EvidenceRef
from src.models.analysis_result import AnalysisResult

logger = logging.getLogger(__name__)


class EvidenceIndex:
    """为 AnalysisResult 中的信号建立索引，便于 evidence_refs 校验。"""

    def __init__(self, analysis_result: AnalysisResult):
        self.result = analysis_result
        self._index: dict[str, dict[str, Any]] = {}
        self._build_index()

    def _build_index(self):
        """按规则生成证据 ID 并建立索引。"""
        # 异常
        for person, items in self.result.anomalies.items():
            for idx, item in enumerate(items):
                self._add("anomaly", f"A-{person}-{idx}", item)

        # 行为模式
        for person, items in self.result.patterns.items():
            for idx, item in enumerate(items):
                self._add("pattern", f"P-{person}-{idx}", item)

        # 时序链
        for person, items in self.result.timeline_chains.items():
            for idx, item in enumerate(items):
                self._add("timeline", f"T-{person}-{idx}", item)

        # 风险研判
        for person, item in self.result.risk_assessment.items():
            self._add("risk", f"R-{person}", item)

        # 重点收支
        for person, items in self.result.key_transactions.items():
            for idx, item in enumerate(items):
                self._add("key_transaction", f"K-{person}-{idx}", item)

        # 大额资金追踪
        for idx, item in enumerate(self.result.fund_tracking):
            self._add("fund_tracking", f"F-{idx}", item)

    def _add(self, evidence_type: str, evidence_id: str, item: Any):
        if evidence_type not in self._index:
            self._index[evidence_type] = {}
        self._index[evidence_type][evidence_id] = item

    def get(self, evidence_type: str, evidence_id: str) -> Optional[Any]:
        return self._index.get(evidence_type, {}).get(evidence_id)

    def exists(self, evidence_type: str, evidence_id: str) -> bool:
        return self.get(evidence_type, evidence_id) is not None

    def summary(self) -> dict[str, int]:
        return {k: len(v) for k, v in self._index.items()}


class SymbolicValidator:
    """AI 输出符号校验器。"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def validate_input(self, analysis_result: AnalysisResult) -> ValidationResult:
        """校验输入是否包含可翻译信号。"""
        errors = []
        warnings = []

        if not analysis_result.persons:
            errors.append("AnalysisResult 中无人员信息")

        total_signals = (
            sum(len(v) for v in analysis_result.anomalies.values())
            + sum(len(v) for v in analysis_result.patterns.values())
            + sum(len(v) for v in analysis_result.timeline_chains.values())
            + len(analysis_result.risk_assessment)
            + sum(len(v) for v in analysis_result.key_transactions.values())
            + len(analysis_result.fund_tracking)
        )

        if total_signals == 0:
            warnings.append("AnalysisResult 中无任何可翻译信号，AI 输出可能为空")

        return ValidationResult(通过=len(errors) == 0, 错误列表=errors, 警告列表=warnings)

    def validate_output(self, raw_output: Any, schema: Optional[dict] = None) -> ValidationResult:
        """校验 LLM 输出是否为合法结构化数据。"""
        errors = []
        warnings = []

        if raw_output is None:
            errors.append("AI 输出为空")
            return ValidationResult(通过=False, 错误列表=errors)

        if not isinstance(raw_output, dict):
            errors.append(f"AI 输出必须是 dict，实际是 {type(raw_output).__name__}")
            return ValidationResult(通过=False, 错误列表=errors)

        # 基础字段存在性检查
        if not raw_output:
            warnings.append("AI 输出为空字典")

        return ValidationResult(通过=len(errors) == 0, 错误列表=errors, 警告列表=warnings)

    def check_hallucination(
        self,
        ai_output: dict,
        analysis_result: AnalysisResult,
    ) -> ValidationResult:
        """幻觉检测：检查 evidence_refs 是否真实存在。"""
        errors = []
        warnings = []
        index = EvidenceIndex(analysis_result)
        refs = self._collect_evidence_refs(ai_output)

        if not refs:
            errors.append("AI 输出未包含任何 evidence_refs")

        for ref in refs:
            if not index.exists(ref.证据类型, ref.证据ID):
                errors.append(
                    f"证据引用不存在: 类型={ref.证据类型}, ID={ref.证据ID}"
                )

        return ValidationResult(通过=len(errors) == 0, 错误列表=errors, 警告列表=warnings)

    def check_consistency(
        self,
        ai_output: dict,
        analysis_result: AnalysisResult,
    ) -> ValidationResult:
        """一致性检测：检查 AI 结论中的数字是否与规则信号一致。"""
        errors = []
        warnings = []
        index = EvidenceIndex(analysis_result)
        refs = self._collect_evidence_refs(ai_output)

        # 从 AI 输出文本中提取金额、人数、次数等数字
        text = self._extract_text(ai_output)
        numbers_in_text = set(self._extract_numbers(text))

        for ref in refs:
            item = index.get(ref.证据类型, ref.证据ID)
            if item is None:
                continue
            source_numbers = self._extract_item_numbers(item)
            # 宽松策略：如果 AI 文本中的数字在源信号中完全找不到，给出警告
            for num in numbers_in_text:
                if not self._number_matches_source(num, source_numbers):
                    warnings.append(
                        f"AI 输出中的数字 {num} 可能不在引用的证据 {ref.证据类型}/{ref.证据ID} 中"
                    )

        return ValidationResult(通过=len(errors) == 0, 错误列表=errors, 警告列表=warnings)

    @staticmethod
    def _collect_evidence_refs(ai_output: dict, _refs: Optional[list] = None) -> list[EvidenceRef]:
        """递归收集 AI 输出中的所有 evidence_refs。"""
        refs = _refs if _refs is not None else []
        if isinstance(ai_output, dict):
            for key, value in ai_output.items():
                if key == "证据引用" and isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            try:
                                refs.append(EvidenceRef(**item))
                            except Exception:
                                pass
                else:
                    SymbolicValidator._collect_evidence_refs(value, refs)
        elif isinstance(ai_output, list):
            for item in ai_output:
                SymbolicValidator._collect_evidence_refs(item, refs)
        return refs

    @staticmethod
    def _extract_text(obj: Any) -> str:
        """从任意对象中提取文本。"""
        if isinstance(obj, str):
            return obj
        if isinstance(obj, dict):
            return " ".join(SymbolicValidator._extract_text(v) for v in obj.values())
        if isinstance(obj, list):
            return " ".join(SymbolicValidator._extract_text(v) for v in obj)
        return ""

    @staticmethod
    def _extract_numbers(text: str) -> list[float]:
        """从文本中提取数字（支持 1,234.56 格式）。"""
        pattern = re.compile(r"\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?")
        results = []
        for m in pattern.findall(text):
            try:
                results.append(float(m.replace(",", "")))
            except ValueError:
                pass
        return results

    @staticmethod
    def _extract_item_numbers(item: Any) -> list[float]:
        """从证据对象中提取可能的数字字段。"""
        numbers = []
        if hasattr(item, "model_dump"):
            data = item.model_dump()
        elif isinstance(item, dict):
            data = item
        else:
            return numbers

        for value in data.values():
            if isinstance(value, (int, float)):
                numbers.append(float(value))
            elif isinstance(value, str):
                numbers.extend(SymbolicValidator._extract_numbers(value))
        return numbers

    @staticmethod
    def _number_matches_source(num: float, source_numbers: list[float], tolerance: float = 0.01) -> bool:
        """判断数字是否匹配源信号（允许小误差，支持金额量级匹配）。"""
        for src in source_numbers:
            # 精确匹配或四舍五入后匹配
            if abs(num - src) < tolerance:
                return True
            # 支持文本中省略小数或单位的粗略匹配（如 50000 与 50000.0）
            if abs(round(num) - round(src)) < 1:
                return True
        return False
