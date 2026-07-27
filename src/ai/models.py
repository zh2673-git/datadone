#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""AI 协同层数据模型（数据规矩）"""

from typing import Optional, List, Any
from pydantic import BaseModel, Field


class EvidenceRef(BaseModel):
    """证据引用：每条 AI 结论必须能回溯到规则信号。"""
    证据类型: str  # anomaly / pattern / timeline / risk / key_transaction / fund_tracking
    证据ID: str
    说明: str = ""


class NarrativeBlock(BaseModel):
    """叙事块：AI 对规则信号的自然语言翻译。"""
    标题: str
    内容: str
    级别: str  # case / person / clue
    本方姓名: Optional[str] = None
    证据引用: List[EvidenceRef] = Field(default_factory=list)
    置信度: str = "中"  # 高 / 中 / 低


class Hypothesis(BaseModel):
    """调查假设：由多个信号组合而成的可验证推断。"""
    假设ID: str
    标题: str
    描述: str
    验证方向: List[str] = Field(default_factory=list)
    证据引用: List[EvidenceRef] = Field(default_factory=list)
    风险等级: str = "中"  # 高 / 中 / 低


class QAPair(BaseModel):
    """问答结果：自然语言提问的结构化回答。"""
    问题: str
    回答: str
    证据引用: List[EvidenceRef] = Field(default_factory=list)
    是否可回答: bool = True
    未回答原因: str = ""


class AIInsight(BaseModel):
    """单个人员的 AI 分析产物。"""
    本方姓名: str
    案件级叙事: List[NarrativeBlock] = Field(default_factory=list)
    人员级叙事: List[NarrativeBlock] = Field(default_factory=list)
    线索级叙事: List[NarrativeBlock] = Field(default_factory=list)
    调查假设: List[Hypothesis] = Field(default_factory=list)
    问答历史: List[QAPair] = Field(default_factory=list)


class ValidationResult(BaseModel):
    """符号校验结果。"""
    通过: bool
    错误列表: List[str] = Field(default_factory=list)
    警告列表: List[str] = Field(default_factory=list)


class GenerationResult(BaseModel):
    """LLM 生成结果通用封装。"""
    成功: bool
    原始输出: str = ""
    结构化输出: Optional[Any] = None
    错误信息: str = ""
