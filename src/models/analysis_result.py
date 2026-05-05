from typing import Optional, List, Dict
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class CashRecognitionItem(BaseModel):
    """存取现识别结果"""
    model_config = ConfigDict(from_attributes=True)

    index: int
    cash_type: str
    confidence: float
    reason: str


class FrequencyItem(BaseModel):
    """频率分析结果"""
    model_config = ConfigDict(from_attributes=True)

    platform: str
    data_source: str
    本方姓名: str
    对方姓名: str
    收入总额: float = 0.0
    支出总额: float = 0.0
    交易次数: int = 0
    交易总金额: float = 0.0
    交易时间跨度: Optional[int] = None


class CallFrequencyItem(BaseModel):
    """话单频率分析结果"""
    model_config = ConfigDict(from_attributes=True)

    platform: str = "话单"
    data_source: str = ""
    本方姓名: str
    对方姓名: str
    对方号码: str = ""
    对方单位名称: Optional[str] = None
    对方职务: Optional[str] = None
    通话次数: int = 0
    主叫次数: int = 0
    被叫次数: int = 0
    通话总时长秒: float = 0.0
    通话总时长分钟: float = 0.0
    通话占比: Optional[str] = None
    特殊时间次数: int = 0


class CrossAnalysisItem(BaseModel):
    """综合交叉分析结果"""
    model_config = ConfigDict(from_attributes=True)

    分析基准: str
    本方姓名: str
    对方姓名: str
    对方号码: Optional[str] = None
    对方单位名称: Optional[str] = None
    对方职务: Optional[str] = None
    通话次数: Optional[int] = None
    通话总时长: Optional[float] = None
    收入总额: Optional[float] = None
    支出总额: Optional[float] = None
    交易次数: Optional[int] = None
    平台: Optional[str] = None
    平台金额分布: Optional[str] = None
    银行_收入总额: Optional[float] = None
    银行_支出总额: Optional[float] = None
    银行_交易次数: Optional[int] = None
    微信_收入总额: Optional[float] = None
    微信_支出总额: Optional[float] = None
    微信_交易次数: Optional[int] = None
    支付宝_收入总额: Optional[float] = None
    支付宝_支出总额: Optional[float] = None
    支付宝_交易次数: Optional[int] = None


class KeyTransactionItem(BaseModel):
    """重点收支识别结果"""
    model_config = ConfigDict(from_attributes=True)

    index: int
    本方姓名: str
    交易日期: Optional[date] = None
    交易金额: float = 0.0
    交易方向: str = ""
    重点类型: str = ""
    重点子类: str = ""
    匹配关键词: str = ""
    置信度: float = 0.0


class SpecialDateItem(BaseModel):
    """特殊日期分析结果"""
    model_config = ConfigDict(from_attributes=True)

    index: int
    交易日期: Optional[date] = None
    特殊日期名称: str = ""


class SpecialAmountItem(BaseModel):
    """特殊金额分析结果"""
    model_config = ConfigDict(from_attributes=True)

    index: int
    交易金额: float = 0.0
    特殊类型: str = ""
    特殊金额名: str = ""


class SpecialAnalysisOutput(BaseModel):
    """特殊分析输出"""
    model_config = ConfigDict(from_attributes=True)

    dates: dict[str, list[SpecialDateItem]] = {}
    amounts: dict[str, list[SpecialAmountItem]] = {}


class FundTrackingItem(BaseModel):
    """大额资金追踪结果"""
    model_config = ConfigDict(from_attributes=True)

    分析类型: str = ""
    追踪层级: int = 0
    核心人员: str = ""
    关联人员: str = ""
    交易日期: Optional[date] = None
    交易金额: Optional[float] = None
    交易方向: str = ""
    大额级别: Optional[str] = None
    数据来源: Optional[str] = None
    资金流向: str = ""
    追踪说明: str = ""
    月份: Optional[str] = None
    通话日期: Optional[date] = None
    通话对方: Optional[str] = None
    通话对方单位: Optional[str] = None


class FundTrackingOutput(BaseModel):
    """大额资金追踪输出"""
    model_config = ConfigDict(from_attributes=True)

    tracking: list[FundTrackingItem] = []
    cash_call_match: list[FundTrackingItem] = []


class AdvancedAnalysisResult(BaseModel):
    """高级分析结果"""
    model_config = ConfigDict(from_attributes=True)

    时间模式: list[dict] = []
    金额模式: list[dict] = []
    异常交易: list[dict] = []
    交易模式: list[dict] = []


# ======================================================================
# 新增数据模型 - 三阶解构方案
# ======================================================================

class MonthlyMetric(BaseModel):
    """月度指标"""
    model_config = ConfigDict(from_attributes=True)

    年月: str = ""
    收入: float = 0.0
    支出: float = 0.0
    交易次数: int = 0
    对手数: int = 0


class PersonBaseline(BaseModel):
    """个人行为基线 - 自我参照系"""
    model_config = ConfigDict(from_attributes=True)

    本方姓名: str
    数据充足度: str = "不足"   # "充足"(≥6月) / "有限"(3-6月) / "不足"(<3月)
    数据月数: int = 0

    # 资金基线
    月均收入: float = 0.0
    月均收入_std: float = 0.0
    月均支出: float = 0.0
    月均支出_std: float = 0.0
    月均交易次数: float = 0.0
    月均交易次数_std: float = 0.0
    月均交易对手数: float = 0.0
    月均交易对手数_std: float = 0.0

    # 金额分布
    单笔金额均值: float = 0.0
    单笔金额中位数: float = 0.0
    单笔金额_P25: float = 0.0
    单笔金额_P75: float = 0.0

    # 时间基线
    工作时间交易占比: float = 0.0  # 9-17点
    深夜交易占比: float = 0.0       # 22-6点
    周末交易占比: float = 0.0

    # 现金基线
    存取现月均金额: float = 0.0
    存取现占收支比: float = 0.0

    # 通话基线
    月均通话次数: float = 0.0
    月均通话时长分钟: float = 0.0
    月均通话对手数: float = 0.0

    # 趋势
    收入趋势: str = "平稳"   # "上升"/"下降"/"平稳"
    支出趋势: str = "平稳"
    对手数趋势: str = "平稳"

    # 月度明细
    月度明细: list[MonthlyMetric] = []


class AnomalyItem(BaseModel):
    """单个异常项"""
    model_config = ConfigDict(from_attributes=True)

    异常类型: str = ""       # 资金异常/时间异常/频率异常/金额异常/对手异常/行为异常
    异常子类: str = ""       # 月度收入突增/深夜交易/规避行为/...
    偏离度: float = 0.0     # 偏离基线的σ倍数（行为异常用-1标记）
    严重程度: str = ""       # 高(>3σ)/中(2-3σ)/低(规则触发)
    本方姓名: str = ""
    对方姓名: str = ""
    交易日期: str = ""
    交易金额: float = 0.0
    基线均值: float = 0.0   # 对比的基线值
    说明: str = ""


class PatternMatch(BaseModel):
    """行为模式匹配结果"""
    model_config = ConfigDict(from_attributes=True)

    模式编号: str = ""       # P1-P5
    模式名称: str = ""       # 周期性收入模式/...
    匹配度: float = 0.0      # 0.0-1.0
    涉及对手: str = ""
    关键证据: str = ""
    满足条件: list[str] = []
    未满足条件: list[str] = []
    报告用语: str = ""       # "存在周期性收入特征"


class TimelineEvent(BaseModel):
    """时序链中的单个事件"""
    model_config = ConfigDict(from_attributes=True)

    时间: str = ""           # ISO格式时间
    事件类型: str = ""       # "通话" / "资金"
    方向: str = ""           # "主叫"/"被叫" / "收入"/"支出"
    金额: float = 0.0        # 通话为时长(秒)，资金为金额
    对手: str = ""
    平台: str = ""           # "银行"/"微信"/"支付宝"/"话单"
    备注: str = ""


class TimelineChain(BaseModel):
    """一条时序链"""
    model_config = ConfigDict(from_attributes=True)

    本方姓名: str = ""
    对方姓名: str = ""
    事件序列: list[TimelineEvent] = []
    链模式: str = ""         # "通话→资金"/"通话→资金→通话"
    链强度: str = ""         # 弱/中/强/很强
    重复次数: int = 0        # 该对手的同类链出现次数
    时间窗口: int = 24       # 检测窗口(小时)
    关键证据描述: str = ""


class KeyPerson(BaseModel):
    """重点人员"""
    model_config = ConfigDict(from_attributes=True)

    排名: int = 0
    对方姓名: str = ""
    对方单位: str = ""
    对方职务: str = ""
    关联度评分: float = 0.0
    通话次数: int = 0
    资金往来金额: float = 0.0
    时序链数: int = 0
    关联特征: str = ""


class RiskAssessment(BaseModel):
    """风险研判结果"""
    model_config = ConfigDict(from_attributes=True)

    本方姓名: str = ""

    # 四维度评分
    异常偏离度得分: float = 0.0   # 0-30
    异常偏离度说明: str = ""
    行为模式得分: float = 0.0     # 0-25
    行为模式说明: str = ""
    证据链得分: float = 0.0       # 0-25
    证据链说明: str = ""
    规模得分: float = 0.0         # 0-20
    规模说明: str = ""

    # 汇总
    综合风险分数: float = 0.0     # 0-100
    综合风险等级: str = ""        # 高风险/中风险/低风险

    # 重点人员
    重点人员: list[KeyPerson] = []

    # 证据充分度
    证据充分度: str = ""          # 充分/较充分/待补充
    已有证据: list[str] = []
    待补充证据: list[str] = []

    # 调查建议
    调查方向建议: list[str] = []
    重点时段: str = ""


class AnalysisResult(BaseModel):
    """分析引擎总结果 - 所有子分析的统一输出"""
    model_config = ConfigDict(from_attributes=True)

    persons: list[str] = []
    data_sources: dict[str, str] = {}

    cash_recognition: dict[str, list[CashRecognitionItem]] = {}
    frequency: dict[str, list[FrequencyItem]] = {}
    call_frequency: list[CallFrequencyItem] = []
    cross_analysis: dict[str, list[CrossAnalysisItem]] = {}
    key_transactions: dict[str, list[KeyTransactionItem]] = {}
    special_dates: dict[str, list[SpecialDateItem]] = {}
    special_amounts: dict[str, list[SpecialAmountItem]] = {}
    fund_tracking: list[FundTrackingItem] = []
    cash_call_match: list[FundTrackingItem] = []
    advanced: dict[str, AdvancedAnalysisResult] = {}

    bank_cash_deposit_total: dict[str, float] = {}
    bank_cash_withdrawal_total: dict[str, float] = {}

    # 新增：三阶解构方案
    baseline: dict[str, PersonBaseline] = {}
    anomalies: dict[str, list[AnomalyItem]] = {}
    patterns: dict[str, list[PatternMatch]] = {}
    timeline_chains: dict[str, list[TimelineChain]] = {}
    risk_assessment: dict[str, RiskAssessment] = {}
