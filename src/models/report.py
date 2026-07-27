from typing import Optional, List, Dict

from pydantic import BaseModel, ConfigDict

from src.models.analysis_result import (
    PersonBaseline, AnomalyItem, PatternMatch, TimelineChain, RiskAssessment,
    HabitInterestItem
)
from src.ai.models import AIInsight


class ExportConfig(BaseModel):
    """导出配置"""
    model_config = ConfigDict(from_attributes=True)

    output_dir: str = "output"
    excel_template: Optional[str] = None
    word_template: Optional[str] = None
    report_version: str = "new"
    highlight_large_amount: bool = True
    large_amount_threshold: float = 50000


class PersonReportData(BaseModel):
    """单人报告数据"""
    model_config = ConfigDict(from_attributes=True)

    本方姓名: str

    # 资金体量
    银行总进账: float = 0.0
    银行总出账: float = 0.0
    微信总进账: float = 0.0
    微信总出账: float = 0.0
    支付宝总进账: float = 0.0
    支付宝总出账: float = 0.0
    时间跨度: str = ""
    账户余额: Optional[float] = None
    最常用银行: Optional[str] = None

    # 活跃时间和交易对手
    各平台年度TOP3: dict[str, list[dict]] = {}
    交易对手TOP3: list[dict] = []

    # 存取现和大额
    存现总额: float = 0.0
    取现总额: float = 0.0
    单笔万以上存取现: dict = {}
    银行总转账金额: float = 0.0
    单笔5万以上转账: dict = {}

    # 纯进/出账
    纯收入对手: list[dict] = []
    纯支出对手: list[dict] = []

    # 特殊金额
    爱情数字交易: list[dict] = []
    其他特殊金额交易: list[dict] = []

    # 特殊日期
    特殊日期交易: list[dict] = []

    # 重点收支
    工作收入: dict = {}
    房产收入: dict = {}
    房产支出: dict = {}
    租金收入: dict = {}
    租金支出: dict = {}
    车辆收入: dict = {}
    车辆支出: dict = {}
    证券收入: dict = {}
    证券支出: dict = {}

    # 重点人员
    存取现话单匹配: list[dict] = []
    大额资金话单匹配: list[dict] = []
    大额资金跟踪层级: list[dict] = []

    # ========== 新增：三阶解构方案 ==========
    # 行为基线
    行为基线: Optional[PersonBaseline] = None

    # 异常识别
    异常列表: list[AnomalyItem] = []

    # 行为模式
    行为模式: list[PatternMatch] = []

    # 时序链
    时序链: list[TimelineChain] = []

    # 风险研判
    风险研判: Optional[RiskAssessment] = None

    # 习惯兴趣
    习惯兴趣: list[HabitInterestItem] = []

    # AI 分析产物（可选注入）
    ai_insights: Optional[AIInsight] = None


class ReportData(BaseModel):
    """报告完整数据 - 导出器的唯一输入"""
    model_config = ConfigDict(from_attributes=True)

    # 基本信息
    persons: list[str] = []
    data_types: list[str] = []
    bank_names: list[str] = []

    # Excel数据
    summary_table: list[dict] = []
    bill_frequency: list[dict] = []
    call_frequency: list[dict] = []
    cross_analysis: dict[str, list[dict]] = {}
    bank_raw: list[dict] = []
    wechat_raw: list[dict] = []
    alipay_raw: list[dict] = []
    advanced_analysis: list[dict] = []
    fund_tracking: list[dict] = []

    # Word数据
    person_reports: dict[str, PersonReportData] = {}
    cross_analysis_summary: list[dict] = []

    # ========== 新增：三阶解构方案 Excel数据 ==========
    baseline_data: list[dict] = []
    anomaly_data: list[dict] = []
    pattern_data: list[dict] = []
    timeline_data: list[dict] = []
    risk_data: list[dict] = []
    cash_detail_data: list[dict] = []
    key_transaction_data: list[dict] = []
    habit_interest_data: list[dict] = []

    # 新增：AI 摘要数据（Excel AI 摘要工作表）
    ai_summary_data: list[dict] = []
