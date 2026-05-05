from typing import Optional
from datetime import date

from pydantic import BaseModel, ConfigDict


class StandardTransaction(BaseModel):
    """统一交易数据结构 - 所有平台标准化后的交易记录"""

    model_config = ConfigDict(from_attributes=True)

    交易日期: Optional[date] = None
    交易时间: Optional[str] = None
    本方姓名: str
    本方账号: Optional[str] = None
    对方姓名: Optional[str] = ""
    对方账号: Optional[str] = ""

    交易金额: float = 0.0
    借贷标识: str = ""
    收入金额: float = 0.0
    支出金额: float = 0.0
    账户余额: Optional[float] = None

    交易类型: Optional[str] = ""
    交易摘要: Optional[str] = ""
    交易备注: Optional[str] = ""

    存取现标识: str = "转账"
    特殊日期名称: Optional[str] = ""

    银行类型: Optional[str] = ""
    数据来源: str = ""
    平台: str = ""
