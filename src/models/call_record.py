from typing import Optional
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class StandardCallRecord(BaseModel):
    """统一话单数据结构"""

    model_config = ConfigDict(from_attributes=True)

    通话开始时间: Optional[datetime] = None
    本方姓名: str
    本方号码: str = ""
    对方姓名: str
    对方号码: str = ""
    对方单位名称: Optional[str] = ""
    对方职务: Optional[str] = ""
    呼叫类型: str = ""
    通话时长: int = 0
    数据来源: str = ""
