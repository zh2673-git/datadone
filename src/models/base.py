from enum import Enum


class DataSourceType(str, Enum):
    """数据源类型"""
    BANK = "bank"
    WECHAT = "wechat"
    ALIPAY = "alipay"
    CALL = "call"


class CashType(str, Enum):
    """存取现类型"""
    DEPOSIT = "存现"
    WITHDRAW = "取现"
    TRANSFER = "转账"
