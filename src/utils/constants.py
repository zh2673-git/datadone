#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
常量定义文件，集中管理项目中的所有常量
"""

# 数据源类型
class DataSourceType:
    BANK = "bank"
    ALIPAY = "alipay"
    WECHAT = "wechat"
    CALL = "call"

# 交易类型
class TransactionType:
    TRANSFER = "转账"
    DEPOSIT = "存现"
    WITHDRAW = "取现"

# 借贷标识
class DirectionFlag:
    # 银行标识
    BANK_CREDIT = "贷"  # 收入
    BANK_DEBIT = "借"   # 支出
    
    # 支付宝标识
    ALIPAY_CREDIT = "收入"
    ALIPAY_DEBIT = "支出"
    
    # 微信标识
    WECHAT_CREDIT = "入"
    WECHAT_DEBIT = "出"

# 列名
class ColumnName:
    # 通用列名
    NAME = "本方姓名"
    DATE = "交易日期"
    AMOUNT = "交易金额"
    DIRECTION = "借贷标识"
    OPPOSITE_NAME = "对方姓名"
    BALANCE = "账户余额"
    DATA_SOURCE = "数据来源"
    INCOME_AMOUNT = "收入金额"
    EXPENSE_AMOUNT = "支出金额"
    SPECIAL_DATE = "特殊日期名称"
    
    # 银行特有列名
    BANK_NAME = "银行类型"
    SUMMARY = "交易摘要"
    REMARK = "交易备注"
    CASH_OPERATION = "存取现标识"
    
    # 通话记录特有列名
    CALL_TYPE = "呼叫类型"
    CALL_DURATION = "通话时长(秒)"
    OPPOSITE_NUMBER = "对方号码"
    CALL_COUNT = "通话次数"
    CALL_IN_COUNT = "被叫次数"  
    CALL_OUT_COUNT = "主叫次数"
    SMS_COUNT = "短信次数"

# 分析结果类型
class AnalysisType:
    FREQUENCY = "frequency"
    CASH = "cash"
    ALL = "all"

# 文件路径和目录
class FilePath:
    CONFIG = "config.json"
    OUTPUT_DIR = "output"
    LOGS_DIR = "logs"
    DEFAULT_LOG = "logs/app.log"
    
# 错误信息
class ErrorMessage:
    NO_DATA_MODEL = "未提供数据模型"
    EMPTY_DATA = "数据为空"
    NO_GROUP_MANAGER = "未提供分组管理器，无法按组分析"
    INVALID_DATA_TYPE = "{} 必须是 {} 类型"
    MISSING_COLUMNS = "数据缺少必要的列: {}" 