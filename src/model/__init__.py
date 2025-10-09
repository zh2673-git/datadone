#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据模型模块
包含所有数据模型的基类和具体实现
"""

"""
模型模块，包含所有数据模型类
"""

from .base_model import BaseDataModel
from .bank_model import BankDataModel
from .call_model import CallDataModel
from .payment_model import PaymentDataModel
from .alipay_model import AlipayDataModel
from .wechat_model import WeChatDataModel

__all__ = [
    'BaseDataModel',
    'BankDataModel', 
    'CallDataModel',
    'PaymentDataModel',
    'AlipayDataModel',
    'WeChatDataModel'
]