#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import os

from src.datasource.payment import PaymentDataModel
from src.utils.config import Config
from src.utils.constants import DataSourceType, ColumnName

class AlipayDataModel(PaymentDataModel):
    """
    支付宝数据模型，用于加载和处理支付宝交易数据
    """
    def __init__(self, data_path=None, data=None, config=None):
        """
        初始化支付宝数据模型
        
        Parameters:
        -----------
        data_path : str, optional
            数据文件路径，如果提供则从文件加载数据
        data : pd.DataFrame, optional
            直接提供的数据，如果提供则使用此数据
        config : Config, optional
            配置对象，如果不提供则使用默认配置
        """
        # 调用父类初始化，指定数据源类型
        super().__init__(data_path, data, config, DataSourceType.ALIPAY)
        
        # 支付宝特有的借贷标识 (父类已经设置，但这里重写以确保正确值)
        self.credit_flag = self.config.get('data_sources.alipay.credit_flag', '收入')  # 收入
        self.debit_flag = self.config.get('data_sources.alipay.debit_flag', '支出')    # 支出
    
    def preprocess(self):
        """
        支付宝数据预处理
        """
        # 调用父类的预处理方法
        super().preprocess()
        
        # 支付宝特有的预处理逻辑可以在这里添加
        
        # 例如：处理交易状态
        if self.transaction_status_column in self.data.columns:
            # 可以对交易状态进行过滤或创建新标志
            # 例如，忽略已退款、已撤销的交易
            cancelled_mask = self.data[self.transaction_status_column].isin(['已退款', '已撤销', '交易关闭'])
            if cancelled_mask.any():
                self.data.loc[cancelled_mask, ColumnName.INCOME_AMOUNT] = 0
                self.data.loc[cancelled_mask, ColumnName.EXPENSE_AMOUNT] = 0
                self.logger.info(f"已将 {cancelled_mask.sum()} 条已取消的交易收支金额设为0")
        
        self.logger.info("支付宝特有的数据预处理完成") 