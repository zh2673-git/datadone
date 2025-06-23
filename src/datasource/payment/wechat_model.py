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

class WeChatDataModel(PaymentDataModel):
    """
    微信数据模型，用于加载和处理微信交易数据
    """
    def __init__(self, data_path=None, data=None, config=None):
        """
        初始化微信数据模型
        
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
        super().__init__(data_path, data, config, DataSourceType.WECHAT)
        
        # 微信特有的借贷标识 (父类已经设置，但这里重写以确保正确值)
        self.credit_flag = self.config.get('data_sources.wechat.credit_flag', '入')  # 收入
        self.debit_flag = self.config.get('data_sources.wechat.debit_flag', '出')    # 支出
    
    def preprocess(self):
        """
        微信数据预处理
        """
        # 调用父类的预处理方法
        super().preprocess()
        
        # 微信特有的预处理逻辑可以在这里添加
        
        # 例如：处理微信账号信息
        if self.account_column in self.data.columns and self.opposite_account_column in self.data.columns:
            # 可以为不同的账号分配分组或标签
            unique_accounts = self.data[self.account_column].dropna().unique()
            self.logger.info(f"微信数据包含 {len(unique_accounts)} 个不同的本方账号")
        
        self.logger.info("微信特有的数据预处理完成")
        
    def get_data_by_account(self, account: str) -> pd.DataFrame:
        """
        按微信账号筛选数据
        
        Parameters:
        -----------
        account : str
            微信账号
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        if self.account_column not in self.data.columns:
            self.logger.warning(f"数据中没有微信账号列 '{self.account_column}'")
            return pd.DataFrame()
        
        return self.filter_by_value(self.account_column, account)
    
    def get_data_by_opposite_account(self, opposite_account: str) -> pd.DataFrame:
        """
        按对方微信账号筛选数据
        
        Parameters:
        -----------
        opposite_account : str
            对方微信账号
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        if self.opposite_account_column not in self.data.columns:
            self.logger.warning(f"数据中没有对方微信账号列 '{self.opposite_account_column}'")
            return pd.DataFrame()
        
        return self.filter_by_value(self.opposite_account_column, opposite_account) 