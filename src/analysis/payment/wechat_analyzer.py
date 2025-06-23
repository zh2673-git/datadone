#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional
from datetime import datetime

from src.analysis.payment import PaymentAnalyzer
from src.datasource.payment.wechat_model import WeChatDataModel
from src.group import GroupManager

class WeChatAnalyzer(PaymentAnalyzer):
    """
    微信数据分析器，用于分析微信交易数据
    """
    def __init__(self, data_model: WeChatDataModel, group_manager: Optional[GroupManager] = None, config: Optional[Dict] = None):
        """
        初始化微信数据分析器
        
        Parameters:
        -----------
        data_model : WeChatDataModel
            微信数据模型
        group_manager : GroupManager, optional
            分组管理器
        config : dict, optional
            配置字典
        """
        if not isinstance(data_model, WeChatDataModel):
            raise TypeError("data_model必须是WeChatDataModel类型")
        
        super().__init__(data_model, group_manager, config)
        self.wechat_model = data_model
    
    # 微信特有的分析方法可以在此添加
    
    def analyze_by_account(self, account: str) -> pd.DataFrame:
        """
        按微信账号分析
        
        Parameters:
        -----------
        account : str
            微信账号
            
        Returns:
        --------
        pd.DataFrame
            分析结果
        """
        account_data = self.wechat_model.get_data_by_account(account)
        if account_data.empty:
            self.logger.warning(f"找不到微信账号 {account} 的数据")
            return pd.DataFrame()
            
        return self.analyze_frequency(account_data)
    
    def analyze_account_interactions(self, source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        分析不同微信账号之间的互动
        
        Parameters:
        -----------
        source_name : str, optional
            数据来源名称，如果提供，只分析此来源
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            账号互动分析结果
        """
        results = {}
        
        if source_name:
            sources_to_analyze = [source_name]
        else:
            sources_to_analyze = self.wechat_model.get_data_sources()
        
        for source in sources_to_analyze:
            source_data = self.wechat_model.data[self.wechat_model.data['数据来源'] == source]
            
            if (source_data.empty or 
                self.wechat_model.account_column not in source_data.columns or
                self.wechat_model.opposite_account_column not in source_data.columns):
                continue
                
            # 按本方账号和对方账号分组统计
            account_stats = source_data.groupby([
                self.wechat_model.account_column, 
                self.wechat_model.opposite_account_column
            ]).agg({
                '收入金额': ['count', 'sum'],
                '支出金额': ['count', 'sum'],
                self.wechat_model.date_column: ['min', 'max']
            }).reset_index()
            
            # 重命名列
            account_stats.columns = [
                '本方账号', '对方账号', 
                '收入次数', '收入总额', 
                '支出次数', '支出总额',
                '首次互动时间', '最近互动时间'
            ]
            
            # 计算互动统计
            account_stats['互动总次数'] = account_stats['收入次数'] + account_stats['支出次数']
            account_stats['互动金额'] = account_stats['收入总额'] + account_stats['支出总额']
            account_stats['互动时间跨度(天)'] = (
                account_stats['最近互动时间'] - account_stats['首次互动时间']
            ).dt.days + 1
            
            results[f"{source}_微信账号互动"] = account_stats.sort_values('互动总次数', ascending=False)
        
        return results 