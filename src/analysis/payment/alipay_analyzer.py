#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional
from datetime import datetime

from src.analysis.payment import PaymentAnalyzer
from ...model.alipay_model import AlipayDataModel
from src.utils.group import GroupManager

class AlipayAnalyzer(PaymentAnalyzer):
    """
    支付宝数据分析器，用于分析支付宝交易数据
    """
    def __init__(self, data_model: AlipayDataModel, group_manager: Optional[GroupManager] = None, config: Optional[Dict] = None):
        """
        初始化支付宝数据分析器
        
        Parameters:
        -----------
        data_model : AlipayDataModel
            支付宝数据模型
        group_manager : GroupManager, optional
            分组管理器
        config : dict, optional
            配置字典
        """
        if not isinstance(data_model, AlipayDataModel):
            raise TypeError("data_model必须是AlipayDataModel类型")
        
        super().__init__(data_model, group_manager, config)
        self.alipay_model = data_model
    
    # 支付宝特有的分析方法可以在此添加
    
    def analyze_transaction_types(self, source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        分析支付宝交易类型分布
        
        Parameters:
        -----------
        source_name : str, optional
            数据来源名称，如果提供，只分析此来源
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            交易类型分析结果
        """
        results = {}
        
        if source_name:
            sources_to_analyze = [source_name]
        else:
            sources_to_analyze = self.alipay_model.get_data_sources()
        
        for source in sources_to_analyze:
            source_data = self.alipay_model.data[self.alipay_model.data['数据来源'] == source]
            
            if source_data.empty or self.alipay_model.transaction_type_column not in source_data.columns:
                continue
                
            # 按交易类型分组统计
            type_stats = source_data.groupby(self.alipay_model.transaction_type_column).agg({
                '收入金额': ['count', 'sum'],
                '支出金额': ['count', 'sum']
            }).reset_index()
            
            # 重命名列
            type_stats.columns = [self.alipay_model.transaction_type_column, '收入次数', '收入总额', '支出次数', '支出总额']
            
            # 计算总交易次数和总金额
            type_stats['总交易次数'] = type_stats['收入次数'] + type_stats['支出次数']
            type_stats['总金额'] = type_stats['收入总额'] + type_stats['支出总额']
            
            results[f"{source}_支付宝交易类型"] = type_stats.sort_values('总交易次数', ascending=False)
        
        return results