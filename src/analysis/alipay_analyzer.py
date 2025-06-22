#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional
from datetime import datetime

from src.base import BaseAnalyzer
from src.datasource import AlipayDataModel
from src.group import GroupManager

class AlipayAnalyzer(BaseAnalyzer):
    """
    支付宝数据分析器，用于分析支付宝交易数据
    """
    def __init__(self, data_model: AlipayDataModel, group_manager: Optional[GroupManager] = None):
        """
        初始化支付宝数据分析器
        
        Parameters:
        -----------
        data_model : AlipayDataModel
            支付宝数据模型
        group_manager : GroupManager, optional
            分组管理器
        """
        if not isinstance(data_model, AlipayDataModel):
            raise TypeError("data_model必须是AlipayDataModel类型")
        
        super().__init__(data_model, group_manager)
        self.alipay_model = data_model
    
    def analyze(self, analysis_type: str = 'all', source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        执行支付宝数据分析, 按数据来源进行聚合.
        
        Parameters:
        -----------
        analysis_type : str
            分析类型，可选值为'frequency'(频率)或'all'(全部)
        source_name : str, optional
            数据来源名称 (例如 '吴平一家明细.xlsx'). 如果提供, 只分析此来源.
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果, 键为结果名 (例如 '吴平一家明细.xlsx_支付宝频率'), 值为结果数据
        """
        if analysis_type not in ['frequency', 'all']:
            raise ValueError("analysis_type必须是'frequency'或'all'")

        all_results = {}
        
        if source_name:
            sources_to_analyze = [source_name]
        else:
            sources_to_analyze = self.alipay_model.get_data_sources()

        if not sources_to_analyze:
            self.logger.warning("没有找到可分析的数据来源.")
            return {}
            
        for source in sources_to_analyze:
            results = self._analyze_for_source(source, analysis_type)
            all_results.update(results)
        
        self.results = all_results
        return all_results

    def _analyze_for_source(self, source_name: str, analysis_type: str) -> Dict[str, pd.DataFrame]:
        """
        为指定的数据来源执行分析
        
        Parameters:
        -----------
        source_name : str
            数据来源名称
        analysis_type : str
            分析类型
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果
        """
        results = {}
        source_data = self.alipay_model.data[self.alipay_model.data['数据来源'] == source_name]

        if source_data.empty:
            self.logger.warning(f"找不到数据来源 '{source_name}' 的数据")
            return results
        
        # 执行交易频率分析
        if analysis_type in ['frequency', 'all']:
            frequency_result = self.analyze_frequency(source_data)
            if not frequency_result.empty:
                results[f"{source_name}_支付宝频率"] = frequency_result
        
        return results

    def analyze_frequency(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析交易频率
        
        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据 (单一来源)
            
        Returns:
        --------
        pd.DataFrame
            交易频率分析结果
        """
        if data.empty:
            self.logger.warning("没有支付宝交易记录数据")
            return pd.DataFrame()

        # 获取相关列名
        name_col = self.alipay_model.name_column
        opposite_name_col = self.alipay_model.opposite_name_column
        date_col = self.alipay_model.date_column
        
        # 定义分组键和聚合操作
        group_cols = [name_col, opposite_name_col]
        agg_dict = {
            '收入金额': 'sum',
            '支出金额': 'sum',
            '交易金额': 'count'
        }
        
        # 按对方姓名进行分组统计
        result = data.groupby(group_cols).agg(agg_dict).reset_index()
        
        # 重命名列
        rename_dict = {
            '交易金额': '交易次数',
            '收入金额': '总收入',
            '支出金额': '总支出'
        }
        result.rename(columns=rename_dict, inplace=True)

        # 计算总金额
        result['交易总金额'] = result['总收入'] + result['总支出']

        # 计算时间跨度
        time_span = data.groupby(group_cols)[date_col].agg(['min', 'max']).reset_index()
        time_span['交易时间跨度'] = (time_span['max'] - time_span['min']).dt.days + 1
        
        result = pd.merge(result, time_span[group_cols + ['交易时间跨度']], on=group_cols, how='left')

        result['数据来源'] = data['数据来源'].iloc[0]
        
        return result.sort_values(by=[name_col, '交易总金额'], ascending=[True, False])
    
    def get_top_transactions(self, data: pd.DataFrame, top_n: int = 10, by_income: bool = True) -> pd.DataFrame:
        """
        获取指定数据子集中的最高交易记录
        
        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据
        top_n : int
            返回的记录数量
        by_income : bool
            是否按收入排序
            
        Returns:
        --------
        pd.DataFrame
            最高交易记录
        """
        if data.empty:
            return pd.DataFrame()
            
        sort_col = '收入金额' if by_income else '支出金额'
        return data.nlargest(top_n, sort_col) 