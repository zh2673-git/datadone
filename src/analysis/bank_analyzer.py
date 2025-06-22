#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional
from datetime import datetime

from src.base import BaseAnalyzer
from src.datasource import BankDataModel
from src.group import GroupManager

class BankAnalyzer(BaseAnalyzer):
    """
    银行数据分析器，用于分析银行交易数据
    """
    def __init__(self, data_model: BankDataModel, group_manager: Optional[GroupManager] = None):
        """
        初始化银行数据分析器
        
        Parameters:
        -----------
        data_model : BankDataModel
            银行数据模型
        group_manager : GroupManager, optional
            分组管理器
        """
        if not isinstance(data_model, BankDataModel):
            raise TypeError("data_model必须是BankDataModel类型")
        
        super().__init__(data_model, group_manager)
        self.bank_model = data_model

    def analyze(self, analysis_type: str = 'all', source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        执行银行数据分析, 按数据来源进行聚合.
        
        Parameters:
        -----------
        analysis_type : str, optional
            分析类型，可选值为'frequency'(交易频率分析)、'cash'(存取现分析)或'all'(全部分析)
        source_name : str, optional
            数据来源名称 (例如 '吴平一家明细.xlsx'). 如果提供, 只分析此来源.
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果，键为结果名 (例如 '吴平一家明细.xlsx_存取现分析'), 值为结果数据
        """
        if analysis_type not in ['frequency', 'cash', 'all']:
            raise ValueError("analysis_type必须是'frequency'、'cash'或'all'")
        
        all_results = {}
        
        if source_name:
            sources_to_analyze = [source_name]
        else:
            sources_to_analyze = self.bank_model.get_data_sources()

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
        source_data = self.bank_model.data[self.bank_model.data['数据来源'] == source_name]
        
        if source_data.empty:
            self.logger.warning(f"找不到数据来源 '{source_name}' 的数据")
            return results
            
        # 1. 存取现分析
        if analysis_type in ['cash', 'all']:
            cash_result = self.analyze_cash_operations(source_data)
            if not cash_result.empty:
                results[f"{source_name}_存取现分析"] = cash_result

        # 2. 交易频率分析
        if analysis_type in ['frequency', 'all']:
            frequency_result = self.analyze_frequency(source_data)
            if not frequency_result.empty:
                results[f"{source_name}_频率分析"] = frequency_result

        return results

    def analyze_cash_operations(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析存取现操作
        
        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据 (应为单一数据来源的子集)
            
        Returns:
        --------
        pd.DataFrame
            存取现分析结果
        """
        if data.empty:
            return pd.DataFrame()

        cash_ops = data[data['存取现标识'].isin(['存现', '取现'])].copy()
        if cash_ops.empty:
            return pd.DataFrame()
            
        # 按'本方姓名'和'存取现标识'进行分组
        summary = cash_ops.groupby(['本方姓名', '存取现标识']).agg(
            交易次数=('交易金额', 'count'),
            总金额=('交易金额', lambda x: x.abs().sum()),
            平均金额=('交易金额', lambda x: x.abs().mean()),
            最早交易日=('交易日期', 'min'),
            最晚交易日=('交易日期', 'max')
        ).reset_index()

        summary['总金额'] = summary['总金额'].round(2)
        summary['平均金额'] = summary['平均金额'].round(2)

        # 添加数据来源列
        summary['数据来源'] = data['数据来源'].iloc[0]

        return summary.sort_values(by=['本方姓名', '存取现标识'])

    def analyze_frequency(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析交易频率
        
        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据 (应为单一数据来源的子集)
            
        Returns:
        --------
        pd.DataFrame
            交易频率分析结果
        """
        if data.empty:
            return pd.DataFrame()
        
        transfer_data = data[data['存取现标识'] == '转账'].copy()
        if transfer_data.empty:
            return pd.DataFrame()

        # 定义分组键
        grouping_keys = ['本方姓名', '对方姓名']
        
        # 准备聚合字典
        agg_dict = {
            '收入金额': 'sum',
            '支出金额': 'sum',
            '交易日期': 'count',
        }
        
        # 按分组键进行统计
        grouped = transfer_data.groupby(grouping_keys).agg(agg_dict).reset_index()
        
        # 重命名聚合列
        rename_map = {
            '交易日期': '交易次数',
            '收入金额': '总收入',
            '支出金额': '总支出',
        }
        grouped = grouped.rename(columns=rename_map)

        # 计算交易总额
        grouped['交易总金额'] = grouped['总收入'] + grouped['总支出']

        # 计算时间跨度
        time_span = transfer_data.groupby(grouping_keys)['交易日期'].agg(['min', 'max']).reset_index()
        time_span['交易时间跨度'] = (time_span['max'] - time_span['min']).dt.days + 1
        
        # 合并结果
        result = pd.merge(grouped, time_span[grouping_keys + ['交易时间跨度']], on=grouping_keys, how='left')

        # 添加数据来源
        result['数据来源'] = data['数据来源'].iloc[0]
        
        # 排序
        return result.sort_values(by=['本方姓名', '交易总金额'], ascending=[True, False])

    def get_top_transactions(self, data: pd.DataFrame, top_n: int = 10, by_income: bool = True) -> pd.DataFrame:
        """
        获取指定数据子集中的最高交易记录
        
        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据
        top_n : int, optional
            返回的记录数
        by_income : bool, optional
            True表示按收入排序, False表示按支出排序
            
        Returns:
        --------
        pd.DataFrame
            最高交易记录
        """
        if data.empty:
            return pd.DataFrame()
            
        sort_col = '收入金额' if by_income else '支出金额'
        return data.nlargest(top_n, sort_col)

    def get_top_cash_transactions_by_source_and_type(self, source_name: str, cash_type: str, top_n: int = 5, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取指定数据来源和类型的单笔最高存取现交易。

        Parameters:
        -----------
        source_name : str
            数据来源名称。
        cash_type : str
            存取现类型（'存现' 或 '取现'）。
        top_n : int, optional
            返回的记录数，默认为5。
        person_name : str, optional
            本方姓名，如果提供则只筛选该人员的交易。

        Returns:
        --------
        pd.DataFrame
            单笔最高存取现交易记录。
        """
        source_data = self.bank_model.data[self.bank_model.data['数据来源'] == source_name]
        if source_data.empty:
            return pd.DataFrame()

        cash_data = source_data[source_data['存取现标识'] == cash_type].copy()
        
        if person_name:
            cash_data = cash_data[cash_data['本方姓名'] == person_name].copy()

        if cash_data.empty:
            return pd.DataFrame()
        
        # 根据存取现类型确定金额列
        amount_col = '收入金额' if cash_type == '存现' else '支出金额'
        
        return cash_data.nlargest(top_n, amount_col) 