#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional, Any
from datetime import datetime

from src.base import BaseAnalyzer
from src.datasource.payment import PaymentDataModel
from src.group import GroupManager
from src.utils.constants import ColumnName
from src.utils.exceptions import InvalidArgumentError

class PaymentAnalyzer(BaseAnalyzer):
    """
    支付数据分析器基类，为支付宝、微信等支付类数据分析提供共同的分析逻辑
    """
    def __init__(self, data_model: PaymentDataModel, group_manager: Optional[GroupManager] = None):
        """
        初始化支付数据分析器
        
        Parameters:
        -----------
        data_model : PaymentDataModel
            支付数据模型
        group_manager : GroupManager, optional
            分组管理器
        """
        if not isinstance(data_model, PaymentDataModel):
            raise TypeError("data_model必须是PaymentDataModel类型")
        
        super().__init__(data_model, group_manager)
        self.payment_model = data_model
    
    def analyze(self, analysis_type: str = 'all', source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        执行支付数据分析, 按数据来源进行聚合。
        
        Parameters:
        -----------
        analysis_type : str
            分析类型，可选值为'frequency'(频率)或'all'(全部)
        source_name : str, optional
            数据来源名称 (例如 '吴平一家明细.xlsx')。 如果提供, 只分析此来源。
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果, 键为结果名 (例如 '吴平一家明细.xlsx_支付宝频率'), 值为结果数据
        """
        if analysis_type not in ['frequency', 'all']:
            raise InvalidArgumentError(f"analysis_type必须是'frequency'或'all'")

        all_results = {}
        
        if source_name:
            sources_to_analyze = [source_name]
        else:
            sources_to_analyze = self.payment_model.get_data_sources()

        if not sources_to_analyze:
            self.logger.warning("没有找到可分析的数据来源")
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
        source_data = self.payment_model.data[self.payment_model.data[ColumnName.DATA_SOURCE] == source_name]

        if source_data.empty:
            self.logger.warning(f"找不到数据来源 '{source_name}' 的数据")
            return results
        
        # 执行交易频率分析
        if analysis_type in ['frequency', 'all']:
            frequency_result = self.analyze_frequency(source_data)
            if not frequency_result.empty:
                # 使用子类名称作为类型标识，如"支付宝"或"微信"
                payment_type = self.__class__.__name__.replace('Analyzer', '')
                results[f"{source_name}_{payment_type}频率"] = frequency_result
        
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
            self.logger.warning("没有交易记录数据")
            return pd.DataFrame()

        # 获取相关列名
        name_col = self.payment_model.name_column
        opposite_name_col = self.payment_model.opposite_name_column
        date_col = self.payment_model.date_column
        
        # 定义分组键和聚合操作
        group_cols = [name_col, opposite_name_col]
        agg_dict = {
            ColumnName.INCOME_AMOUNT: 'sum',
            ColumnName.EXPENSE_AMOUNT: 'sum',
            self.payment_model.amount_column: 'count'
        }
        
        # 按对方姓名进行分组统计
        result = data.groupby(group_cols).agg(agg_dict).reset_index()
        
        # 重命名列
        rename_dict = {
            self.payment_model.amount_column: '交易次数',
            ColumnName.INCOME_AMOUNT: '总收入',
            ColumnName.EXPENSE_AMOUNT: '总支出'
        }
        result.rename(columns=rename_dict, inplace=True)

        # 计算总金额
        result['交易总金额'] = result['总收入'] + result['总支出']

        # 计算时间跨度
        time_span = data.groupby(group_cols)[date_col].agg(['min', 'max']).reset_index()
        time_span['交易时间跨度'] = (time_span['max'] - time_span['min']).dt.days + 1
        
        result = pd.merge(result, time_span[group_cols + ['交易时间跨度']], on=group_cols, how='left')

        # 添加数据来源
        result[ColumnName.DATA_SOURCE] = data[ColumnName.DATA_SOURCE].iloc[0]
        
        return result.sort_values(by=[name_col, '交易总金额'], ascending=[True, False])
    
    def analyze_by_person(self, person_name: str) -> pd.DataFrame:
        """
        按人进行分析
        
        Parameters:
        -----------
        person_name : str
            人名
            
        Returns:
        --------
        pd.DataFrame
            分析结果
        """
        person_data = self.payment_model.get_data_by_person(person_name)
        if person_data.empty:
            self.logger.warning(f"找不到 {person_name} 的数据")
            return pd.DataFrame()
        
        # 执行频率分析
        return self.analyze_frequency(person_data)
    
    def analyze_by_group(self, group_name: str) -> Dict[str, pd.DataFrame]:
        """
        按组进行分析
        
        Parameters:
        -----------
        group_name : str
            组名
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果，键为人名，值为该人的分析结果
        """
        if self.group_manager is None:
            self.logger.warning("未提供分组管理器，无法按组分析")
            raise ValueError("未提供分组管理器，无法按组分析")
        
        group_members = self.group_manager.get_group(group_name)
        if not group_members:
            self.logger.warning(f"找不到分组 {group_name}")
            return {}
        
        # 对每个成员进行分析
        results = {}
        for member in group_members:
            result = self.analyze_by_person(member)
            if not result.empty:
                results[member] = result
        
        return results
    
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
            
        sort_col = ColumnName.INCOME_AMOUNT if by_income else ColumnName.EXPENSE_AMOUNT
        return data.nlargest(top_n, sort_col) 