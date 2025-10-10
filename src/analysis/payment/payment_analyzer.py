#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional, Any
from datetime import datetime
from zhdate import ZhDate

from ..base_analyzer import BaseAnalyzer
from ...model.payment_model import PaymentDataModel
from ...utils.group import GroupManager
from ...utils.constants import ColumnName
from ...utils.exceptions import InvalidArgumentError

class PaymentAnalyzer(BaseAnalyzer):
    """
    支付数据分析器基类，为支付宝、微信等支付类数据分析提供共同的分析逻辑
    """
    def __init__(self, data_model: PaymentDataModel, group_manager: Optional[GroupManager] = None, config: Optional[Dict] = None):
        """
        初始化支付数据分析器
        
        Parameters:
        -----------
        data_model : PaymentDataModel
            支付数据模型
        group_manager : GroupManager, optional
            分组管理�?
        config : dict, optional
            配置字典
        """
        if not isinstance(data_model, PaymentDataModel):
            raise TypeError("data_model必须是PaymentDataModel类型")
        
        super().__init__(data_model, group_manager, config)
        self.payment_model = data_model
    
    def analyze(self, analysis_type: str = 'all', source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        执行支付数据分析, 按数据来源进行聚合�?
        
        Parameters:
        -----------
        analysis_type : str
            分析类型，可选值为'frequency'(频率)�?special'(特殊分析)�?all'(全部)
        source_name : str, optional
            数据来源名称 (例如 '吴平一家明�?xlsx')�?如果提供, 只分析此来源�?
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果, 键为结果�?(例如 '吴平一家明�?xlsx_支付宝频�?), 值为结果数据
        """
        if analysis_type not in ['frequency', 'special', 'all']:
            raise InvalidArgumentError(f"analysis_type必须�?frequency', 'special' �?all'")

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
                # 使用子类名称作为类型标识，如"支付�?�?微信"
                payment_type = self.__class__.__name__.replace('Analyzer', '')
                results[f"{source_name}_{payment_type}频率"] = frequency_result
        
        # 3. 特殊日月和金额分�?
        if analysis_type in ['special', 'all']:
            payment_type = self.__class__.__name__.replace('Analyzer', '')
            special_dates_result = self.analyze_special_dates(source_data)
            if not special_dates_result.empty:
                results[f"{source_name}_{payment_type}特殊日期原始"] = special_dates_result
            
            special_amounts_result = self.analyze_special_amounts(source_data)
            if not special_amounts_result.empty:
                results[f"{source_name}_{payment_type}特殊金额分析"] = special_amounts_result

            # 确定平台类型
            platform_type = 'wechat' if hasattr(self.payment_model, 'account_column') else 'alipay'
            integer_amounts_result = self.analyze_integer_amounts(source_data, platform_type)
            if not integer_amounts_result.empty:
                results[f"{source_name}_整数金额分析"] = integer_amounts_result

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
        
        # 按对方姓名进行分组统�?
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

    def analyze_special_dates(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析特殊日期的交�?(支持农历和公�?

        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据

        Returns:
        --------
        pd.DataFrame
            发生在特殊日期的原始交易记录
        """
        special_dates_config = self.config.get('analysis', {}).get('special_date', {}).get('dates', {})
        if not special_dates_config or data.empty:
            return pd.DataFrame()

        date_col = self.payment_model.date_column
        df = data.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        # 预计算所有年份的节假日公历日�?
        years = df[date_col].dt.year.dropna().unique()
        holiday_map = {}
        for year_float in years:
            # 检查是否为NaN值，避免转换错误
            if pd.isna(year_float):
                continue
            year = int(year_float)
            for name, details in special_dates_config.items():
                try:
                    if details['type'] == 'lunar':
                        holiday_date = ZhDate(year, details['month'], details['day']).to_datetime().date()
                    else: # solar
                        holiday_date = datetime(year, details['month'], details['day']).date()
                    holiday_map[holiday_date] = name
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"无法计算日期 '{name}' �?{year} �? {e}")
                    continue
        
        df['normalized_date'] = df[date_col].dt.date
        df['特殊日期名称'] = df['normalized_date'].map(holiday_map)
        
        special_transactions = df.dropna(subset=['特殊日期名称']).copy()

        return special_transactions

    def analyze_special_amounts(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析特殊金额的交�?

        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据

        Returns:
        --------
        pd.DataFrame
            特殊金额交易分析结果
        """
        special_amounts_config = self.config.get('analysis', {}).get('special_amount', {}).get('amounts', [])
        if not special_amounts_config or data.empty:
            return pd.DataFrame()

        amount_col = self.payment_model.amount_column
        
        special_transactions = data[data[amount_col].abs().isin(special_amounts_config)].copy()
        
        return special_transactions

    def analyze_integer_amounts(self, data: pd.DataFrame, platform_type: str = 'wechat') -> pd.DataFrame:
        """
        分析整百数金额的交易

        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据
        platform_type : str
            平台类型�?wechat' �?'alipay'

        Returns:
        --------
        pd.DataFrame
            整百数金额交易分析结�?
        """
        integer_config = self.config.get('analysis', {}).get('integer_amount', {})
        threshold_key = f'{platform_type}_threshold'
        threshold = integer_config.get(threshold_key, 200)

        if data.empty:
            return pd.DataFrame()

        amount_col = self.payment_model.amount_column

        # 筛选出大于等于阈值的整百数金额交易（能被100整除�?
        integer_mask = (data[amount_col].abs() >= threshold) & (data[amount_col].abs() % 100 == 0)
        integer_transactions = data[integer_mask].copy()

        return integer_transactions
    
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
            分析结果，键为人名，值为该人的分析结�?
        """
        if self.group_manager is None:
            self.logger.warning("未提供分组管理器，无法按组分析")
            raise ValueError("未提供分组管理器，无法按组分析")
        
        group_members = self.group_manager.get_group(group_name)
        if not group_members:
            self.logger.warning(f"找不到分组 {group_name}")
            return {}
        
        # 对每个成员进行分�?
        results = {}
        for member in group_members:
            result = self.analyze_by_person(member)
            if not result.empty:
                results[member] = result
        
        return results
    
    def get_top_transactions(self, data: pd.DataFrame, top_n: int = 10, by_income: bool = True) -> pd.DataFrame:
        """
        获取指定数据子集中的最高交易记�?
        
        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据
        top_n : int
            返回的记录数�?
        by_income : bool
            是否按收入排�?
            
        Returns:
        --------
        pd.DataFrame
            最高交易记�?
        """
        if data.empty:
            return pd.DataFrame()
            
        sort_col = ColumnName.INCOME_AMOUNT if by_income else ColumnName.EXPENSE_AMOUNT
        return data.nlargest(top_n, sort_col)
