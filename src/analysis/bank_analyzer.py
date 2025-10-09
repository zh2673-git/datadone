#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional
from datetime import datetime
from zhdate import ZhDate

from .base_analyzer import BaseAnalyzer
from ..model.bank_model import BankDataModel
from ..utils.group import GroupManager
from ..utils.analysis.advanced_analysis import AdvancedAnalysisEngine
from ..utils.config import Config
from ..utils import (
    format_bank_anomaly_data,
    format_bank_pattern_data,
    get_friendly_dimension_name,
    get_friendly_metric_name,
    get_metric_description,
    generate_friendly_description,
    convert_dict_to_dataframe_with_person,
    convert_time_analysis_to_df,
    convert_amount_analysis_to_df,
    convert_default_to_df,
    convert_dict_to_dataframe
)

class BankAnalyzer(BaseAnalyzer):
    """
    银行数据分析器，用于分析银行交易数据
    """
    def __init__(self, data_model: BankDataModel, group_manager: Optional[GroupManager] = None, config: Optional[Dict] = None):
        """
        初始化银行数据分析器
        
        Parameters:
        -----------
        data_model : BankDataModel
            银行数据模型
        group_manager : GroupManager, optional
            分组管理器
        config : dict, optional
            配置字典
        """
        if not isinstance(data_model, BankDataModel):
            raise TypeError("data_model必须是BankDataModel类型")
        
        super().__init__(data_model, group_manager, config)
        self.bank_model = data_model

        # 初始化高级分析引擎
        self.advanced_analysis_engine = AdvancedAnalysisEngine(config)

    def analyze(self, analysis_type: str = 'all', source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        执行银行数据分析, 按数据来源进行聚合.
        
        Parameters:
        -----------
        analysis_type : str, optional
            分析类型，可选值为'frequency'(交易频率分析)、'cash'(存取现分析)、'special'(特殊日月分析)或'all'(全部分析)
        source_name : str, optional
            数据来源名称 (例如 '吴平一家明细.xlsx'). 如果提供, 只分析此来源.
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果，键为结果名 (例如 '吴平一家明细.xlsx_存取现分析'), 值为结果数据
        """
        if analysis_type not in ['frequency', 'cash', 'special', 'advanced', 'all']:
            raise ValueError("analysis_type必须是'frequency'、'cash'、'special'、'advanced'或'all'")
        
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
            
        # 1. 存取现分析 - 已移除单独的存取现分析表，因为已被存取现汇总概括
        # if analysis_type in ['cash', 'all']:
        #     cash_result = self.analyze_cash_operations(source_data)
        #     if not cash_result.empty:
        #         results[f"{source_name}_存取现分析"] = cash_result

        # 2. 交易频率分析
        if analysis_type in ['frequency', 'all']:
            frequency_result = self.analyze_frequency(source_data)
            if not frequency_result.empty:
                results[f"{source_name}_频率分析"] = frequency_result

        # 3. 特殊日月和金额分析
        if analysis_type in ['special', 'all']:
            special_dates_result = self.analyze_special_dates(source_data)
            if not special_dates_result.empty:
                results[f"{source_name}_特殊日期原始表"] = special_dates_result

            special_amounts_result = self.analyze_special_amounts(source_data)
            if not special_amounts_result.empty:
                results[f"{source_name}_特殊金额分析"] = special_amounts_result

            integer_amounts_result = self.analyze_integer_amounts(source_data)
            if not integer_amounts_result.empty:
                results[f"{source_name}_整数金额分析"] = integer_amounts_result

        # 4. 高级分析
        if analysis_type in ['advanced', 'all']:
            advanced_results = self.analyze_advanced_patterns(source_data, source_name)
            results.update(advanced_results)

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

    def analyze_advanced_patterns(self, data: pd.DataFrame, source_name: str) -> Dict[str, pd.DataFrame]:
        """
        执行高级模式分析，按人分开统计

        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据
        source_name : str
            数据来源名称

        Returns:
        --------
        Dict[str, pd.DataFrame]
            高级分析结果
        """
        results = {}

        if data.empty:
            return results

        # 获取所有人员
        if '本方姓名' not in data.columns:
            return results

        persons = data['本方姓名'].unique()

        # 为每个人进行高级分析
        all_time_patterns = []
        all_amount_patterns = []
        all_anomalies = []
        all_transaction_patterns = []

        for person in persons:
            person_data = data[data['本方姓名'] == person].copy()
            if person_data.empty:
                continue

            # 时间模式分析
            time_patterns = self.advanced_analysis_engine.analyze_time_patterns(
                person_data, '交易日期', '交易时间'
            )
            if time_patterns:
                # 为每个分析结果添加人员信息
                time_df = convert_dict_to_dataframe_with_person(time_patterns, '时间模式分析', person)
                all_time_patterns.append(time_df)

            # 金额模式分析
            amount_patterns = self.advanced_analysis_engine.analyze_amount_patterns(
                person_data, '交易金额'
            )
            if amount_patterns:
                amount_df = convert_dict_to_dataframe_with_person(amount_patterns, '金额模式分析', person)
                all_amount_patterns.append(amount_df)

            # 异常检测
            anomalies = self.advanced_analysis_engine.detect_anomalies(
                person_data, '本方姓名', '交易金额', '交易日期', '交易时间'
            )
            if anomalies and anomalies.get('anomalies'):
                anomaly_df = format_bank_anomaly_data(anomalies['anomalies'])
                # 为异常数据添加人员信息
                anomaly_df['姓名'] = person
                all_anomalies.append(anomaly_df)

            # 交易模式分析
            transaction_patterns = self.advanced_analysis_engine.analyze_transaction_patterns(
                person_data, '本方姓名', '交易金额', '交易日期'
            )
            if transaction_patterns and transaction_patterns.get('person_patterns'):
                pattern_df = format_bank_pattern_data(transaction_patterns['person_patterns'])
                all_transaction_patterns.append(pattern_df)

        # 合并所有人的分析结果
        if all_time_patterns:
            combined_time_df = pd.concat(all_time_patterns, ignore_index=True)
            results[f"{source_name}_时间模式分析"] = combined_time_df

        if all_amount_patterns:
            combined_amount_df = pd.concat(all_amount_patterns, ignore_index=True)
            results[f"{source_name}_金额模式分析"] = combined_amount_df

        if all_anomalies:
            combined_anomaly_df = pd.concat(all_anomalies, ignore_index=True)
            results[f"{source_name}_异常交易检测"] = combined_anomaly_df

        if all_transaction_patterns:
            combined_pattern_df = pd.concat(all_transaction_patterns, ignore_index=True)
            results[f"{source_name}_个人交易模式"] = combined_pattern_df

        return results



    def _convert_dict_to_dataframe(self, data_dict: dict, analysis_type: str) -> pd.DataFrame:
        """
        将字典数据转换为更直观的DataFrame格式

        Parameters:
        -----------
        data_dict : dict
            要转换的字典数据
        analysis_type : str
            分析类型

        Returns:
        --------
        pd.DataFrame
            转换后的DataFrame
        """
        if analysis_type == '时间模式分析':
            return convert_time_analysis_to_df(data_dict)
        elif analysis_type == '金额模式分析':
            return convert_amount_analysis_to_df(data_dict)
        else:
            # 默认转换方式
            return convert_default_to_df(data_dict, analysis_type)







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

    def analyze_special_dates(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析特殊日期的交易 (支持农历和公历)

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

        date_col = self.bank_model.date_column
        df = data.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        # 预计算所有年份的节假日公历日期
        years = df[date_col].dt.year.dropna().unique()
        holiday_map = {}
        for year_float in years:
            year = int(year_float)
            for name, details in special_dates_config.items():
                try:
                    if details['type'] == 'lunar':
                        # 将农历日期转换为该年份的公历日期
                        holiday_date = ZhDate(year, details['month'], details['day']).to_datetime().date()
                    else: # solar
                        holiday_date = datetime(year, details['month'], details['day']).date()
                    holiday_map[holiday_date] = name
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"无法计算日期 '{name}' 在 {year} 年: {e}")
                    continue
        
        # 将交易日期标准化为date对象，并映射节假日名称
        df['normalized_date'] = df[date_col].dt.date
        df['特殊日期名称'] = df['normalized_date'].map(holiday_map)
        
        special_transactions = df.dropna(subset=['特殊日期名称']).copy()
        
        # 直接返回包含原始交易和特殊日期名称的DataFrame
        return special_transactions

    def analyze_special_amounts(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析特殊金额的交易

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

        amount_col = self.bank_model.amount_column
        
        # 筛选出交易金额在特殊金额列表中的交易
        special_transactions = data[data[amount_col].abs().isin(special_amounts_config)].copy()
        
        return special_transactions

    def analyze_integer_amounts(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析整百数金额的交易

        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据

        Returns:
        --------
        pd.DataFrame
            整百数金额交易分析结果
        """
        integer_config = self.config.get('analysis', {}).get('integer_amount', {})
        threshold = integer_config.get('bank_threshold', 1000)

        if data.empty:
            return pd.DataFrame()

        amount_col = self.bank_model.amount_column

        # 筛选出大于等于阈值的整百数金额交易（能被100整除）
        integer_mask = (data[amount_col].abs() >= threshold) & (data[amount_col].abs() % 100 == 0)
        integer_transactions = data[integer_mask].copy()

        return integer_transactions

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