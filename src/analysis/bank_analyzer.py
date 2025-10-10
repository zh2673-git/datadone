#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional
from datetime import datetime
from zhdate import ZhDate

from src.base import BaseAnalyzer
from src.datasource import BankDataModel
from src.group import GroupManager
from src.utils.advanced_analysis import AdvancedAnalysisEngine
from src.utils.config import Config

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
        self.advanced_analysis_engine = AdvancedAnalysisEngine(data_model.config)

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
            总金额=('交易金额', lambda x: x.abs().sum() if not x.isna().all() else 0),
            平均金额=('交易金额', lambda x: x.abs().mean() if not x.isna().all() else 0),
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
                time_df = self._convert_dict_to_dataframe_with_person(time_patterns, '时间模式分析', person)
                all_time_patterns.append(time_df)

            # 金额模式分析
            amount_patterns = self.advanced_analysis_engine.analyze_amount_patterns(
                person_data, '交易金额'
            )
            if amount_patterns:
                amount_df = self._convert_dict_to_dataframe_with_person(amount_patterns, '金额模式分析', person)
                all_amount_patterns.append(amount_df)

            # 异常检测
            anomalies = self.advanced_analysis_engine.detect_anomalies(
                person_data, '本方姓名', '交易金额', '交易日期', '交易时间'
            )
            if anomalies and anomalies.get('anomalies'):
                anomaly_df = self._format_anomaly_data(anomalies['anomalies'])
                # 为异常数据添加人员信息
                anomaly_df['姓名'] = person
                all_anomalies.append(anomaly_df)

            # 交易模式分析
            transaction_patterns = self.advanced_analysis_engine.analyze_transaction_patterns(
                person_data, '本方姓名', '交易金额', '交易日期'
            )
            if transaction_patterns and transaction_patterns.get('person_patterns'):
                pattern_df = self._format_pattern_data(transaction_patterns['person_patterns'])
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

    def _convert_dict_to_dataframe_with_person(self, data_dict: Dict, analysis_type: str, person_name: str) -> pd.DataFrame:
        """
        将字典数据转换为DataFrame格式，并添加人员信息

        Parameters:
        -----------
        data_dict : Dict
            要转换的字典数据
        analysis_type : str
            分析类型
        person_name : str
            人员姓名

        Returns:
        --------
        pd.DataFrame
            转换后的DataFrame
        """
        rows = []

        def flatten_dict(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)

        flattened = flatten_dict(data_dict)

        for key, value in flattened.items():
            # 解析键名并转换为通俗易懂的名称
            if '_' in key:
                parts = key.split('_')
                if len(parts) >= 2:
                    dimension_key = parts[0]
                    metric_key = '_'.join(parts[1:])
                else:
                    dimension_key = key
                    metric_key = '值'
            else:
                dimension_key = key
                metric_key = '值'

            # 转换为通俗易懂的名称
            dimension = self._get_friendly_dimension_name(dimension_key)
            metric = self._get_friendly_metric_name(metric_key)

            # 格式化数值
            if isinstance(value, (int, float)):
                # 处理NaN值
                if pd.isna(value):
                    formatted_value = 'N/A'
                elif metric_key in ['占比', '比例'] or '占比' in metric_key:
                    formatted_value = f"{value:.1%}"
                elif isinstance(value, float) and value != int(value):
                    formatted_value = f"{value:.2f}"
                else:
                    formatted_value = str(int(value))
            else:
                formatted_value = str(value)

            rows.append({
                '姓名': person_name,
                '分析类型': dimension,
                '具体指标': metric,
                '数值': formatted_value,
                '说明': self._get_metric_description(dimension_key, metric_key)
            })

        return pd.DataFrame(rows)

    def _get_friendly_dimension_name(self, dimension_key: str) -> str:
        """
        将技术性的维度名称转换为通俗易懂的名称

        Parameters:
        -----------
        dimension_key : str
            技术性的维度键名

        Returns:
        --------
        str
            通俗易懂的维度名称
        """
        friendly_names = {
            'weekday_distribution': '工作日vs周末',
            'working_hours_analysis': '工作时间vs非工作时间',
            'amount_ranges': '金额区间分布',
            'round_number_analysis': '整数金额偏好',
            'hourly_distribution': '一天中的活跃时段',
            'monthly_distribution': '一年中的活跃月份',
            'time_patterns': '时间规律分析',
            'amount_patterns': '金额习惯分析'
        }

        return friendly_names.get(dimension_key, dimension_key)

    def _get_friendly_metric_name(self, metric_key: str) -> str:
        """
        将技术性的指标名称转换为通俗易懂的名称

        Parameters:
        -----------
        metric_key : str
            技术性的指标键名

        Returns:
        --------
        str
            通俗易懂的指标名称
        """
        friendly_names = {
            '工作日交易数': '工作日交易次数',
            '周末交易数': '周末交易次数',
            '工作日占比': '工作日交易比例',
            '工作时间交易数': '工作时间交易次数',
            '非工作时间交易数': '非工作时间交易次数',
            '工作时间占比': '工作时间交易比例',
            '小额': '小额交易（1000元以下）',
            '中额': '中额交易（1000-1万元）',
            '大额': '大额交易（1万-10万元）',
            '巨额': '巨额交易（10万元以上）',
            '整百金额占比': '整百元交易比例',
            '整千金额占比': '整千元交易比例',
            '整万金额占比': '整万元交易比例',
            '最活跃时段': '最常交易的时间',
            '最活跃月份': '最常交易的月份'
        }

        return friendly_names.get(metric_key, metric_key)

    def _get_metric_description(self, dimension: str, metric: str) -> str:
        """
        获取指标的描述信息，使用通俗易懂的语言

        Parameters:
        -----------
        dimension : str
            分析维度
        metric : str
            指标名称

        Returns:
        --------
        str
            指标描述
        """
        descriptions = {
            'weekday_distribution': {
                '工作日交易数': '周一到周五的交易次数',
                '周末交易数': '周六、周日的交易次数',
                '工作日占比': '工作日交易占全部交易的百分比'
            },
            'working_hours_analysis': {
                '工作时间交易数': '上午9点到下午5点的交易次数',
                '非工作时间交易数': '晚上、早晨和中午的交易次数',
                '工作时间占比': '工作时间交易占全部交易的百分比'
            },
            'amount_ranges': {
                '小额': '1000元以下的小额交易（日常消费）',
                '中额': '1000-1万元的中等金额交易（较大支出）',
                '大额': '1万-10万元的大额交易（重要支出）',
                '巨额': '10万元以上的巨额交易（特殊支出）'
            },
            'round_number_analysis': {
                '整百金额占比': '使用整百元（如500、1000元）的交易比例',
                '整千金额占比': '使用整千元（如2000、5000元）的交易比例',
                '整万金额占比': '使用整万元（如1万、5万元）的交易比例'
            },
            'hourly_distribution': {
                '最活跃时段': '一天中交易最多的时间段',
                '活跃度': '该时段的交易活跃程度'
            },
            'monthly_distribution': {
                '最活跃月份': '一年中交易最多的月份',
                '活跃度': '该月份的交易活跃程度'
            }
        }

        if dimension in descriptions and metric in descriptions[dimension]:
            return descriptions[dimension][metric]
        else:
            # 提供更通俗的默认描述
            return self._generate_friendly_description(dimension, metric)

    def _generate_friendly_description(self, dimension: str, metric: str) -> str:
        """
        生成友好的指标描述

        Parameters:
        -----------
        dimension : str
            分析维度
        metric : str
            指标名称

        Returns:
        --------
        str
            友好的描述
        """
        # 处理复杂的指标名称
        if 'distribution_' in metric:
            # 处理类似 distribution_工作日交易数 的指标
            if '工作日交易数' in metric:
                return '周一到周五期间的交易次数统计'
            elif '周末交易数' in metric:
                return '周六、周日期间的交易次数统计'
            elif '工作时间交易数' in metric:
                return '上午9点到下午5点期间的交易次数'
            elif '非工作时间交易数' in metric:
                return '下午5点到上午9点期间的交易次数'
            else:
                return f'关于{metric.replace("distribution_", "")}的分布统计'

        # 处理元组形式的指标名称，如 ('交易日期', 'count')_1.0
        if '(' in metric and ')' in metric:
            if 'count' in metric:
                return '交易次数的统计分析'
            elif 'sum' in metric:
                return '交易金额的汇总统计'
            elif 'mean' in metric:
                return '交易金额的平均值统计'
            else:
                return '数据的统计分析结果'

        # 处理常见的指标类型
        if '占比' in metric or '比例' in metric:
            return f"{metric}的百分比统计"
        elif '数量' in metric or '次数' in metric:
            return f"{metric}的计数统计"
        elif '金额' in metric:
            return f"{metric}的金额统计"
        elif '时间' in metric:
            return f"{metric}的时间分析"
        elif '分布' in metric:
            return f"{metric}的分布情况"
        elif '平均' in metric:
            return f"{metric}的平均值"
        elif '最大' in metric or '最小' in metric:
            return f"{metric}的极值统计"
        elif '总计' in metric or '合计' in metric:
            return f"{metric}的汇总统计"
        else:
            return f"{metric}的分析结果"

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
            return self._convert_time_analysis_to_df(data_dict)
        elif analysis_type == '金额模式分析':
            return self._convert_amount_analysis_to_df(data_dict)
        else:
            # 默认转换方式
            return self._convert_default_to_df(data_dict, analysis_type)

    def _convert_time_analysis_to_df(self, data_dict: dict) -> pd.DataFrame:
        """转换时间分析数据为直观表格"""
        rows = []

        # 工作日分布
        if 'weekday_distribution' in data_dict:
            wd = data_dict['weekday_distribution']
            rows.append({
                '分析维度': '工作日分布',
                '指标': '工作日交易数',
                '数值': wd.get('工作日交易数', 0),
                '说明': '周一至周五的交易数量'
            })
            rows.append({
                '分析维度': '工作日分布',
                '指标': '周末交易数',
                '数值': wd.get('周末交易数', 0),
                '说明': '周六、周日的交易数量'
            })
            rows.append({
                '分析维度': '工作日分布',
                '指标': '工作日占比',
                '数值': f"{wd.get('工作日占比', 0):.1%}",
                '说明': '工作日交易占总交易的比例'
            })

        # 工作时间分布
        if 'working_hours_analysis' in data_dict:
            wh = data_dict['working_hours_analysis']
            rows.append({
                '分析维度': '工作时间分布',
                '指标': '工作时间交易数',
                '数值': wh.get('工作时间交易数', 0),
                '说明': '9:00-17:00时间段的交易数量'
            })
            rows.append({
                '分析维度': '工作时间分布',
                '指标': '非工作时间交易数',
                '数值': wh.get('非工作时间交易数', 0),
                '说明': '17:00-9:00时间段的交易数量'
            })
            rows.append({
                '分析维度': '工作时间分布',
                '指标': '工作时间占比',
                '数值': f"{wh.get('工作时间占比', 0):.1%}",
                '说明': '工作时间交易占总交易的比例'
            })

        # 小时分布（显示前5个最活跃时段）
        if 'hourly_distribution' in data_dict:
            hourly = data_dict['hourly_distribution']
            sorted_hours = sorted(hourly.items(), key=lambda x: x[1], reverse=True)[:5]
            for i, (hour, count) in enumerate(sorted_hours, 1):
                rows.append({
                    '分析维度': '活跃时段TOP5',
                    '指标': f'第{i}活跃时段',
                    '数值': f"{hour}:00时段({count}次)",
                    '说明': f'该时段交易次数排名第{i}'
                })

        return pd.DataFrame(rows)

    def _convert_amount_analysis_to_df(self, data_dict: dict) -> pd.DataFrame:
        """转换金额分析数据为直观表格"""
        rows = []

        # 金额区间分布
        if 'amount_ranges' in data_dict:
            for range_name, range_data in data_dict['amount_ranges'].items():
                rows.append({
                    '分析维度': '金额区间分布',
                    '指标': f'{range_name}交易数',
                    '数值': range_data.get('交易数', 0),
                    '说明': f'{range_name}区间的交易数量'
                })
                rows.append({
                    '分析维度': '金额区间分布',
                    '指标': f'{range_name}总金额',
                    '数值': f"{range_data.get('总金额', 0):,.0f}元",
                    '说明': f'{range_name}区间的交易总金额'
                })
                rows.append({
                    '分析维度': '金额区间分布',
                    '指标': f'{range_name}占比',
                    '数值': f"{range_data.get('占比', 0):.1%}",
                    '说明': f'{range_name}交易占总交易的比例'
                })

        # 整数金额分析
        if 'round_number_analysis' in data_dict:
            rn = data_dict['round_number_analysis']
            rows.append({
                '分析维度': '整数金额偏好',
                '指标': '整百金额交易数',
                '数值': rn.get('整百金额交易数', 0),
                '说明': '使用100、200、500等整百数字的交易'
            })
            rows.append({
                '分析维度': '整数金额偏好',
                '指标': '整百金额占比',
                '数值': f"{rn.get('整百金额占比', 0):.1%}",
                '说明': '整百金额交易占总交易的比例'
            })
            rows.append({
                '分析维度': '整数金额偏好',
                '指标': '整百金额总额',
                '数值': f"{rn.get('整百金额总额', 0):,.0f}元",
                '说明': '所有整百金额交易的总和'
            })

        # 金额统计
        if 'amount_statistics' in data_dict:
            stats = data_dict['amount_statistics']
            rows.append({
                '分析维度': '金额统计',
                '指标': '最大金额',
                '数值': f"{stats.get('最大金额', 0):,.0f}元",
                '说明': '单笔交易的最大金额'
            })
            rows.append({
                '分析维度': '金额统计',
                '指标': '平均金额',
                '数值': f"{stats.get('平均金额', 0):,.0f}元",
                '说明': '所有交易的平均金额'
            })
            rows.append({
                '分析维度': '金额统计',
                '指标': '总金额',
                '数值': f"{stats.get('总金额', 0):,.0f}元",
                '说明': '所有交易的总金额'
            })

        return pd.DataFrame(rows)

    def _convert_default_to_df(self, data_dict: dict, analysis_type: str) -> pd.DataFrame:
        """默认转换方式"""
        rows = []

        def flatten_dict(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)

        flattened = flatten_dict(data_dict)

        for key, value in flattened.items():
            rows.append({
                '分析类型': analysis_type,
                '指标名称': key,
                '指标值': value
            })

        return pd.DataFrame(rows)

    def _format_anomaly_data(self, anomalies: list) -> pd.DataFrame:
        """格式化异常检测数据"""
        formatted_rows = []

        for i, anomaly in enumerate(anomalies, 1):
            anomaly_type = anomaly.get('type', '未知异常')
            person = anomaly.get('person', '未知')
            description = anomaly.get('description', '无描述')

            # 根据异常类型提供更详细的说明
            if anomaly_type == '高频交易':
                count = anomaly.get('count', 0)
                risk_level = '高' if count > 20 else '中' if count > 15 else '低'
                formatted_rows.append({
                    '序号': i,
                    '异常类型': '高频交易异常',
                    '涉及人员': person,
                    '异常详情': f'交易次数: {count}次',
                    '风险等级': risk_level,
                    '说明': '短时间内交易次数过多，可能存在异常操作'
                })
            elif anomaly_type == '金额异常':
                amounts = anomaly.get('outlier_amounts', [])
                amounts_str = ', '.join([f'{amt:,.0f}元' for amt in amounts[:3]])
                if len(amounts) > 3:
                    amounts_str += f' 等{len(amounts)}笔'
                formatted_rows.append({
                    '序号': i,
                    '异常类型': '金额异常',
                    '涉及人员': person,
                    '异常详情': f'异常金额: {amounts_str}',
                    '风险等级': '高',
                    '说明': '交易金额偏离个人历史平均值过大'
                })
            elif anomaly_type == '时间间隔异常':
                intervals = anomaly.get('short_intervals', [])
                min_interval = min(intervals) if intervals else 0
                formatted_rows.append({
                    '序号': i,
                    '异常类型': '时间间隔异常',
                    '涉及人员': person,
                    '异常详情': f'最短间隔: {min_interval:.1f}小时',
                    '风险等级': '中',
                    '说明': '连续交易时间间隔过短，可能是批量操作'
                })
            else:
                formatted_rows.append({
                    '序号': i,
                    '异常类型': anomaly_type,
                    '涉及人员': person,
                    '异常详情': description,
                    '风险等级': '待评估',
                    '说明': '需要进一步分析的异常模式'
                })

        return pd.DataFrame(formatted_rows)

    def _format_pattern_data(self, person_patterns: dict) -> pd.DataFrame:
        """格式化个人交易模式数据"""
        formatted_rows = []

        for person, pattern in person_patterns.items():
            # 整数金额偏好分析
            round_ratio = pattern.get('整数金额比例', 0)
            round_preference = '强' if round_ratio > 0.8 else '中' if round_ratio > 0.5 else '弱'

            # 规律性分析
            regular_ratio = pattern.get('规律时间间隔比例', 0)
            regularity = '强' if regular_ratio > 0.8 else '中' if regular_ratio > 0.5 else '弱'

            # 金额稳定性分析
            variation_coeff = pattern.get('金额变异系数', 0)
            stability = '稳定' if variation_coeff < 0.5 else '一般' if variation_coeff < 1.0 else '波动大'

            # 交易活跃度
            transaction_count = pattern.get('交易次数', 0)
            activity = '高' if transaction_count > 50 else '中' if transaction_count > 20 else '低'

            formatted_rows.append({
                '姓名': person,
                '交易次数': transaction_count,
                '平均金额': f"{pattern.get('平均金额', 0):,.0f}元",
                '整数金额偏好': f"{round_preference}({round_ratio:.1%})",
                '时间规律性': f"{regularity}({regular_ratio:.1%})",
                '金额稳定性': f"{stability}(变异系数{variation_coeff:.2f})",
                '活跃度': activity,
                '行为特征': self._get_behavior_description(pattern)
            })

        return pd.DataFrame(formatted_rows)

    def _get_behavior_description(self, pattern: dict) -> str:
        """生成行为特征描述"""
        features = []

        # 整数金额偏好
        if pattern.get('是否偏好整数金额', False):
            features.append('偏好整数金额')

        # 时间规律性
        if pattern.get('是否有规律时间间隔', False):
            interval = pattern.get('最常见时间间隔', 0)
            if interval:
                features.append(f'每{interval}天交易')

        # 金额特征
        avg_amount = pattern.get('平均金额', 0)
        if avg_amount > 50000:
            features.append('大额交易为主')
        elif avg_amount < 1000:
            features.append('小额交易为主')

        # 变异系数
        variation = pattern.get('金额变异系数', 0)
        if variation < 0.3:
            features.append('金额稳定')
        elif variation > 2.0:
            features.append('金额波动大')

        return '; '.join(features) if features else '无明显特征'

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
        # 处理可能的NaN值，避免转换错误
        time_span['交易时间跨度'] = (time_span['max'] - time_span['min']).dt.days.fillna(0).astype(int) + 1
        
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