#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
分析模块相关的格式化辅助函数
包含结果格式化、数据转换等通用功能
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Union
import logging

logger = logging.getLogger(__name__)


def format_anomaly_data(anomalies: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    格式化异常检测结果数据
    
    Parameters:
    -----------
    anomalies : List[Dict[str, Any]]
        异常检测结果列表
        
    Returns:
    --------
    pd.DataFrame
        格式化后的异常数据DataFrame
    """
    if not anomalies:
        return pd.DataFrame()
    
    # 转换为DataFrame
    df = pd.DataFrame(anomalies)
    
    # 确保必要的列存在
    required_columns = ['person_name', 'anomaly_type', 'description', 'severity']
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
    
    # 按严重程度排序
    severity_order = {'high': 3, 'medium': 2, 'low': 1}
    df['severity_order'] = df['severity'].map(severity_order).fillna(0)
    df = df.sort_values(['severity_order', 'person_name'], ascending=[False, True])
    df = df.drop('severity_order', axis=1)
    
    return df


def format_pattern_data(person_patterns: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    """
    格式化个人交易模式数据
    
    Parameters:
    -----------
    person_patterns : Dict[str, Dict[str, Any]]
        个人交易模式字典
        
    Returns:
    --------
    pd.DataFrame
        格式化后的模式数据DataFrame
    """
    if not person_patterns:
        return pd.DataFrame()
    
    rows = []
    for person_name, patterns in person_patterns.items():
        for pattern_type, pattern_data in patterns.items():
            row = {
                'person_name': person_name,
                'pattern_type': pattern_type,
                'description': pattern_data.get('description', ''),
                'frequency': pattern_data.get('frequency', 0),
                'amount_range': pattern_data.get('amount_range', ''),
                'time_period': pattern_data.get('time_period', '')
            }
            rows.append(row)
    
    df = pd.DataFrame(rows)
    
    # 按频率排序
    df = df.sort_values(['frequency', 'person_name'], ascending=[False, True])
    
    return df


def calculate_percentage(df: pd.DataFrame, value_column: str, total_column: str, 
                        new_column_name: str = 'percentage') -> pd.DataFrame:
    """
    计算百分比列
    
    Parameters:
    -----------
    df : pd.DataFrame
        原始数据
    value_column : str
        值列名
    total_column : str
        总计列名
    new_column_name : str, optional
        新列名
        
    Returns:
    --------
    pd.DataFrame
        包含百分比列的数据
    """
    df_copy = df.copy()
    
    # 避免除零错误
    df_copy[new_column_name] = df_copy.apply(
        lambda row: (row[value_column] / row[total_column] * 100) if row[total_column] != 0 else 0,
        axis=1
    )
    
    # 格式化百分比
    df_copy[new_column_name] = df_copy[new_column_name].round(2)
    
    return df_copy


def standardize_frequency_table(df: pd.DataFrame, platform: str = '银行') -> pd.DataFrame:
    """
    标准化频率分析表结构
    
    Parameters:
    -----------
    df : pd.DataFrame
        原始频率表
    platform : str, optional
        平台名称
        
    Returns:
    --------
    pd.DataFrame
        标准化后的频率表
    """
    df_copy = df.copy()
    
    # 添加平台信息
    if '平台' not in df_copy.columns:
        df_copy['平台'] = platform
    
    # 确保必要的列存在
    base_columns = ['平台', '数据来源', '本方姓名', '对方姓名']
    for col in base_columns:
        if col not in df_copy.columns:
            df_copy[col] = ''
    
    # 添加占比字段
    if '交易次数' in df_copy.columns and '总交易次数' in df_copy.columns:
        df_copy = calculate_percentage(df_copy, '交易次数', '总交易次数', '交易次数占比')
    
    if '总金额' in df_copy.columns and '总交易金额' in df_copy.columns:
        df_copy = calculate_percentage(df_copy, '总金额', '总交易金额', '金额占比')
    
    return df_copy


def standardize_call_frequency_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化话单频率表结构
    
    Parameters:
    -----------
    df : pd.DataFrame
        原始话单频率表
        
    Returns:
    --------
    pd.DataFrame
        标准化后的话单频率表
    """
    df_copy = df.copy()
    
    # 确保必要的列存在
    base_columns = ['平台', '数据来源', '本方姓名', '对方姓名']
    detail_columns = ['对方号码', '对方单位名称', '对方职务']
    
    for col in base_columns + detail_columns:
        if col not in df_copy.columns:
            df_copy[col] = ''
    
    # 重新排列列顺序
    final_columns = base_columns + detail_columns
    other_columns = [col for col in df_copy.columns if col not in final_columns]
    final_columns.extend(other_columns)
    
    df_copy = df_copy[[col for col in final_columns if col in df_copy.columns]]
    
    return df_copy


def format_time_range(time_range: str) -> str:
    """
    格式化时间范围字符串
    
    Parameters:
    -----------
    time_range : str
        时间范围字符串
        
    Returns:
    --------
    str
        格式化后的时间范围
    """
    if not time_range:
        return ""
    
    # 简单的格式化逻辑
    return time_range.replace("_", " ").title()


def format_bank_anomaly_data(anomalies: list) -> pd.DataFrame:
    """
    格式化银行异常检测数据
    
    Parameters:
    -----------
    anomalies : list
        异常检测结果列表
        
    Returns:
    --------
    pd.DataFrame
        格式化后的异常数据DataFrame
    """
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


def format_bank_pattern_data(person_patterns: dict) -> pd.DataFrame:
    """
    格式化银行个人交易模式数据
    
    Parameters:
    -----------
    person_patterns : dict
        个人交易模式字典
        
    Returns:
    --------
    pd.DataFrame
        格式化后的模式数据DataFrame
    """
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
            '行为特征': _get_bank_behavior_description(pattern)
        })

    return pd.DataFrame(formatted_rows)


def _get_bank_behavior_description(pattern: dict) -> str:
    """
    生成银行行为特征描述
    
    Parameters:
    -----------
    pattern : dict
        交易模式字典
        
    Returns:
    --------
    str
        行为特征描述
    """
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
    if pattern.get('金额变异系数', 0) < 0.3:
        features.append('金额稳定')
    elif pattern.get('金额变异系数', 0) > 1.0:
        features.append('金额波动大')

    # 活跃度特征
    if pattern.get('交易次数', 0) > 100:
        features.append('交易频繁')
    elif pattern.get('交易次数', 0) < 10:
        features.append('交易稀少')

    # 时间偏好
    if pattern.get('工作日交易占比', 0) > 0.8:
        features.append('偏好工作日交易')
    elif pattern.get('周末交易占比', 0) > 0.3:
        features.append('偏好周末交易')

    if pattern.get('工作时间交易占比', 0) > 0.7:
        features.append('偏好工作时间交易')
    elif pattern.get('非工作时间交易占比', 0) > 0.5:
        features.append('偏好非工作时间交易')

    return '、'.join(features) if features else '无明显特征'


def get_friendly_dimension_name(dimension_key: str) -> str:
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


def get_friendly_metric_name(metric_key: str) -> str:
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


def get_metric_description(dimension: str, metric: str) -> str:
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
        return generate_friendly_description(dimension, metric)


def generate_friendly_description(dimension: str, metric: str) -> str:
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


def convert_dict_to_dataframe_with_person(data_dict: dict, analysis_type: str, person_name: str) -> pd.DataFrame:
    """
    将字典数据转换为DataFrame格式，并添加人员信息

    Parameters:
    -----------
    data_dict : dict
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
        dimension = get_friendly_dimension_name(dimension_key)
        metric = get_friendly_metric_name(metric_key)

        # 格式化数值
        if isinstance(value, (int, float)):
            if metric_key in ['占比', '比例'] or '占比' in metric_key:
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
            '说明': get_metric_description(dimension_key, metric_key)
        })

    return pd.DataFrame(rows)


def convert_time_analysis_to_df(data_dict: dict) -> pd.DataFrame:
    """
    转换时间分析数据为直观表格

    Parameters:
    -----------
    data_dict : dict
        时间分析数据字典

    Returns:
    --------
    pd.DataFrame
        转换后的时间分析DataFrame
    """
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


def convert_amount_analysis_to_df(data_dict: dict) -> pd.DataFrame:
    """
    转换金额分析数据为直观表格

    Parameters:
    -----------
    data_dict : dict
        金额分析数据字典

    Returns:
    --------
    pd.DataFrame
        转换后的金额分析DataFrame
    """
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


def convert_default_to_df(data_dict: dict, analysis_type: str) -> pd.DataFrame:
    """
    默认转换方式 - 将字典数据转换为DataFrame

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


def convert_dict_to_dataframe(data_dict: dict, analysis_type: str) -> pd.DataFrame:
    """
    根据分析类型选择不同的转换方式

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


def format_time_range(time_range: str) -> str:
    """
    格式化时间范围字符串

    Parameters:
    -----------
    time_range : str
        原始时间范围字符串
        
    Returns:
    --------
    str
        格式化后的时间范围
    """
    if not time_range:
        return ""
    
    # 将类似"2023-01-01 00:00:00 - 2023-12-31 23:59:59"的格式简化为"2023年1月-12月"
    try:
        parts = time_range.split(' - ')
        if len(parts) == 2:
            start_date = pd.to_datetime(parts[0])
            end_date = pd.to_datetime(parts[1])
            
            if start_date.year == end_date.year:
                return f"{start_date.year}年{start_date.month}月-{end_date.month}月"
            else:
                return f"{start_date.year}年{start_date.month}月-{end_date.year}年{end_date.month}月"
    except:
        pass
    
    return time_range


if __name__ == "__main__":
    # 测试代码
    test_anomalies = [
        {'person_name': '张三', 'anomaly_type': '金额异常', 'description': '单笔交易金额过大', 'severity': 'high'},
        {'person_name': '李四', 'anomaly_type': '频率异常', 'description': '交易频率异常', 'severity': 'medium'}
    ]
    
    result = format_anomaly_data(test_anomalies)
    print("异常数据格式化测试:")
    print(result)