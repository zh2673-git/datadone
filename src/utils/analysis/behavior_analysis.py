"""
行为分析辅助函数模块

包含各种行为分析的通用辅助函数，用于支持word_exporter.py中的分析逻辑
"""

from typing import Dict


def analyze_cash_behavior(bank_data: Dict) -> str:
    """
    分析存取现行为

    Parameters:
    -----------
    bank_data : Dict
        银行数据字典

    Returns:
    --------
    str
        存取现行为分析结果
    """
    insights = []

    cash_count = bank_data.get('cash_transaction_count', 0)
    deposit_count = bank_data.get('deposit_count', 0)
    withdraw_count = bank_data.get('withdraw_count', 0)
    deposit_amount = bank_data.get('deposit_amount', 0)
    withdraw_amount = bank_data.get('withdraw_amount', 0)
    total_count = bank_data.get('transaction_count', 0)

    if cash_count > 0:
        cash_ratio = cash_count / total_count if total_count > 0 else 0
        if cash_ratio > 0.5:
            insights.append("频繁进行存取现操作")
        elif cash_ratio > 0.2:
            insights.append("较常进行存取现操作")

        if deposit_count > withdraw_count:
            insights.append("存现次数多于取现")
        elif withdraw_count > deposit_count:
            insights.append("取现次数多于存现")

        if deposit_amount > withdraw_amount * 2:
            insights.append("存现金额显著大于取现金额")
        elif withdraw_amount > deposit_amount * 2:
            insights.append("取现金额显著大于存现金额")
    else:
        insights.append("无存取现交易记录")

    return "；".join(insights) + "。" if insights else ""


def analyze_anomalies(anomalies: Dict) -> str:
    """
    分析异常情况

    Parameters:
    -----------
    anomalies : Dict
        异常数据字典

    Returns:
    --------
    str
        异常情况分析结果
    """
    insights = []

    anomaly_list = anomalies.get('anomalies', [])
    for anomaly in anomaly_list:
        anomaly_type = anomaly.get('type', '')
        if anomaly_type == '高频交易':
            count = anomaly.get('count', 0)
            insights.append(f"存在高频交易异常（{count}次）")
        elif anomaly_type == '金额异常':
            amounts = anomaly.get('outlier_amounts', [])
            if amounts:
                max_amount = max(amounts)
                insights.append(f"存在异常大额交易（{max_amount:,.0f}元）")
        elif anomaly_type == '时间间隔异常':
            insights.append("存在短时间连续交易")

    return "；".join(insights) + "。" if insights else ""


def analyze_regular_patterns(advanced_data: Dict, bank_data: Dict) -> str:
    """
    分析规律性模式

    Parameters:
    -----------
    advanced_data : Dict
        高级分析数据
    bank_data : Dict
        银行数据

    Returns:
    --------
    str
        规律性模式分析结果
    """
    insights = []

    avg_amount = bank_data.get('avg_transaction_amount', 0)

    # 推测可能的固定支出
    if avg_amount > 0:
        if 2000 <= avg_amount <= 8000:
            insights.append("平均交易金额符合工资水平特征")
        elif 1000 <= avg_amount <= 5000:
            insights.append("平均交易金额符合房租或贷款特征")
        elif avg_amount < 500:
            insights.append("以小额日常消费为主")
        elif avg_amount > 20000:
            insights.append("以大额交易为主")

    # 分析金额分布
    amount_patterns = advanced_data.get('amount_patterns', {})
    if amount_patterns:
        ranges = amount_patterns.get('amount_ranges', {})
        if ranges:
            small_ratio = ranges.get('小额', {}).get('占比', 0)
            large_ratio = ranges.get('大额', {}).get('占比', 0)

            if small_ratio > 0.7:
                insights.append("主要为日常小额消费")
            elif large_ratio > 0.3:
                insights.append("存在较多大额交易")

    return "；".join(insights) + "。" if insights else ""


def analyze_call_behavior(call_data: Dict) -> str:
    """
    分析通话行为

    Parameters:
    -----------
    call_data : Dict
        通话数据字典

    Returns:
    --------
    str
        通话行为分析结果
    """
    insights = []

    total_calls = call_data.get('total_calls', 0)
    unique_contacts = call_data.get('unique_contacts', 0)
    avg_duration = call_data.get('avg_call_duration', 0)

    if total_calls > 0:
        if total_calls > 1000:
            insights.append("通话频率极高")
        elif total_calls > 500:
            insights.append("通话频率较高")
        elif total_calls < 50:
            insights.append("通话频率较低")

        if unique_contacts > 0:
            contact_ratio = total_calls / unique_contacts
            if contact_ratio > 10:
                insights.append("与少数人频繁通话")
            elif contact_ratio < 2:
                insights.append("联系人分布较广泛")

        if avg_duration > 300:  # 5分钟
            insights.append("通话时长较长")
        elif avg_duration < 60:  # 1分钟
            insights.append("通话时长较短")

    return "；".join(insights) + "。" if insights else ""