"""
高级分析工具类
提供时间段分析、金额区间分析、异常交易检测等高级分析功能
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional, Any
import logging
from datetime import datetime, timedelta


class AdvancedAnalysisEngine:
    """高级分析引擎"""
    
    def __init__(self, config):
        """
        初始化分析引擎
        
        Parameters:
        -----------
        config : Config
            配置对象
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 加载配置参数
        self._load_config()

    def _safe_datetime_convert(self, series, time_format=None):
        """
        安全地转换时间序列，抑制警告

        Parameters:
        -----------
        series : pd.Series
            要转换的时间序列
        time_format : str, optional
            指定的时间格式

        Returns:
        --------
        pd.Series
            转换后的时间序列
        """
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            if time_format:
                try:
                    return pd.to_datetime(series, format=time_format, errors='coerce')
                except:
                    pass

            # 尝试常见的时间格式
            common_formats = ['%H:%M:%S', '%H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
            for fmt in common_formats:
                try:
                    return pd.to_datetime(series, format=fmt, errors='coerce')
                except:
                    continue

            # 最后使用自动推断
            return pd.to_datetime(series, errors='coerce')
    
    def _load_config(self):
        """加载配置参数"""
        # 时间分析配置
        self.time_analysis_enabled = self.config.get('analysis.advanced.time_analysis.enabled', True)
        self.working_hours_start = self.config.get('analysis.advanced.time_analysis.working_hours_start', 9)
        self.working_hours_end = self.config.get('analysis.advanced.time_analysis.working_hours_end', 17)
        self.weekend_analysis = self.config.get('analysis.advanced.time_analysis.weekend_analysis', True)
        
        # 金额分析配置
        self.amount_analysis_enabled = self.config.get('analysis.advanced.amount_analysis.enabled', True)
        self.amount_ranges = self.config.get('analysis.advanced.amount_analysis.ranges', [
            {'name': '小额', 'min': 0, 'max': 1000},
            {'name': '中额', 'min': 1000, 'max': 10000},
            {'name': '大额', 'min': 10000, 'max': 100000},
            {'name': '巨额', 'min': 100000, 'max': float('inf')}
        ])
        
        # 异常检测配置
        self.anomaly_detection_enabled = self.config.get('analysis.advanced.anomaly_detection.enabled', True)
        self.frequency_threshold = self.config.get('analysis.advanced.anomaly_detection.frequency_threshold', 10)
        self.amount_std_threshold = self.config.get('analysis.advanced.anomaly_detection.amount_std_threshold', 3)
        self.time_gap_threshold_hours = self.config.get('analysis.advanced.anomaly_detection.time_gap_threshold_hours', 1)
        
        # 模式识别配置
        self.pattern_analysis_enabled = self.config.get('analysis.advanced.pattern_analysis.enabled', True)
        self.round_number_threshold = self.config.get('analysis.advanced.pattern_analysis.round_number_threshold', 0.3)
        self.regular_interval_threshold = self.config.get('analysis.advanced.pattern_analysis.regular_interval_threshold', 0.8)
    
    def analyze_time_patterns(self, data: pd.DataFrame, date_column: str, time_column: str = None) -> Dict[str, Any]:
        """
        分析时间模式
        
        Parameters:
        -----------
        data : pd.DataFrame
            待分析的数据
        date_column : str
            日期列名
        time_column : str, optional
            时间列名
            
        Returns:
        --------
        Dict[str, Any]
            时间模式分析结果
        """
        if not self.time_analysis_enabled or data.empty:
            return {}
        
        results = {}
        
        # 确保日期列是datetime类型
        if date_column in data.columns:
            data[date_column] = self._safe_datetime_convert(data[date_column])

            # 按工作日/周末分析
            data['weekday'] = data[date_column].dt.weekday
            data['is_weekend'] = data['weekday'].isin([5, 6])

            results['weekday_distribution'] = {
                '工作日交易数': len(data[~data['is_weekend']]),
                '周末交易数': len(data[data['is_weekend']]),
                '工作日占比': len(data[~data['is_weekend']]) / len(data) if len(data) > 0 else 0
            }

            # 按月份分析
            data['month'] = data[date_column].dt.month
            monthly_stats = data.groupby('month').agg({
                date_column: 'count',
                '交易金额': ['sum', 'mean'] if '交易金额' in data.columns else 'count'
            }).round(2)
            results['monthly_distribution'] = monthly_stats.to_dict()

            # 按小时分析（如果有时间列）
            if time_column and time_column in data.columns:
                data[time_column] = self._safe_datetime_convert(data[time_column])
                data['hour'] = data[time_column].dt.hour
                
                # 工作时间 vs 非工作时间
                working_hours_mask = (data['hour'] >= self.working_hours_start) & (data['hour'] <= self.working_hours_end)
                results['working_hours_analysis'] = {
                    '工作时间交易数': len(data[working_hours_mask]),
                    '非工作时间交易数': len(data[~working_hours_mask]),
                    '工作时间占比': len(data[working_hours_mask]) / len(data) if len(data) > 0 else 0
                }
                
                # 小时分布
                hourly_stats = data.groupby('hour').size().to_dict()
                results['hourly_distribution'] = hourly_stats
        
        return results
    
    def analyze_amount_patterns(self, data: pd.DataFrame, amount_column: str) -> Dict[str, Any]:
        """
        分析金额模式
        
        Parameters:
        -----------
        data : pd.DataFrame
            待分析的数据
        amount_column : str
            金额列名
            
        Returns:
        --------
        Dict[str, Any]
            金额模式分析结果
        """
        if not self.amount_analysis_enabled or data.empty or amount_column not in data.columns:
            return {}
        
        results = {}
        amounts = data[amount_column].abs()
        
        # 金额区间分析
        range_stats = {}
        for range_config in self.amount_ranges:
            mask = (amounts >= range_config['min']) & (amounts < range_config['max'])
            range_data = data[mask]
            range_stats[range_config['name']] = {
                '交易数': len(range_data),
                '总金额': amounts[mask].sum(),
                '平均金额': amounts[mask].mean() if len(range_data) > 0 else 0,
                '占比': len(range_data) / len(data) if len(data) > 0 else 0
            }
        results['amount_ranges'] = range_stats
        
        # 整数金额分析
        round_amounts = amounts[amounts % 100 == 0]
        results['round_number_analysis'] = {
            '整百金额交易数': len(round_amounts),
            '整百金额占比': len(round_amounts) / len(data) if len(data) > 0 else 0,
            '整百金额总额': round_amounts.sum()
        }
        
        # 特殊金额分析
        special_amounts = self.config.get('analysis.special_amount.amounts', [])
        special_mask = amounts.isin(special_amounts)
        results['special_amount_analysis'] = {
            '特殊金额交易数': len(data[special_mask]),
            '特殊金额占比': len(data[special_mask]) / len(data) if len(data) > 0 else 0,
            '特殊金额列表': amounts[special_mask].unique().tolist()
        }
        
        # 金额统计
        results['amount_statistics'] = {
            '最大金额': amounts.max(),
            '最小金额': amounts.min(),
            '平均金额': amounts.mean(),
            '中位数金额': amounts.median(),
            '标准差': amounts.std(),
            '总金额': amounts.sum()
        }
        
        return results
    
    def detect_anomalies(self, data: pd.DataFrame, person_column: str, amount_column: str, 
                        date_column: str, time_column: str = None) -> Dict[str, Any]:
        """
        检测异常交易
        
        Parameters:
        -----------
        data : pd.DataFrame
            待分析的数据
        person_column : str
            人员列名
        amount_column : str
            金额列名
        date_column : str
            日期列名
        time_column : str, optional
            时间列名
            
        Returns:
        --------
        Dict[str, Any]
            异常检测结果
        """
        if not self.anomaly_detection_enabled or data.empty:
            return {}
        
        results = {}
        anomalies = []
        
        # 确保日期时间列是正确的类型
        data[date_column] = self._safe_datetime_convert(data[date_column])
        if time_column and time_column in data.columns:
            data[time_column] = self._safe_datetime_convert(data[time_column])

            # 合并日期和时间
            try:
                # 明确指定日期时间格式以避免警告
                date_str = data[date_column].dt.date.astype(str)
                time_str = data[time_column].dt.time.astype(str)
                datetime_str = date_str + ' ' + time_str

                # 尝试使用常见的日期时间格式
                try:
                    data['datetime'] = pd.to_datetime(datetime_str, format='%Y-%m-%d %H:%M:%S', errors='coerce')
                except:
                    try:
                        data['datetime'] = pd.to_datetime(datetime_str, format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
                    except:
                        # 如果指定格式失败，使用自动推断但不显示警告
                        import warnings
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            data['datetime'] = pd.to_datetime(datetime_str, errors='coerce')
            except:
                # 如果合并失败，只使用日期
                data['datetime'] = data[date_column]
        else:
            data['datetime'] = data[date_column]
        
        # 按人员分组检测异常
        for person, person_data in data.groupby(person_column):
            person_data = person_data.sort_values('datetime')
            amounts = person_data[amount_column].abs()
            
            # 1. 频率异常检测
            if len(person_data) > self.frequency_threshold:
                anomalies.append({
                    'type': '高频交易',
                    'person': person,
                    'count': len(person_data),
                    'description': f'{person} 交易次数异常高: {len(person_data)}次'
                })
            
            # 2. 金额异常检测（基于标准差）
            if len(amounts) > 1:
                mean_amount = amounts.mean()
                std_amount = amounts.std()
                outliers = amounts[abs(amounts - mean_amount) > self.amount_std_threshold * std_amount]
                
                if len(outliers) > 0:
                    anomalies.append({
                        'type': '金额异常',
                        'person': person,
                        'outlier_amounts': outliers.tolist(),
                        'description': f'{person} 存在异常金额交易: {outliers.tolist()}'
                    })
            
            # 3. 时间间隔异常检测
            if len(person_data) > 1 and 'datetime' in person_data.columns:
                time_diffs = person_data['datetime'].diff().dt.total_seconds() / 3600  # 转换为小时
                short_intervals = time_diffs[time_diffs < self.time_gap_threshold_hours]
                
                if len(short_intervals) > 0:
                    anomalies.append({
                        'type': '时间间隔异常',
                        'person': person,
                        'short_intervals': short_intervals.tolist(),
                        'description': f'{person} 存在短时间间隔交易: {short_intervals.tolist()}小时'
                    })
        
        results['anomalies'] = anomalies
        results['anomaly_count'] = len(anomalies)
        results['anomaly_types'] = list(set([a['type'] for a in anomalies]))
        
        return results
    
    def analyze_transaction_patterns(self, data: pd.DataFrame, person_column: str, 
                                   amount_column: str, date_column: str) -> Dict[str, Any]:
        """
        分析交易模式
        
        Parameters:
        -----------
        data : pd.DataFrame
            待分析的数据
        person_column : str
            人员列名
        amount_column : str
            金额列名
        date_column : str
            日期列名
            
        Returns:
        --------
        Dict[str, Any]
            交易模式分析结果
        """
        if not self.pattern_analysis_enabled or data.empty:
            return {}
        
        results = {}
        
        # 确保日期列是datetime类型
        data[date_column] = self._safe_datetime_convert(data[date_column])
        
        # 按人员分析模式
        person_patterns = {}
        for person, person_data in data.groupby(person_column):
            person_data = person_data.sort_values(date_column)
            amounts = person_data[amount_column].abs()
            
            # 金额模式分析
            round_amounts = amounts[amounts % 100 == 0]
            round_ratio = len(round_amounts) / len(amounts) if len(amounts) > 0 else 0
            
            # 时间间隔模式分析
            if len(person_data) > 2:
                time_diffs = person_data[date_column].diff().dt.days
                time_diffs = time_diffs.dropna()
                
                # 检查是否有规律的时间间隔
                if len(time_diffs) > 0:
                    most_common_interval = time_diffs.mode().iloc[0] if len(time_diffs.mode()) > 0 else None
                    regular_interval_ratio = (time_diffs == most_common_interval).sum() / len(time_diffs) if most_common_interval else 0
                else:
                    most_common_interval = None
                    regular_interval_ratio = 0
            else:
                most_common_interval = None
                regular_interval_ratio = 0
            
            person_patterns[person] = {
                '交易次数': len(person_data),
                '整数金额比例': round_ratio,
                '是否偏好整数金额': round_ratio > self.round_number_threshold,
                '最常见时间间隔': most_common_interval,
                '规律时间间隔比例': regular_interval_ratio,
                '是否有规律时间间隔': regular_interval_ratio > self.regular_interval_threshold,
                '平均金额': amounts.mean(),
                '金额标准差': amounts.std(),
                '金额变异系数': amounts.std() / amounts.mean() if amounts.mean() > 0 else 0
            }
        
        results['person_patterns'] = person_patterns
        
        # 整体模式统计
        total_round_amounts = data[amount_column].abs()[data[amount_column].abs() % 100 == 0]
        results['overall_patterns'] = {
            '整数金额偏好人数': sum(1 for p in person_patterns.values() if p['是否偏好整数金额']),
            '规律时间间隔人数': sum(1 for p in person_patterns.values() if p['是否有规律时间间隔']),
            '整体整数金额比例': len(total_round_amounts) / len(data) if len(data) > 0 else 0
        }
        
        return results
