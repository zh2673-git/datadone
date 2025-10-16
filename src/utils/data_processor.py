#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理工具类
提供通用的数据处理方法，减少重复代码
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import logging


class DataProcessor:
    """
    数据处理工具类
    
    提供通用的数据处理方法，包括：
    1. DataFrame优化操作
    2. 日期处理
    3. 数据验证
    4. 性能优化方法
    """
    
    def __init__(self):
        """初始化数据处理器"""
        self.logger = logging.getLogger(__name__)
    
    @staticmethod
    def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
        """
        优化DataFrame内存使用
        
        Parameters:
        -----------
        df : pd.DataFrame
            原始数据框
            
        Returns:
        --------
        pd.DataFrame
            优化后的数据框
        """
        if df.empty:
            return df
        
        # 优化数值类型
        for col in df.select_dtypes(include=[np.number]).columns:
            if df[col].dtype == 'int64':
                if df[col].min() >= 0:
                    if df[col].max() < 255:
                        df[col] = df[col].astype('uint8')
                    elif df[col].max() < 65535:
                        df[col] = df[col].astype('uint16')
                    elif df[col].max() < 4294967295:
                        df[col] = df[col].astype('uint32')
                else:
                    if df[col].min() > -128 and df[col].max() < 127:
                        df[col] = df[col].astype('int8')
                    elif df[col].min() > -32768 and df[col].max() < 32767:
                        df[col] = df[col].astype('int16')
                    elif df[col].min() > -2147483648 and df[col].max() < 2147483647:
                        df[col] = df[col].astype('int32')
            elif df[col].dtype == 'float64':
                df[col] = pd.to_numeric(df[col], downcast='float')
        
        # 优化字符串类型
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype('category')
        
        return df
    
    @staticmethod
    def safe_date_conversion(series: pd.Series, format: str = 'mixed') -> pd.Series:
        """
        安全的日期转换
        
        Parameters:
        -----------
        series : pd.Series
            要转换的序列
        format : str
            日期格式
            
        Returns:
        --------
        pd.Series
            转换后的日期序列
        """
        try:
            return pd.to_datetime(series, errors='coerce', format=format)
        except Exception:
            # 如果指定格式失败，尝试自动推断
            return pd.to_datetime(series, errors='coerce')
    
    @staticmethod
    def batch_process_dataframe(df: pd.DataFrame, 
                              process_func: callable, 
                              batch_size: int = 1000) -> pd.DataFrame:
        """
        批量处理DataFrame，避免内存溢出
        
        Parameters:
        -----------
        df : pd.DataFrame
            要处理的数据框
        process_func : callable
            处理函数
        batch_size : int
            批次大小
            
        Returns:
        --------
        pd.DataFrame
            处理后的数据框
        """
        if len(df) <= batch_size:
            return process_func(df)
        
        results = []
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            processed_batch = process_func(batch)
            results.append(processed_batch)
        
        return pd.concat(results, ignore_index=True)
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, 
                          required_columns: List[str] = None) -> bool:
        """
        验证DataFrame的有效性
        
        Parameters:
        -----------
        df : pd.DataFrame
            要验证的数据框
        required_columns : List[str], optional
            必需的列名列表
            
        Returns:
        --------
        bool
            是否有效
        """
        if df is None or df.empty:
            return False
        
        if required_columns:
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                return False
        
        return True
    
    @staticmethod
    def safe_groupby_agg(df: pd.DataFrame, 
                        group_keys: List[str], 
                        agg_dict: Dict[str, Union[str, callable]]) -> pd.DataFrame:
        """
        安全的分组聚合操作
        
        Parameters:
        -----------
        df : pd.DataFrame
            数据框
        group_keys : List[str]
            分组键
        agg_dict : Dict[str, Union[str, callable]]
            聚合字典
            
        Returns:
        --------
        pd.DataFrame
            聚合结果
        """
        try:
            # 检查分组键是否存在
            missing_keys = set(group_keys) - set(df.columns)
            if missing_keys:
                return pd.DataFrame()
            
            # 检查聚合列是否存在
            agg_columns = [col for col in agg_dict.keys() if col in df.columns]
            if not agg_columns:
                return pd.DataFrame()
            
            # 过滤聚合字典
            filtered_agg_dict = {col: agg_dict[col] for col in agg_columns}
            
            return df.groupby(group_keys).agg(filtered_agg_dict).reset_index()
        
        except Exception as e:
            logging.getLogger(__name__).error(f"分组聚合操作失败: {str(e)}")
            return pd.DataFrame()
    
    @staticmethod
    def merge_dataframes_safely(left_df: pd.DataFrame, 
                               right_df: pd.DataFrame, 
                               on: Union[str, List[str]], 
                               how: str = 'left') -> pd.DataFrame:
        """
        安全地合并DataFrame
        
        Parameters:
        -----------
        left_df : pd.DataFrame
            左数据框
        right_df : pd.DataFrame
            右数据框
        on : Union[str, List[str]]
            合并键
        how : str
            合并方式
            
        Returns:
        --------
        pd.DataFrame
            合并结果
        """
        if left_df.empty or right_df.empty:
            return left_df if not left_df.empty else right_df
        
        try:
            # 检查合并键是否存在
            if isinstance(on, str):
                on = [on]
            
            missing_keys_left = set(on) - set(left_df.columns)
            missing_keys_right = set(on) - set(right_df.columns)
            
            if missing_keys_left or missing_keys_right:
                return left_df
            
            return pd.merge(left_df, right_df, on=on, how=how)
        
        except Exception as e:
            logging.getLogger(__name__).error(f"DataFrame合并失败: {str(e)}")
            return left_df
    
    @staticmethod
    def extract_numeric_columns(df: pd.DataFrame) -> List[str]:
        """
        提取数值列名
        
        Parameters:
        -----------
        df : pd.DataFrame
            数据框
            
        Returns:
        --------
        List[str]
            数值列名列表
        """
        return df.select_dtypes(include=[np.number]).columns.tolist()
    
    @staticmethod
    def extract_text_columns(df: pd.DataFrame) -> List[str]:
        """
        提取文本列名
        
        Parameters:
        -----------
        df : pd.DataFrame
            数据框
            
        Returns:
        --------
        List[str]
            文本列名列表
        """
        return df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        清理DataFrame数据
        
        Parameters:
        -----------
        df : pd.DataFrame
            原始数据框
            
        Returns:
        --------
        pd.DataFrame
            清理后的数据框
        """
        if df.empty:
            return df
        
        # 移除完全空白的行
        df = df.dropna(how='all')
        
        # 移除完全空白的列
        df = df.dropna(axis=1, how='all')
        
        # 清理字符串列的前后空格
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
        
        return df
