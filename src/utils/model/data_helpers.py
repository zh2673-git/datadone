#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据模型相关的辅助函数
包含数据预处理、字段验证、类型转换等通用功能
"""

import pandas as pd
import numpy as np
from typing import Union, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


def safe_to_numeric(series: pd.Series) -> pd.Series:
    """
    安全地将Series转换为数值类型，无法转换的填充为0
    
    Parameters:
    -----------
    series : pd.Series
        要转换的Series
        
    Returns:
    --------
    pd.Series
        转换后的数值Series
    """
    return pd.to_numeric(series, errors='coerce').fillna(0)


def validate_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    验证DataFrame是否包含必需的列
    
    Parameters:
    -----------
    df : pd.DataFrame
        要验证的DataFrame
    required_columns : List[str]
        必需的列名列表
        
    Returns:
    --------
    bool
        是否包含所有必需列
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"缺少必需列: {missing_columns}")
        return False
    
    return True


def convert_to_datetime(series: pd.Series, date_format: str = None) -> pd.Series:
    """
    将Series转换为日期时间类型
    
    Parameters:
    -----------
    series : pd.Series
        要转换的Series
    date_format : str, optional
        日期格式字符串
        
    Returns:
    --------
    pd.Series
        转换后的日期时间Series
    """
    return pd.to_datetime(series, format=date_format, errors='coerce')


def add_missing_columns(df: pd.DataFrame, columns_config: Dict[str, Any]) -> pd.DataFrame:
    """
    为DataFrame添加缺失的列
    
    Parameters:
    -----------
    df : pd.DataFrame
        要处理的DataFrame
    columns_config : Dict[str, Any]
        列配置字典，键为列名，值为默认值
        
    Returns:
    --------
    pd.DataFrame
        处理后的DataFrame
    """
    df_copy = df.copy()
    
    for column_name, default_value in columns_config.items():
        if column_name not in df_copy.columns:
            df_copy[column_name] = default_value
            logger.info(f"添加缺失列 '{column_name}'，默认值: {default_value}")
    
    return df_copy


def extract_data_source(file_path: str) -> str:
    """
    从文件路径中提取数据源名称
    
    Parameters:
    -----------
    file_path : str
        文件路径
        
    Returns:
    --------
    str
        数据源名称
    """
    if not file_path:
        return "未知数据源"
    
    import os
    filename = os.path.basename(file_path)
    source_name = os.path.splitext(filename)[0]
    
    return source_name


# 配置相关的辅助函数
def get_config_value(config: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    安全地从配置字典中获取值
    
    Parameters:
    -----------
    config : Dict[str, Any]
        配置字典
    key : str
        配置键
    default : Any, optional
        默认值
        
    Returns:
    --------
    Any
        配置值或默认值
    """
    keys = key.split('.')
    current = config
    
    for k in keys:
        if isinstance(current, dict) and k in current:
            current = current[k]
        else:
            return default
    
    return current


if __name__ == "__main__":
    # 测试代码
    test_series = pd.Series(['1', '2.5', 'abc', '3'])
    result = safe_to_numeric(test_series)
    print("安全数值转换测试:", result.tolist())