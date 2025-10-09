#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
工具模块，包含项目所有工具类和函数
包含子模块：
- model: 模型相关的工具
- analysis: 分析相关的工具  
- export: 导出相关的工具
"""

from .logger import setup_logger, get_default_logger
from .config import Config
from .constants import *
from .exceptions import *

# 导入子模块
from . import model
from . import analysis
from . import export

# 导入具体的辅助函数
from .model.data_helpers import (
    safe_to_numeric, validate_columns, convert_to_datetime,
    add_missing_columns, extract_data_source, get_config_value
)

from .analysis.format_helpers import (
    format_anomaly_data, format_pattern_data, calculate_percentage,
    standardize_frequency_table, standardize_call_frequency_table, format_time_range,
    format_bank_anomaly_data, format_bank_pattern_data, get_friendly_dimension_name,
    get_friendly_metric_name, get_metric_description, generate_friendly_description,
    convert_dict_to_dataframe_with_person, convert_time_analysis_to_df,
    convert_amount_analysis_to_df, convert_default_to_df, convert_dict_to_dataframe
)

from .export.excel_helpers import (
    set_column_widths, format_sheet, add_conditional_formatting,
    validate_data, get_file_path, format_platform_details, create_summary_sheet
)

__all__ = [
    'setup_logger', 'get_default_logger', 'Config',
    'model', 'analysis', 'export',
    # 数据模型辅助函数
    'safe_to_numeric', 'validate_columns', 'convert_to_datetime',
    'add_missing_columns', 'extract_data_source', 'get_config_value',
    # 分析格式化辅助函数
    'format_anomaly_data', 'format_pattern_data', 'calculate_percentage',
    'standardize_frequency_table', 'standardize_call_frequency_table', 'format_time_range',
    'format_bank_anomaly_data', 'format_bank_pattern_data', 'get_friendly_dimension_name',
    'get_friendly_metric_name', 'get_metric_description', 'generate_friendly_description',
    'convert_dict_to_dataframe_with_person', 'convert_time_analysis_to_df',
    'convert_amount_analysis_to_df', 'convert_default_to_df', 'convert_dict_to_dataframe',
    # 导出辅助函数
    'set_column_widths', 'format_sheet', 'add_conditional_formatting',
    'validate_data', 'get_file_path', 'format_platform_details', 'create_summary_sheet'
]