#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
导出模块相关的Excel辅助函数
包含Excel格式化、样式设置等通用功能
"""

import pandas as pd
import xlsxwriter
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


def set_column_widths(worksheet, df: pd.DataFrame, config: Dict[str, Any] = None) -> None:
    """
    设置Excel列的宽度
    
    Parameters:
    -----------
    worksheet : xlsxwriter.Worksheet
        Excel工作表对象
    df : pd.DataFrame
        数据DataFrame
    config : Dict[str, Any], optional
        配置字典
    """
    if config is None:
        config = {}
    
    default_width = config.get('default_width', 15)
    column_widths = config.get('column_widths', {})
    
    for i, column in enumerate(df.columns):
        # 使用配置中的宽度或默认宽度
        width = column_widths.get(column, default_width)
        
        # 根据列名自动调整宽度
        if column in ['对方姓名', '本方姓名', '交易摘要', '交易备注']:
            width = max(width, 20)
        elif column in ['交易金额', '收入金额', '支出金额']:
            width = max(width, 12)
        elif column in ['交易日期', '交易时间']:
            width = max(width, 15)
        
        worksheet.set_column(i, i, width)


def format_sheet(worksheet, df: pd.DataFrame, workbook, config: Dict[str, Any] = None) -> None:
    """
    格式化Excel工作表
    
    Parameters:
    -----------
    worksheet : xlsxwriter.Worksheet
        Excel工作表对象
    df : pd.DataFrame
        数据DataFrame
    workbook : xlsxwriter.Workbook
        Excel工作簿对象
    config : Dict[str, Any], optional
        配置字典
    """
    if config is None:
        config = {}
    
    # 设置列宽
    set_column_widths(worksheet, df, config)
    
    # 添加条件格式
    add_conditional_formatting(worksheet, df, workbook, config)
    
    # 冻结首行
    worksheet.freeze_panes(1, 0)


def add_conditional_formatting(worksheet, df: pd.DataFrame, workbook, 
                             config: Dict[str, Any] = None) -> None:
    """
    为Excel工作表添加条件格式
    
    Parameters:
    -----------
    worksheet : xlsxwriter.Worksheet
        Excel工作表对象
    df : pd.DataFrame
        数据DataFrame
    workbook : xlsxwriter.Workbook
        Excel工作簿对象
    config : Dict[str, Any], optional
        配置字典
    """
    if config is None:
        config = {}
    
    # 定义格式
    formats = {
        'positive': workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'}),
        'negative': workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
        'highlight': workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
    }
    
    # 为金额列添加条件格式
    amount_columns = ['交易金额', '收入金额', '支出金额', '总金额', '平均金额']
    
    for i, column in enumerate(df.columns):
        if column in amount_columns:
            # 正数格式（绿色）
            worksheet.conditional_format(1, i, len(df), i, {
                'type': 'cell',
                'criteria': '>',
                'value': 0,
                'format': formats['positive']
            })
            
            # 负数格式（红色）
            worksheet.conditional_format(1, i, len(df), i, {
                'type': 'cell',
                'criteria': '<',
                'value': 0,
                'format': formats['negative']
            })
        
        # 为频率列添加高亮格式
        if '频率' in column or '次数' in column:
            worksheet.conditional_format(1, i, len(df), i, {
                'type': 'cell',
                'criteria': '>',
                'value': 10,
                'format': formats['highlight']
            })


def validate_data(df: pd.DataFrame) -> bool:
    """
    验证数据是否适合导出
    
    Parameters:
    -----------
    df : pd.DataFrame
        要验证的数据
        
    Returns:
    --------
    bool
        数据是否有效
    """
    if df is None or df.empty:
        logger.warning("数据为空，无法导出")
        return False
    
    # 检查是否有NaN值过多的列
    nan_ratio = df.isnull().sum() / len(df)
    high_nan_columns = nan_ratio[nan_ratio > 0.8].index.tolist()
    
    if high_nan_columns:
        logger.warning(f"以下列包含大量空值: {high_nan_columns}")
    
    # 检查是否有重复的列名
    if len(df.columns) != len(set(df.columns)):
        logger.warning("存在重复的列名")
        return False
    
    return True


def get_file_path(filename: str, extension: str, output_dir: str = 'output') -> str:
    """
    生成完整的文件路径
    
    Parameters:
    -----------
    filename : str
        文件名（不含路径和扩展名）
    extension : str
        文件扩展名
    output_dir : str, optional
        输出目录
        
    Returns:
    --------
    str
        完整的文件路径
    """
    import os
    from datetime import datetime
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 添加时间戳避免文件名冲突
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_filename = f"{filename}_{timestamp}.{extension}".replace(' ', '_')
    
    return os.path.join(output_dir, safe_filename)


def format_platform_details(group: pd.DataFrame) -> str:
    """
    格式化平台详细信息
    
    Parameters:
    -----------
    group : pd.DataFrame
        分组数据
        
    Returns:
    --------
    str
        格式化后的平台信息
    """
    if group.empty:
        return ""
    
    details = []
    
    # 统计各平台的数据量
    if '平台' in group.columns:
        platform_counts = group['平台'].value_counts()
        for platform, count in platform_counts.items():
            details.append(f"{platform}: {count}条")
    
    # 统计数据来源
    if '数据来源' in group.columns:
        source_counts = group['数据来源'].value_counts()
        for source, count in source_counts.items():
            details.append(f"来源: {source}")
    
    return "; ".join(details)


def create_summary_sheet(writer, summary_data: Dict[str, Any], sheet_name: str = "分析汇总") -> None:
    """
    创建分析汇总表
    
    Parameters:
    -----------
    writer : pd.ExcelWriter
        Excel写入器
    summary_data : Dict[str, Any]
        汇总数据
    sheet_name : str, optional
        工作表名称
    """
    if not summary_data:
        return
    
    # 创建汇总DataFrame
    summary_rows = []
    
    for category, items in summary_data.items():
        for item_name, item_data in items.items():
            row = {
                '分析类别': category,
                '分析项目': item_name,
                '数据量': item_data.get('count', 0),
                '说明': item_data.get('description', '')
            }
            summary_rows.append(row)
    
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        summary_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # 格式化工作表
        worksheet = writer.sheets[sheet_name]
        set_column_widths(worksheet, summary_df)


if __name__ == "__main__":
    # 测试代码
    test_df = pd.DataFrame({
        '姓名': ['张三', '李四'],
        '金额': [1000, -500],
        '频率': [5, 15]
    })
    
    is_valid = validate_data(test_df)
    print("数据验证测试:", is_valid)