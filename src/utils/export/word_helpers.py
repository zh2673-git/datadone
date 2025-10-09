"""
Word导出辅助函数模块

包含Word文档生成相关的通用辅助函数，用于支持word_exporter.py
"""

import pandas as pd
from typing import Any, Union


def format_time_range_to_year_month(time_range: str) -> str:
    """
    将时间范围格式化为年月格式，去掉具体日期

    Parameters:
    -----------
    time_range : str
        原始时间范围，格式如 "2023-01-15 至 2023-12-30"

    Returns:
    --------
    str
        格式化后的时间范围，格式如 "2023年01月至2023年12月"
    """
    try:
        if ' 至 ' in time_range:
            start_date, end_date = time_range.split(' 至 ')

            # 提取年月信息
            if '-' in start_date:
                start_parts = start_date.split('-')
                if len(start_parts) >= 2:
                    start_year, start_month = start_parts[0], start_parts[1]
                    start_formatted = f"{start_year}年{start_month}月"
                else:
                    start_formatted = start_date
            else:
                start_formatted = start_date

            if '-' in end_date:
                end_parts = end_date.split('-')
                if len(end_parts) >= 2:
                    end_year, end_month = end_parts[0], end_parts[1]
                    end_formatted = f"{end_year}年{end_month}月"
                else:
                    end_formatted = end_date
            else:
                end_formatted = end_date

            return f"{start_formatted}至{end_formatted}"
        else:
            return time_range
    except Exception:
        return time_range


def is_numeric_value(x: Any) -> bool:
    """
    检查值是否为数字

    Parameters:
    -----------
    x : Any
        要检查的值

    Returns:
    --------
    bool
        是否为数字
    """
    if pd.isna(x):
        return False
    if isinstance(x, (int, float)):
        return True
    if isinstance(x, str):
        try:
            float(x)
            return True
        except ValueError:
            return False
    return False


def format_dataframe_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """
    格式化DataFrame中的数字列

    Parameters:
    -----------
    df : pd.DataFrame
        要格式化的DataFrame

    Returns:
    --------
    pd.DataFrame
        格式化后的DataFrame
    """
    df_copy = df.copy()
    
    for col in df_copy.columns:
        col_name_lower = col.lower()

        # 金额列保留2位小数
        if any(keyword in col for keyword in ['金额', '总额', '收入', '支出', '余额', '价格']):
            df_copy[col] = df_copy[col].apply(
                lambda x: f"{float(x):.2f}" if is_numeric_value(x) else x
            )
        # 次数、序号等整数列
        elif any(keyword in col for keyword in ['次数', '序号', '数量', '笔数', '个数', '排名']):
            df_copy[col] = df_copy[col].apply(
                lambda x: str(int(float(x))) if is_numeric_value(x) else x
            )
        # 电话号码、银行卡号等保持原样（文本格式）
        elif any(keyword in col for keyword in ['电话', '号码', '手机', '银行卡', '身份证', '卡号']):
            df_copy[col] = df_copy[col].apply(lambda x: str(x) if pd.notna(x) else x)

    return df_copy


def to_chinese_numeral(num: int) -> str:
    """
    将数字转换为中文数字

    Parameters:
    -----------
    num : int
        要转换的数字

    Returns:
    --------
    str
        中文数字
    """
    numerals = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九', '十']
    if 0 <= num < len(numerals):
        return numerals[num]
    return str(num)


def add_df_to_doc(doc, df: pd.DataFrame):
    """
    将DataFrame添加到Word文档

    Parameters:
    -----------
    doc : Document
        Word文档对象
    df : pd.DataFrame
        要添加的DataFrame
    """
    if df.empty:
        doc.add_paragraph("无相关数据。")
        return

    # 格式化数值列
    df_copy = format_dataframe_numbers(df)
    df_copy = df_copy.fillna('N/A').astype(str)

    table = doc.add_table(rows=1, cols=len(df_copy.columns))
    table.style = 'Table Grid'

    # 设置表头
    for i, column in enumerate(df_copy.columns):
        table.cell(0, i).text = str(column)

    # 添加数据行
    for _, row in df_copy.iterrows():
        cells = table.add_row().cells
        for i, value in enumerate(row):
            cells[i].text = str(value)


def add_grouped_df_to_doc(doc: object, df: pd.DataFrame, group_by: str):
    """
    按指定列分组显示DataFrame，使结果更清晰

    Parameters:
    -----------
    doc : Document
        Word文档对象
    df : pd.DataFrame
        要显示的数据框
    group_by : str
        分组列名
    """
    if df.empty:
        doc.add_paragraph("无数据可显示。")
        return

    # 按分组列分组
    grouped = df.groupby(group_by)

    for group_name, group_df in grouped:
        # 添加分组标题
        doc.add_paragraph(f"【{group_name}】", style='Heading 3')

        # 为该分组创建表格，不显示分组列（因为已经在标题中显示了）
        display_df = group_df.drop(columns=[group_by]).reset_index(drop=True)

        # 添加表格
        add_df_to_doc(doc, display_df)

        # 添加空行分隔
        doc.add_paragraph("")