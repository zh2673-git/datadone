"""数据验证工具模块"""

from typing import List, Optional

import pandas as pd


def validate_dataframe(df: Optional[pd.DataFrame], required_columns: list[str]) -> bool:
    """
    验证 DataFrame 是否包含必需列

    Args:
        df: 待验证的 DataFrame
        required_columns: 必需的列名列表

    Returns:
        True 如果 DataFrame 非空且包含所有必需列，否则 False
    """
    if df is None or df.empty:
        return False

    existing_columns = set(df.columns)
    for col in required_columns:
        if col not in existing_columns:
            return False

    return True


def safe_float(val) -> float:
    """
    安全转为浮点数

    Args:
        val: 任意值

    Returns:
        浮点数，转换失败返回 0.0
    """
    if val is None:
        return 0.0
    try:
        result = float(val)
        if pd.isna(result):
            return 0.0
        return result
    except (ValueError, TypeError):
        return 0.0


def safe_int(val) -> int:
    """
    安全转为整数

    Args:
        val: 任意值

    Returns:
        整数，转换失败返回 0
    """
    if val is None:
        return 0
    try:
        f = float(val)
        if pd.isna(f):
            return 0
        return int(f)
    except (ValueError, TypeError):
        return 0
