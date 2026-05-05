"""日期工具模块"""

from datetime import date, datetime
from typing import Dict, Optional, List


def lunar_to_solar(year: int, month: int, day: int) -> date:
    """
    农历转公历

    Args:
        year: 公历年份
        month: 农历月份
        day: 农历日期

    Returns:
        对应的公历日期
    """
    try:
        from zhdate import ZhDate
        lunar = ZhDate(year, month, day)
        solar = lunar.to_datetime()
        return solar.date()
    except ImportError:
        raise ImportError("请安装 zhdate 库: pip install zhdate")
    except Exception as e:
        raise ValueError(f"农历转换失败 ({year}年农历{month}月{day}日): {e}")


def build_special_date_mapping(
    years: list[int],
    special_dates_config: dict,
) -> dict[date, str]:
    """
    构建日期→节日名映射

    Args:
        years: 需要覆盖的公历年份列表
        special_dates_config: 特殊日期配置，格式如
            {"元旦": {"type": "solar", "month": 1, "day": 1},
             "春节": {"type": "lunar", "month": 1, "day": 1}, ...}

    Returns:
        {date对象: 节日名称} 的映射字典
    """
    mapping: dict[date, str] = {}

    for name, info in special_dates_config.items():
        date_type = info.get("type", "solar")
        month = info.get("month")
        day = info.get("day")

        if month is None or day is None:
            continue

        for year in years:
            try:
                if date_type == "solar":
                    d = date(year, month, day)
                elif date_type == "lunar":
                    d = lunar_to_solar(year, month, day)
                else:
                    continue
                mapping[d] = name
            except (ValueError, Exception):
                continue

    return mapping


def parse_date(date_str: Optional[str]) -> Optional[date]:
    """
    灵活解析各种日期格式

    支持格式:
        - YYYY-MM-DD
        - YYYY/MM/DD
        - YYYY.MM.DD
        - YYYY年MM月DD日
        - YYYYMMDD
        - 包含时间的日期字符串（取日期部分）

    Args:
        date_str: 日期字符串

    Returns:
        解析后的 date 对象，解析失败返回 None
    """
    if date_str is None or (isinstance(date_str, str) and not date_str.strip()):
        return None

    date_str = str(date_str).strip()

    # 尝试常见格式列表
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y年%m月%d日",
        "%Y%m%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y.%m.%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.date()
        except ValueError:
            continue

    # 尝试 pandas 风格的 Timestamp 解析
    try:
        import pandas as pd
        ts = pd.Timestamp(date_str)
        if not pd.isna(ts):
            return ts.date()
    except Exception:
        pass

    return None
