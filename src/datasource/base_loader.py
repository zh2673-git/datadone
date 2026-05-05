#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据加载器基类 - 定义所有平台加载器的公共接口和逻辑
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import pandas as pd

from .field_mapper import FieldMapper


class BaseLoader(ABC):
    """数据加载器基类"""

    def __init__(self, config: dict):
        """
        初始化数据加载器

        Parameters:
        -----------
        config : dict
            data_sources 配置字典
        """
        self.config = config
        self.field_mapper = FieldMapper(config)
        self._data: dict[str, pd.DataFrame] = {}  # {本方姓名: 标准化DataFrame}
        self._raw_data: Optional[pd.DataFrame] = None

    @abstractmethod
    def load(self, path: str) -> dict[str, pd.DataFrame]:
        """
        加载并标准化数据，返回 {本方姓名: DataFrame}

        Parameters:
        -----------
        path : str
            数据文件路径

        Returns:
        --------
        Dict[str, pd.DataFrame]
            按本方姓名分组的标准数据
        """
        ...

    def get_persons(self) -> list[str]:
        """获取所有本方姓名列表"""
        return list(self._data.keys())

    def get_data(self, person: str) -> Optional[pd.DataFrame]:
        """获取指定人员的标准化数据"""
        return self._data.get(person)

    def get_all_data(self) -> pd.DataFrame:
        """获取所有人员合并的数据"""
        if not self._data:
            return pd.DataFrame()
        return pd.concat(self._data.values(), ignore_index=True)

    def validate(self, df: pd.DataFrame) -> bool:
        """验证数据完整性"""
        required = ['本方姓名', '交易金额']
        return all(col in df.columns for col in required)

    def _add_default_columns(self, df: pd.DataFrame, source_type: str) -> pd.DataFrame:
        """添加默认列"""
        defaults = {
            '存取现标识': '转账',
            '特殊日期名称': '',
            '收入金额': 0.0,
            '支出金额': 0.0,
            '平台': source_type,
        }
        for col, default in defaults.items():
            if col not in df.columns:
                df[col] = default
        return df

    def _read_file(self, path: str) -> pd.DataFrame:
        """
        读取数据文件，支持 Excel 和 CSV

        Parameters:
        -----------
        path : str
            文件路径

        Returns:
        --------
        pd.DataFrame
        """
        if path.endswith(('.xlsx', '.xls')):
            try:
                return pd.read_excel(path)
            except Exception:
                return pd.read_excel(path, engine='openpyxl')
        elif path.endswith('.csv'):
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.read_csv(path, encoding='gbk', sep='\t')
        else:
            raise ValueError(f"不支持的文件格式: {path}")

    def _safe_to_numeric(self, series: pd.Series) -> pd.Series:
        """安全地将 Series 转换为数值类型，无法转换的填充为 0"""
        return pd.to_numeric(series, errors='coerce').fillna(0)

    def _split_by_person(self, df: pd.DataFrame, person_column: str = '本方姓名') -> dict[str, pd.DataFrame]:
        """
        按本方姓名分组数据

        Parameters:
        -----------
        df : pd.DataFrame
            标准化后的数据
        person_column : str
            本方姓名列名

        Returns:
        --------
        Dict[str, pd.DataFrame]
        """
        if person_column not in df.columns or df.empty:
            return {}

        result = {}
        for person, group in df.groupby(person_column, dropna=True):
            if person and str(person).strip():
                result[str(person)] = group.reset_index(drop=True)

        return result
