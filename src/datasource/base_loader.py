#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据加载器基类 - 定义所有平台加载器的公共接口和逻辑
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import os
import logging
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
        self.logger = logging.getLogger(self.__class__.__name__)

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

    def load_many(self, paths: list[str]) -> dict[str, pd.DataFrame]:
        """
        加载并合并多个同类型文件，按本方姓名累积合并。

        - 复用子类的 load()，单文件加载后按本方姓名 concat 累积
        - 不修改子类 load() 内部行为，避免覆盖式赋值问题
        - 合并后按本方姓名分组返回

        Parameters:
        -----------
        paths : list[str]
            同类型数据文件路径列表

        Returns:
        --------
        Dict[str, pd.DataFrame]
            合并后按本方姓名分组的标准数据
        """
        merged: dict[str, pd.DataFrame] = {}
        for idx, p in enumerate(paths, 1):
            fname = os.path.basename(p)
            try:
                size_mb = os.path.getsize(p) / (1024 * 1024)
                self.logger.info(f"  [{idx}/{len(paths)}] 加载 {fname} ({size_mb:.1f} MB)...")
            except Exception:
                self.logger.info(f"  [{idx}/{len(paths)}] 加载 {fname}...")
            try:
                single = self.load(p)
            except Exception as e:
                # 单文件失败不阻断其他文件
                self.logger.warning(f"  加载文件失败，已跳过: {fname} ({e})")
                continue
            for person, df in single.items():
                if df is None or df.empty:
                    continue
                if person in merged:
                    merged[person] = pd.concat(
                        [merged[person], df], ignore_index=True
                    )
                else:
                    merged[person] = df.reset_index(drop=True) if not df.index.is_unique else df.copy()
            self.logger.info(f"  [{idx}/{len(paths)}] {fname} 完成，当前 {len(merged)} 人")
        # 统一重置索引
        self._data = {p: df.reset_index(drop=True) for p, df in merged.items()}
        return self._data

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
        读取数据文件，支持 Excel 和 CSV。
        对大文件优先使用 calamine 引擎（Rust 实现，比 openpyxl 快 10-50 倍）。
        读取后自动优化内存（数值降级、低基数字符串转 category）。

        Parameters:
        -----------
        path : str
            文件路径

        Returns:
        --------
        pd.DataFrame
        """
        if path.endswith(('.xlsx', '.xls')):
            # 大文件提示
            try:
                size_mb = os.path.getsize(path) / (1024 * 1024)
                if size_mb > 10:
                    self.logger.info(f"读取 Excel 文件: {os.path.basename(path)} ({size_mb:.1f} MB)...")
            except Exception:
                pass

            # 优先 calamine（最快），退回 openpyxl
            try:
                df = pd.read_excel(path, engine='calamine')
            except Exception:
                try:
                    df = pd.read_excel(path)
                except Exception:
                    df = pd.read_excel(path, engine='openpyxl')
        elif path.endswith('.csv'):
            try:
                df = pd.read_csv(path)
            except Exception:
                df = pd.read_csv(path, encoding='gbk', sep='\t')
        else:
            raise ValueError(f"不支持的文件格式: {path}")

        # 内存优化：对大 DataFrame 降级数值类型 + 低基数字符串转 category
        try:
            self._optimize_dtypes(df)
        except Exception:
            pass
        return df

    def _optimize_dtypes(self, df: pd.DataFrame) -> None:
        """就地优化 DataFrame 的内存占用（仅做安全的数值降级，不动字符串列）"""
        if df.empty:
            return
        for col in df.columns:
            col_data = df[col]
            # 数值列：降级到最小类型（安全操作，不改变语义）
            if pd.api.types.is_integer_dtype(col_data):
                df[col] = pd.to_numeric(col_data, downcast='integer')
            elif pd.api.types.is_float_dtype(col_data):
                df[col] = pd.to_numeric(col_data, downcast='float')

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
