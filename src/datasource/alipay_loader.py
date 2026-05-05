#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
支付宝数据加载器 - 加载、映射、标准化支付宝交易数据
"""

import os
import pandas as pd
from typing import Dict

from .base_loader import BaseLoader


class AlipayLoader(BaseLoader):
    """支付宝数据加载器"""

    def load(self, path: str) -> dict[str, pd.DataFrame]:
        """
        加载并标准化支付宝数据

        Parameters:
        -----------
        path : str
            数据文件路径

        Returns:
        --------
        Dict[str, pd.DataFrame]
            {本方姓名: 标准化DataFrame}
        """
        # 1. 读取原始数据
        df = self._read_file(path)
        if df.empty:
            self._data = {}
            return self._data

        # 2. 字段映射
        df = self.field_mapper.map_fields(df, 'alipay')

        # 3. 借贷标识标准化：支付宝原始标识为'收入'/'支出'，保持原值
        # (收入/支出金额计算移到分析层)
        df = self._standardize_direction(df)

        # 4. 交易金额取绝对值
        if '交易金额' in df.columns:
            df['交易金额'] = self._safe_to_numeric(df['交易金额']).abs()

        # 5. 确保日期列为日期类型
        if '交易日期' in df.columns:
            df['交易日期'] = pd.to_datetime(df['交易日期'], errors='coerce')

        # 6. 填充可选字段的默认值
        df = self._fill_optional_fields(df)

        # 7. 添加数据来源列
        if '数据来源' not in df.columns:
            df['数据来源'] = os.path.basename(path)

        # 8. 添加默认列（支付宝不执行存取现识别）
        df = self._add_default_columns(df, 'alipay')

        # 9. 按本方姓名分组（支付宝不执行去重）
        self._data = self._split_by_person(df)
        self._raw_data = df

        return self._data

    def _standardize_direction(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化借贷标识
        支付宝原始标识为'收入'/'支出'，保留原始值
        """
        if '借贷标识' not in df.columns:
            return df

        # 清理空白
        df['借贷标识'] = df['借贷标识'].astype(str).str.strip()

        return df

    def _fill_optional_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """填充可选字段的默认值"""
        optional_defaults = {
            '交易备注': '',
            '交易摘要': '',
            '交易时间': '',
        }
        for col, default in optional_defaults.items():
            if col not in df.columns:
                df[col] = default
            else:
                df[col] = df[col].fillna(default)

        # 对方姓名：保留空值（NaN），频率分析需要区分"有对方"和"无对方"
        if '对方姓名' not in df.columns:
            df['对方姓名'] = None

        return df
