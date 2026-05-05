#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
话单数据加载器 - 加载、映射、标准化通话记录数据
"""

import os
import pandas as pd
from typing import Dict

from .base_loader import BaseLoader


class CallLoader(BaseLoader):
    """话单数据加载器"""

    def load(self, path: str) -> dict[str, pd.DataFrame]:
        """
        加载并标准化话单数据

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

        # 2. 字段映射（话单使用不同的标准列名）
        df = self.field_mapper.map_fields(df, 'call')

        # 3. 合并日期和时间列为 通话开始时间(datetime)
        df = self._merge_datetime(df)

        # 4. 通话时长转为整数秒
        if '通话时长' in df.columns:
            df['通话时长'] = pd.to_numeric(df['通话时长'], errors='coerce').fillna(0).astype(int)

        # 5. 呼叫类型标准化
        df = self._standardize_call_type(df)

        # 6. 填充可选字段的默认值
        df = self._fill_optional_fields(df)

        # 7. 添加数据来源列
        if '数据来源' not in df.columns:
            df['数据来源'] = os.path.basename(path)

        # 8. 按本方姓名分组
        self._data = self._split_by_person(df)
        self._raw_data = df

        return self._data

    def validate(self, df: pd.DataFrame) -> bool:
        """验证话单数据完整性"""
        required = ['本方姓名', '通话时长']
        return all(col in df.columns for col in required)

    def _merge_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        合并日期和时间列为 通话开始时间(datetime)

        如果映射后已有 '通话开始时间' 列，则直接转为 datetime。
        如果有 '呼叫日期' 和 '时间' 分开列，则合并。
        """
        # 如果映射后已有通话开始时间
        if '通话开始时间' in df.columns:
            df['通话开始时间'] = pd.to_datetime(df['通话开始时间'], errors='coerce')
            return df

        # 从配置中获取原始日期和时间列名
        call_config = self.config.get('call', {})
        date_col = call_config.get('date_column', '呼叫日期')
        time_col = call_config.get('time_column', '时间')

        if date_col in df.columns:
            if time_col and time_col in df.columns and time_col != date_col:
                # 合并日期和时间
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                df[time_col] = df[time_col].astype(str).str.strip()
                # 尝试将日期和时间合并
                try:
                    df['通话开始时间'] = pd.to_datetime(
                        df[date_col].dt.date.astype(str) + ' ' + df[time_col],
                        errors='coerce'
                    )
                except Exception:
                    df['通话开始时间'] = pd.to_datetime(df[date_col], errors='coerce')
            else:
                df['通话开始时间'] = pd.to_datetime(df[date_col], errors='coerce')

        return df

    def _standardize_call_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        呼叫类型标准化

        常见原始值：主叫/被叫/未接/接收/发送 等
        统一为：主叫/被叫/未接/短信
        """
        if '呼叫类型' not in df.columns:
            return df

        call_type_map = {
            '主叫': '主叫',
            '呼叫': '主叫',
            '拨出': '主叫',
            '被叫': '被叫',
            '接听': '被叫',
            '呼入': '被叫',
            '未接': '未接',
            '未接来电': '未接',
            '接收': '短信',
            '发送': '短信',
            '收短信': '短信',
            '发短信': '短信',
        }

        df['呼叫类型'] = df['呼叫类型'].astype(str).str.strip().map(call_type_map).fillna(df['呼叫类型'])

        return df

    def _fill_optional_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """填充可选字段的默认值"""
        optional_defaults = {
            '对方姓名': '',
            '对方号码': '',
            '对方单位名称': '',
            '对方职务': '',
            '本方号码': '',
        }
        for col, default in optional_defaults.items():
            if col not in df.columns:
                df[col] = default
            else:
                df[col] = df[col].fillna(default)
        return df
