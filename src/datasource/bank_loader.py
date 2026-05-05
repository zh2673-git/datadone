#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
银行数据加载器 - 加载、映射、标准化银行交易数据
"""

import os
import re
import numpy as np
import pandas as pd
from typing import Dict, Optional

from .base_loader import BaseLoader


class BankLoader(BaseLoader):
    """银行数据加载器"""

    # 常见银行卡号前缀映射
    BANK_PREFIXES = {
        '622848': '农业银行',
        '622700': '建设银行',
        '621700': '建设银行',
        '621661': '建设银行',
        '621226': '工商银行',
        '622202': '工商银行',
        '622262': '交通银行',
        '622666': '中国银行',
        '622622': '中国银行',
        '622588': '招商银行',
        '621286': '招商银行',
        '622155': '浦发银行',
        '622169': '浦发银行',
        '622516': '浦发银行',
        '622916': '民生银行',
        '622918': '民生银行',
        '622909': '兴业银行',
        '622908': '兴业银行',
        '621095': '邮政储蓄银行',
        '620062': '邮政储蓄银行',
        '623218': '邮政储蓄银行',
        '6217002': '建设银行',
        '6227002': '建设银行',
        '4367422': '建设银行',
    }

    # 银行名称关键词映射（从交易机构名称中提取）
    BANK_KEYWORDS = {
        '建行': '建设银行',
        '工行': '工商银行',
        '农行': '农业银行',
        '中行': '中国银行',
        '交行': '交通银行',
        '招行': '招商银行',
        '邮储': '邮政储蓄银行',
    }

    # 银行全称列表
    BANK_FULL_NAMES = [
        '建设银行', '工商银行', '农业银行', '中国银行', '交通银行',
        '招商银行', '浦发银行', '民生银行', '兴业银行', '邮政储蓄银行',
    ]

    def load(self, path: str) -> dict[str, pd.DataFrame]:
        """
        加载并标准化银行数据

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

        # 2. 处理借贷分列情况（有些银行数据有独立的借/贷列）
        df = self._merge_debit_credit_columns(df)

        # 3. 字段映射
        df = self.field_mapper.map_fields(df, 'bank')

        # 4. 标准化借贷标识（统一为 '借'/'贷'）
        df = self._standardize_direction(df)

        # 5. 交易金额取绝对值
        if '交易金额' in df.columns:
            df['交易金额'] = self._safe_to_numeric(df['交易金额']).abs()

        # 6. 确保账户余额为数值
        if '账户余额' in df.columns:
            df['账户余额'] = self._safe_to_numeric(df['账户余额'])

        # 7. 确保日期列为日期类型
        if '交易日期' in df.columns:
            df['交易日期'] = pd.to_datetime(df['交易日期'], errors='coerce')

        # 8. 填充可选字段的默认值
        df = self._fill_optional_fields(df)

        # 9. 银行名称提取
        df = self._extract_bank_name(df)

        # 10. 添加数据来源列
        if '数据来源' not in df.columns:
            df['数据来源'] = os.path.basename(path)

        # 11. 添加默认列
        df = self._add_default_columns(df, 'bank')

        # 12. 银行数据智能去重
        df = self._remove_bank_duplicates(df)

        # 13. 按本方姓名分组
        self._data = self._split_by_person(df)
        self._raw_data = df

        return self._data

    def _merge_debit_credit_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理借贷分列情况：如果数据中有独立的'借'/'贷'或'借方发生额'/'贷方发生额'列，
        而没有'交易金额'列，则合并为'交易金额'和'借贷标识'列。
        """
        debit_col = next((col for col in df.columns if col in ['借', '借方发生额']), None)
        credit_col = next((col for col in df.columns if col in ['贷', '贷方发生额']), None)

        # 获取配置中的金额列名
        bank_config = self.config.get('bank', {})
        amount_col_name = bank_config.get('amount_column', '交易金额')

        if debit_col and credit_col and amount_col_name not in df.columns:
            debit_values = self._safe_to_numeric(df[debit_col])
            credit_values = self._safe_to_numeric(df[credit_col])

            df[amount_col_name] = np.where(debit_values != 0, debit_values, credit_values)
            direction_col_name = bank_config.get('direction_column', '借贷标识')
            df[direction_col_name] = np.where(debit_values != 0, '借', '贷')

            # 金额为0时标识不明确
            df.loc[df[amount_col_name] == 0, direction_col_name] = '未知'

        return df

    def _standardize_direction(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化借贷标识，统一为 '借'/'贷'

        原始值可能是：借/贷、借方/贷方、支出/收入 等
        """
        if '借贷标识' not in df.columns:
            return df

        # 标准化映射
        direction_map = {
            '贷': '贷', '贷方': '贷', '收入': '贷', '入': '贷', 'C': '贷', 'c': '贷', 'CR': '贷',
            '借': '借', '借方': '借', '支出': '借', '出': '借', 'D': '借', 'd': '借', 'DR': '借',
        }

        df['借贷标识'] = df['借贷标识'].astype(str).str.strip().map(direction_map).fillna(df['借贷标识'])

        return df

    def _fill_optional_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """填充可选字段的默认值"""
        optional_defaults = {
            '交易摘要': '',
            '交易备注': '',
            '交易类型': '',
            '本方账号': '',
            '对方账号': '',
            '交易时间': '',
        }
        for col, default in optional_defaults.items():
            if col not in df.columns:
                df[col] = default
            else:
                df[col] = df[col].fillna(default)

        # 对方姓名：保留空值（NaN），存取现识别依赖"对方姓名为空"判断
        # 只在列不存在时才添加，存在时不填充 NaN
        if '对方姓名' not in df.columns:
            df['对方姓名'] = None

        return df

    def _extract_bank_name(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        提取银行名称，按优先级：
        1. 已有 '银行类型' 列 → 直接使用
        2. 从 '交易机构名称' 列提取（关键词匹配）
        3. 从 '对方银行名称' 列提取
        4. 从 '本方账号' 列提取（卡号前缀映射）
        """
        if '银行类型' in df.columns:
            # 已有银行类型列，填充空值
            df['银行类型'] = df['银行类型'].fillna('未知银行')
            return df

        # 尝试从交易机构名称提取
        if '交易机构名称' in df.columns:
            df['银行类型'] = df['交易机构名称'].apply(self._extract_bank_from_institution)
            return df

        # 尝试从对方银行名称提取
        if '对方银行名称' in df.columns:
            df['银行类型'] = df['对方银行名称'].fillna('未知银行')
            return df

        # 尝试从本方账号提取
        if '本方账号' in df.columns:
            df['银行类型'] = df['本方账号'].apply(self._extract_bank_from_account)
            return df

        # 无法提取，设为默认值
        df['银行类型'] = '未知银行'
        return df

    def _extract_bank_from_institution(self, institution_name) -> str:
        """从交易机构名称中提取银行名称"""
        if not institution_name or not isinstance(institution_name, str):
            return '未知银行'

        # 直接匹配全称
        for bank in self.BANK_FULL_NAMES:
            if bank in institution_name:
                return bank

        # 匹配简称
        for keyword, bank in self.BANK_KEYWORDS.items():
            if keyword in institution_name:
                return bank

        # 提取冒号后的分行信息
        if ':' in institution_name:
            parts = institution_name.split(':')
            if len(parts) > 1 and '银行' in parts[1]:
                return parts[1].split('银行')[0] + '银行'

        return '未知银行'

    def _extract_bank_from_account(self, account) -> str:
        """从账号中提取银行名称"""
        if not account or not isinstance(account, str):
            return '未知银行'

        # 按前缀长度降序排列，优先匹配更长的前缀
        sorted_prefixes = sorted(self.BANK_PREFIXES.keys(), key=len, reverse=True)

        for prefix in sorted_prefixes:
            if str(account).startswith(prefix):
                return self.BANK_PREFIXES[prefix]

        return '未知银行'

    def _remove_bank_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        银行数据智能去重
        通过本方姓名、银行类型、交易日期、交易金额、账户余额综合判断重复
        """
        if df.empty:
            return df

        duplicate_fields = []

        if '本方姓名' in df.columns:
            duplicate_fields.append('本方姓名')

        # 银行类型字段的多种可能名称
        bank_type_fields = ['银行类型', '银行名称', '开户行', '所属银行', '交易机构名称']
        for field in bank_type_fields:
            if field in df.columns:
                duplicate_fields.append(field)
                break

        if '交易日期' in df.columns:
            duplicate_fields.append('交易日期')

        if '交易金额' in df.columns:
            duplicate_fields.append('交易金额')

        # 账户余额字段的多种可能名称
        balance_fields = ['账户余额', '帐户余额', '余额']
        for field in balance_fields:
            if field in df.columns:
                duplicate_fields.append(field)
                break

        # 关键字段不足3个则不做去重
        if len(duplicate_fields) < 3:
            return df

        return df.drop_duplicates(subset=duplicate_fields, keep='first')
