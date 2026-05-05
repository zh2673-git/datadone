#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
字段映射器 - 根据配置将原始列名重命名为标准列名
"""

import pandas as pd
from typing import Dict


class FieldMapper:
    """
    字段映射器：根据 data_sources 配置中的列名映射，
    将原始列名重命名为标准列名。
    """

    # 交易类标准列名映射关系：config_key -> 标准列名
    TRANSACTION_FIELD_MAP = {
        'bank': {
            'name_column': '本方姓名',
            'date_column': '交易日期',
            'time_column': '交易时间',
            'amount_column': '交易金额',
            'balance_column': '账户余额',
            'type_column': '交易类型',
            'direction_column': '借贷标识',
            'opposite_name_column': '对方姓名',
            'summary_column': '交易摘要',
            'remark_column': '交易备注',
            'bank_name_column': '银行类型',
            'account_column': '本方账号',
            'opposite_account_column': '对方账号',
        },
        'wechat': {
            'name_column': '本方姓名',
            'date_column': '交易日期',
            'time_column': '交易时间',
            'amount_column': '交易金额',
            'direction_column': '借贷标识',
            'opposite_name_column': '对方姓名',
            'remark_column_2': '交易备注',
            'description_column': '交易摘要',
        },
        'alipay': {
            'name_column': '本方姓名',
            'date_column': '交易日期',
            'time_column': '交易时间',
            'amount_column': '交易金额',
            'direction_column': '借贷标识',
            'opposite_name_column': '对方姓名',
            'remark_column': '交易备注',
            'product_name_column': '交易摘要',
        },
    }

    # 话单类标准列名映射关系
    CALL_FIELD_MAP = {
        'name_column': '本方姓名',
        'date_column': '通话开始时间',
        'opposite_name_column': '对方姓名',
        'opposite_number_column': '对方号码',
        'opposite_unit_column': '对方单位名称',
        'opposite_title_column': '对方职务',
        'call_type_column': '呼叫类型',
        'duration_column': '通话时长',
        'number_column': '本方号码',
    }

    def __init__(self, config: dict):
        """
        初始化字段映射器

        Parameters:
        -----------
        config : dict
            data_sources 配置字典，结构如：
            {
                "bank": {"name_column": "本方姓名", ...},
                "wechat": {"name_column": "本方姓名", ...},
                ...
            }
        """
        self.config = config

    def map_fields(self, df: pd.DataFrame, source_type: str) -> pd.DataFrame:
        """
        根据配置中的列名映射，将原始列名重命名为标准列名

        Parameters:
        -----------
        df : pd.DataFrame
            原始数据
        source_type : str
            数据源类型：'bank', 'wechat', 'alipay', 'call'

        Returns:
        --------
        pd.DataFrame
            重命名后的数据
        """
        if source_type == 'call':
            mapping = self._build_rename_mapping(source_type, self.CALL_FIELD_MAP)
        else:
            mapping = self._build_rename_mapping(source_type, self.TRANSACTION_FIELD_MAP.get(source_type, {}))

        # 只重命名 DataFrame 中实际存在的列
        existing_mapping = {orig_col: std_col for orig_col, std_col in mapping.items() if orig_col in df.columns}

        if existing_mapping:
            df = df.rename(columns=existing_mapping)

        return df

    def _build_rename_mapping(self, source_type: str, field_map: dict[str, str]) -> dict[str, str]:
        """
        构建重命名映射：原始列名 → 标准列名

        从 config 中获取每种数据源的原始列名配置，
        再根据 field_map 映射到标准列名。

        Parameters:
        -----------
        source_type : str
            数据源类型
        field_map : dict
            config_key → 标准列名 的映射

        Returns:
        --------
        dict
            原始列名 → 标准列名 的映射
        """
        source_config = self.config.get(source_type, {})
        mapping = {}

        for config_key, standard_name in field_map.items():
            original_col = source_config.get(config_key, '')
            if original_col and original_col != standard_name:
                mapping[original_col] = standard_name

        return mapping
