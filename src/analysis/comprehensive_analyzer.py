#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional, Tuple
from datetime import datetime
import logging

from ..model.bank_model import BankDataModel
from ..model.call_model import CallDataModel
from ..model.wechat_model import WeChatDataModel
from ..model.alipay_model import AlipayDataModel
from src.utils.group import GroupManager
from src.analysis.bank_analyzer import BankAnalyzer
from src.analysis.call_analyzer import CallAnalyzer
from src.analysis.payment.wechat_analyzer import WeChatAnalyzer
from src.analysis.payment.alipay_analyzer import AlipayAnalyzer


class ComprehensiveAnalyzer:
    """
    综合数据分析器，用于综合分析银行、话单、微信、支付宝数据
    """

    def __init__(self, data_models: Dict, group_manager: Optional['GroupManager'] = None, config: Optional[Dict] = None):
        """
        初始化综合数据分析器

        Parameters:
        -----------
        data_models : Dict
            数据模型字典
        group_manager : GroupManager, optional
            分组管理器
        config : dict, optional
            配置字典
        """
        self.data_models = data_models
        self.group_manager = group_manager
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.bank_analyzer = BankAnalyzer(
            self.data_models.get('bank'), self.group_manager, self.config
        ) if self.data_models.get('bank') else None
        self.call_analyzer = CallAnalyzer(
            self.data_models.get('call'), self.group_manager, self.config
        ) if self.data_models.get('call') else None
        self.wechat_analyzer = WeChatAnalyzer(
            self.data_models.get('wechat'), self.group_manager, self.config
        ) if self.data_models.get('wechat') else None
        self.alipay_analyzer = AlipayAnalyzer(
            self.data_models.get('alipay'), self.group_manager, self.config
        ) if self.data_models.get('alipay') else None
        self.analyzers = {
            'bank': self.bank_analyzer,
            'call': self.call_analyzer,
            'wechat': self.wechat_analyzer,
            'alipay': self.alipay_analyzer
        }

    def analyze(self, base_source: str = 'bank') -> Dict[str, pd.DataFrame]:
        """
        综合分析, 将所有数据源的分析结果进行合并.

        Parameters:
        -----------
        base_source : str, optional
            基础数据源，可选值为'call', 'bank', 'wechat', 'alipay'，默认为'bank'

        Returns:
        --------
        Dict[str, pd.DataFrame]
            综合分析结果
        """
        self.logger.info(f"开始综合分析，基础数据源为 {base_source}")

        # 检查是否至少有两种数据模型被加载
        available_models = [
            model for model in self.data_models.values() if model and not model.data.empty]
        if len(available_models) < 2:
            self.logger.warning("综合分析至少需要两种数据类型的数据，当前数据不足。")
            return {}

        # 1. 对每种数据类型，运行其所有的频率分析
        all_frequency_dfs = {}
        for data_type, analyzer in self.analyzers.items():
            if not analyzer:
                continue

            try:
                # 调用各分析器的analyze方法获取频率分析结果
                if data_type in ['bank', 'wechat', 'alipay']:
                    results = analyzer.analyze(analysis_type='frequency')
                else:  # call_analyzer没有analysis_type参数
                    results = analyzer.analyze()

                # 从结果中筛选出频率分析的DataFrame
                freq_dfs = [df for key, df in results.items(
                ) if '频率' in key or '通话频率' in key]

                if freq_dfs:
                    # 合并同一数据类型的所有频率分析结果
                    all_frequency_dfs[data_type] = pd.concat(
                        freq_dfs, ignore_index=True)
            except Exception as e:
                self.logger.error(f"在为综合分析准备 '{data_type}' 数据时出错: {e}", exc_info=True)

        if len(all_frequency_dfs) < 2:
            self.logger.warning("综合分析需要至少两种有频率分析结果的数据，当前不足。")
            return {}

        # 2. 创建全局联系人单位映射 (话单功能2)
        global_contact_map = {}
        if 'call' in all_frequency_dfs:
            call_df = all_frequency_dfs['call']
            # 动态查找对方单位列
            unit_col = next((col for col in call_df.columns if '对方单位名称' in col), None)
            if unit_col:
                # 筛选出对方姓名和单位非空的记录
                contact_info = call_df[['对方姓名', unit_col]].dropna()
                # 去重，保留第一个遇到的单位作为该联系人的单位
                global_contact_map = contact_info.drop_duplicates('对方姓名').set_index('对方姓名')[unit_col].to_dict()
                self.logger.info(f"已从话单数据中创建包含 {len(global_contact_map)} 个联系人的全局单位映射。")

        # 3. 生成综合分析结果 - 修改逻辑以支持跨数据源的对手信息交叉比对
        final_results = {}

        # 为每个可用的数据源生成综合分析结果
        for source_type in all_frequency_dfs.keys():
            base_df = all_frequency_dfs[source_type]
            if base_df is None or base_df.empty:
                continue

            # 获取其他数据源的分析结果
            other_dfs = {k: v for k, v in all_frequency_dfs.items()
                         if k != source_type}

            # 以基础数据源为主体，不创建额外组合
            merged_df = base_df.copy()

            # 确保基础数据源的金额和次数列正确命名
            if source_type == 'bank':
                merged_df['银行总额'] = merged_df['交易总金额']
            elif source_type == 'wechat':
                merged_df['微信总额'] = merged_df['交易总金额']
            elif source_type == 'alipay':
                merged_df['支付宝总额'] = merged_df['交易总金额']
            elif source_type == 'call':
                merged_df['通话次数'] = merged_df['通话次数']

            # 对每个其他数据源进行匹配
            for other_type, other_df in other_dfs.items():
                if other_df is None or other_df.empty:
                    continue

                # 首先尝试完全匹配（本方姓名+对方姓名）
                exact_matched_df = pd.merge(
                    merged_df[['本方姓名', '对方姓名']].drop_duplicates(),
                    other_df,
                    on=['本方姓名', '对方姓名'],
                    how='left'
                )

                # 禁止跨人员匹配，只保留完全匹配的结果
                # 跨人员匹配会导致A与B通话的记录显示C与B的账单关系
                # 保持严格的本人-对手匹配原则

            # 使用全局联系人映射填充缺失的单位信息
            if global_contact_map:
                # 动态查找对方单位列
                unit_col_merged = next((col for col in merged_df.columns if '对方单位名称' in col), None)
                if unit_col_merged:
                    # 使用map填充缺失值
                    merged_df[unit_col_merged] = merged_df[unit_col_merged].fillna(merged_df['对方姓名'].map(global_contact_map))
                else:
                    # 如果merged_df中没有单位列，则直接创建
                    merged_df['对方单位名称'] = merged_df['对方姓名'].map(global_contact_map)

            if not merged_df.empty:
                result_key = f"综合分析_以{self._get_chinese_data_type_name(source_type)}为基准"
                final_results[result_key] = self._format_comprehensive_result(merged_df)
                self.logger.info(f"完成基于{source_type}的综合分析")

        if not final_results:
            self.logger.warning("综合分析未能生成任何合并结果。")

        return final_results

    def _get_chinese_data_type_name(self, data_type_key: str) -> str:
        """
        根据数据类型键返回中文名称。
        """
        name_map = {
            'bank': '银行',
            'call': '话单',
            'wechat': '微信',
            'alipay': '支付宝'
        }
        return name_map.get(data_type_key, data_type_key)

    def _format_comprehensive_result(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        格式化综合分析结果的列名和顺序
        """
        # 按照需求文档中的字段顺序排列
        ordered_columns = [
            '本方姓名', '对方姓名', '对方单位', '对方职务', '对方身份证号', '对方手机号',
            '银行总额', '银行收入金额', '银行支出金额', '银行交易时间跨度',
            '微信总额', '微信收入金额', '微信支出金额', '微信交易时间跨度',
            '支付宝总额', '支付宝收入金额', '支付宝支出金额', '支付宝交易时间跨度',
            '通话次数', '通话总时长', '通话时间跨度'
        ]

        # 重命名字段以匹配最终输出
        rename_map = {
            'bank_交易总金额': '银行总额', 'bank_总收入': '银行收入金额', 'bank_总支出': '银行支出金额', 'bank_交易时间跨度': '银行交易时间跨度',
            'wechat_交易总金额': '微信总额', 'wechat_总收入': '微信收入金额', 'wechat_总支出': '微信支出金额', 'wechat_交易时间跨度': '微信交易时间跨度',
            'alipay_交易总金额': '支付宝总额', 'alipay_总收入': '支付宝收入金额', 'alipay_总支出': '支付宝支出金额', 'alipay_交易时间跨度': '支付宝交易时间跨度',
            'call_通话次数': '通话次数', 'call_通话总时长(分钟)': '通话总时长', 'call_通话时间跨度': '通话时间跨度'
        }
        df.rename(columns=rename_map, inplace=True)

        # 只保留结果中存在的列，并按指定顺序排列
        final_columns = [col for col in ordered_columns if col in df.columns]
        # 添加未在排序列表中的列
        for col in df.columns:
            if col not in final_columns:
                final_columns.append(col)

        return df[final_columns]

    def merge_analysis_results(self, base_df: pd.DataFrame, base_type: str,
                               other_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        合并各数据源的分析结果
        """
        if base_df is None or base_df.empty:
            return pd.DataFrame()

        # 定义关键列
        base_name_col = '本方姓名'
        match_key_col = '对方姓名'

        # 预处理所有df，确保关键列存在
        all_dfs = {base_type: base_df.copy()}
        all_dfs.update({k: v.copy() for k, v in other_dfs.items()})

        # 获取所有数据源中的单位和职务信息
        company_position_map = {}
        for source_type, df in all_dfs.items():
            if df is not None and not df.empty:
                # 检查并获取对方单位和职务信息
                unit_col = next(
                    (col for col in df.columns if '对方单位名称_' in col), None)
                position_col = next(
                    (col for col in df.columns if '对方职务_' in col), None)

                if unit_col or position_col:
                    # 按对方姓名分组，收集所有非空的单位和职务信息
                    agg_dict = {}
                    if unit_col:
                        agg_dict[unit_col] = lambda x: '|'.join(
                            sorted(set(x.dropna().astype(str))))
                    if position_col:
                        agg_dict[position_col] = lambda x: '|'.join(
                            sorted(set(x.dropna().astype(str))))

                    if agg_dict:
                        group_data = df.groupby('对方姓名').agg(
                            agg_dict).reset_index()

                        # 更新映射字典
                        for _, row in group_data.iterrows():
                            name = row['对方姓名']
                            if name not in company_position_map:
                                company_position_map[name] = {
                                    '对方单位': set(), '对方职务': set()}

                            # 添加非空的单位信息
                            if unit_col and pd.notna(row[unit_col]):
                                units = row[unit_col].split('|')
                                company_position_map[name]['对方单位'].update(
                                    units)

                            # 添加非空的职务信息
                            if position_col and pd.notna(row[position_col]):
                                positions = row[position_col].split('|')
                                company_position_map[name]['对方职务'].update(
                                    positions)

        # 统一的字段映射
        field_mapping = {
            'bank': {'总收入': '银行总金额', '总支出': None},
            'wechat': {'总收入': '微信总金额', '总支出': None},
            'alipay': {'总收入': '支付宝总金额', '总支出': None},
            'call': {'通话次数': '通话次数', '通话总时长(分钟)': None}
        }

        # 合并数据
        merged_df = base_df[[base_name_col, match_key_col]].copy()

        # 添加对方单位信息（从映射中获取）
        merged_df['对方单位'] = merged_df[match_key_col].apply(
            lambda x: '|'.join(sorted(company_position_map.get(x, {'对方单位': set()})[
                               '对方单位'])) if x in company_position_map else None
        )

        # 初始化统一字段
        for field in ['银行总金额', '微信总金额', '支付宝总金额', '通话次数']:
            merged_df[field] = None

        # 对每个数据源进行匹配
        for source_type, df in all_dfs.items():
            if df is None or df.empty:
                continue

            # 获取字段映射
            source_mapping = field_mapping.get(source_type, {})
            if not source_mapping:
                continue

            # 准备要合并的列
            merge_cols = [base_name_col, match_key_col]
            rename_dict = {}

            # 添加要合并的列和重命名规则
            for old_col, new_col in source_mapping.items():
                if new_col and old_col in df.columns:
                    merge_cols.append(old_col)
                    rename_dict[old_col] = new_col

            # 如果没有要合并的列，跳过
            if len(merge_cols) <= 2:
                continue

            # 选择并重命名列
            df_subset = df[merge_cols].copy()
            df_subset.rename(columns=rename_dict, inplace=True)

            # 合并数据
            merged_df = pd.merge(
                merged_df,
                df_subset,
                on=[base_name_col, match_key_col],
                how='left'
            )

        # 计算匹配到的数据源数量
        for col in ['银行总金额', '微信总金额', '支付宝总金额', '通话次数']:
            merged_df[f'{col}_exists'] = merged_df[col].notna().astype(int)

        # 计算总匹配数
        merged_df['match_count'] = merged_df[[f'{col}_exists' for col in [
            '银行总金额', '微信总金额', '支付宝总金额', '通话次数']]].sum(axis=1)

        # 按匹配数量降序排序
        merged_df = merged_df.sort_values(
            ['match_count', base_name_col, match_key_col], ascending=[False, True, True])

        # 删除辅助列
        merged_df = merged_df.drop([col for col in merged_df.columns if col.endswith(
            '_exists')] + ['match_count'], axis=1)

        # 设置最终的列顺序
        final_cols = [
            '本方姓名', '对方姓名', '对方单位', '银行总金额', '微信总金额',
            '支付宝总金额', '通话次数'
        ]

        # 删除原始的对方单位和职务列（如果存在）
        for col in ['对方单位', '对方职务']:
            if col in base_df.columns:
                base_df = base_df.drop(columns=[col])
            for df in other_dfs.values():
                if df is not None and col in df.columns:
                    df = df.drop(columns=[col])

        return merged_df[final_cols]

    def _extract_bank_info(self, bank_data: pd.DataFrame) -> Dict[str, List[str]]:
        """
        提取银行信息

        Parameters:
        -----------
        bank_data : pd.DataFrame
            银行数据

        Returns:
        --------
        Dict[str, List[str]]
            银行信息，格式为 {人名: [银行名称列表]}
        """
        result = {}

        # 检查是否有银行名称列
        bank_name_column = None
        possible_bank_columns = ['银行名称', '开户行', '银行', '所属银行']

        for col in possible_bank_columns:
            if col in bank_data.columns:
                bank_name_column = col
                break

        # 如果没有找到银行名称列，尝试从账号中提取
        if bank_name_column is None and '本方账号' in bank_data.columns:
            # 从账号中提取银行名称
            bank_data['提取银行名称'] = bank_data['本方账号'].apply(
                self._extract_bank_from_account)
            bank_name_column = '提取银行名称'

        # 如果仍然没有找到银行名称列，返回空结果
        if bank_name_column is None:
            self.logger.warning("无法找到银行名称列")
            return result

        # 按人名分组，提取银行名称
        if '本方姓名' in bank_data.columns:
            for name, group in bank_data.groupby('本方姓名'):
                banks = group[bank_name_column].dropna().unique().tolist()
                if banks:
                    result[name] = banks

        return result

    def _extract_bank_from_account(self, account: str) -> str:
        """
        从账号中提取银行名称

        Parameters:
        -----------
        account : str
            账号

        Returns:
        --------
        str
            银行名称
        """
        if not account or not isinstance(account, str):
            return ""

        # 常见银行卡号前缀映射
        bank_prefixes = {
            '622848': '农业银行',
            '622700': '建设银行',
            '621700': '建设银行',
            '621661': '建设银行',
            '621226': '工商银行',
            '622202': '工商银行',
            '622848': '农业银行',
            '622700': '建设银行',
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
            '623218': '邮政储蓄银行'
        }

        # 尝试从账号中提取银行名称
        for prefix, bank in bank_prefixes.items():
            if account.startswith(prefix):
                return bank

        return "未知银行"

    def _get_base_data(self, group_name, base_source):
        # This method is deprecated and logic is moved to _analyze_by_group/_analyze_by_person
        pass

    def _get_other_data(self, group_name, data_source):
       # This method is deprecated and logic is moved to _analyze_by_group/_analyze_by_person
        pass

    def _merge_data(self, base_data, bank_data, wechat_data, alipay_data, call_data, base_source):
        # This method is deprecated and logic is moved to merge_analysis_results
        pass
