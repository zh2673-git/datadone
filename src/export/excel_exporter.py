#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import logging
from typing import Dict, List, Optional, Union, Any
import xlsxwriter
from datetime import datetime
import numpy as np

from src.export.base_exporter import BaseExporter
from ..model.base_model import BaseDataModel
from ..model.bank_model import BankDataModel
from src.utils.config import Config
from src.utils.constants import FilePath
from src.utils.exceptions import ExportError
from src.utils.export.excel_export import ExcelSummaryGenerator, ExcelFormatter, ExcelCrossAnalyzer, ExcelDataExtractor
from src.utils.analysis.format_helpers import standardize_frequency_table, standardize_call_frequency_table

class ExcelExporter(BaseExporter):
    """
    Excel导出器，用于将分析结果导出为Excel文件
    """
    def __init__(self, output_dir: Optional[str] = None, config: Optional[Config] = None):
        """
        初始化Excel导出器
        
        Parameters:
        -----------
        output_dir : str, optional
            输出目录
        config : Config, optional
            配置对象，如果不提供则使用默认配置
        """
        super().__init__(output_dir)
        self.config = config or Config()
        self.excel_config = self.config.get_section('export.excel')
        
        # 初始化工具类实例
        self.summary_generator = ExcelSummaryGenerator()
        self.formatter = ExcelFormatter()
        self.cross_analyzer = ExcelCrossAnalyzer()
        self.data_extractor = ExcelDataExtractor()
        
    def export(self, analysis_results: Dict[str, pd.DataFrame], filename: str, 
              data_models: Optional[Dict[str, BaseDataModel]] = None, **kwargs) -> str:
        """
        将所有分析结果导出到单个Excel文件中，每个结果一个sheet。
        
        Parameters:
        -----------
        analysis_results : Dict[str, pd.DataFrame]
            分析结果字典，键为 "分析类型"，值为分析结果的DataFrame。
        filename : str
            输出的Excel文件名（不含路径和扩展名）。
        data_models : Dict[str, BaseDataModel], optional
            包含原始数据的数据模型字典，用于添加补充信息。
        **kwargs
            其他导出参数:
            - add_summaries: bool = True，是否添加汇总表
            - add_raw_data: bool = True，是否添加原始数据表
            - include_unit_info: bool = True，是否包含单位信息
            
        Returns:
        --------
        str
            成功导出的文件路径
        
        Raises:
        -------
        ExportError
            如果导出过程中出现错误
        """
        if not analysis_results:
            raise ExportError("没有分析结果可供导出")
        
        # 获取导出选项
        add_summaries = kwargs.get('add_summaries', True)
        add_raw_data = kwargs.get('add_raw_data', True)
        include_unit_info = kwargs.get('include_unit_info', True)
        
        # 获取完整文件路径
        filepath = self._get_file_path(filename, "xlsx")
        
        try:
            with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                # 获取话单数据中的对方单位信息
                company_position_map = {}
                if include_unit_info and data_models and 'call' in data_models and data_models['call']:
                    company_position_map = self._extract_company_info(data_models['call'].data)

                # 收集和分类分析结果
                advanced_analysis_dfs = []
                frequency_analysis_dfs = []
                other_results = {}

                for result_name, df in analysis_results.items():
                    if not self.validate_data(df):
                        continue

                    # 检查是否是高级分析结果，如果是则收集起来合并
                    if any(pattern in result_name for pattern in ['时间模式分析', '金额模式分析', '异常交易检测', '个人交易模式']):
                        # 添加数据来源信息
                        df_copy = df.copy()
                        df_copy['数据来源'] = result_name.split('_')[0] if '_' in result_name else '未知'
                        advanced_analysis_dfs.append(df_copy)
                        continue

                    # 检查是否是频率分析结果，如果是则收集起来合并
                    if '频率' in result_name:
                        # 添加平台信息
                        df_copy = df.copy()
                        if '通话频率' in result_name:
                            platform = '话单'
                            df_copy['平台'] = platform
                            df_copy['数据来源'] = result_name.split('_')[0] if '_' in result_name else '未知'
                            # 话单类频率表单独处理
                            df_copy = standardize_call_frequency_table(df_copy)
                        else:
                            # 账单类频率表
                            if 'Wechat' in result_name or '微信' in result_name:
                                platform = '微信'
                            elif 'Alipay' in result_name or '支付宝' in result_name:
                                platform = '支付宝'
                            else:
                                platform = '银行'

                            df_copy['平台'] = platform
                            df_copy['数据来源'] = result_name.split('_')[0] if '_' in result_name else '未知'
                            # 统一频率表的字段结构并添加占比字段
                            df_copy = standardize_frequency_table(df_copy, platform)

                        frequency_analysis_dfs.append(df_copy)
                        continue

                    # 检查是否是以XX为基准的综合分析结果，如果是则跳过（因为已有综合分析总表）
                    if '综合分析_以' in result_name and '为基准' in result_name:
                        self.logger.info(f"跳过冗余的综合分析表: {result_name}")
                        continue

                    # 其他分析结果
                    if include_unit_info and '对方姓名' in df.columns:
                        # 使用utils模块的data_extractor处理单位信息
                        if data_models and 'call' in data_models and data_models['call']:
                            call_data = data_models['call'].data
                            company_info_df = self.data_extractor.extract_company_info(call_data)
                            df = self.data_extractor.add_company_info_to_dataframe(df, company_info_df)
                    other_results[result_name] = df

                # 按照指定顺序生成Excel表：分析汇总、频率（账单类、话单类分开）、综合分析、银行分析原始、微信分析原始、支付宝分析原始、高级分析

                # 1. 分析汇总表
                if add_summaries:
                    self.logger.info("正在生成分析汇总表...")
                    self.add_comprehensive_summary_sheets(writer, data_models)

                # 2. 频率表（账单类、话单类分开）
                if frequency_analysis_dfs:
                    # 分离话单类和账单类频率表
                    call_frequency_dfs = [df for df in frequency_analysis_dfs if df['平台'].iloc[0] == '话单']
                    bill_frequency_dfs = [df for df in frequency_analysis_dfs if df['平台'].iloc[0] != '话单']

                    # 生成账单类频率表
                    if bill_frequency_dfs:
                        combined_bill_df = pd.concat(bill_frequency_dfs, ignore_index=True)
                        base_cols = ['平台', '数据来源', '本方姓名', '对方姓名']
                        other_cols = [col for col in combined_bill_df.columns if col not in base_cols]
                        final_cols = base_cols + other_cols
                        combined_bill_df = combined_bill_df[[col for col in final_cols if col in combined_bill_df.columns]]

                        combined_bill_df.to_excel(writer, sheet_name='账单类频率表', index=False)
                        worksheet = writer.sheets['账单类频率表']
                        self.formatter.set_column_widths(worksheet, combined_bill_df)
                        self.formatter.format_sheet(worksheet, combined_bill_df, writer.book)

                    # 生成话单类频率表
                    if call_frequency_dfs:
                        combined_call_df = pd.concat(call_frequency_dfs, ignore_index=True)
                        base_cols = ['平台', '数据来源', '本方姓名', '对方姓名']

                        # 在对方姓名后面添加对方详细信息字段，优先使用带lambda后缀的字段
                        detail_cols = []
                        if '对方号码' in combined_call_df.columns:
                            detail_cols.append('对方号码')
                        if '对方单位名称_<lambda>' in combined_call_df.columns:
                            detail_cols.append('对方单位名称_<lambda>')
                        elif '对方单位名称' in combined_call_df.columns:
                            detail_cols.append('对方单位名称')
                        if '对方职务_<lambda>' in combined_call_df.columns:
                            detail_cols.append('对方职务_<lambda>')
                        elif '对方职务' in combined_call_df.columns:
                            detail_cols.append('对方职务')

                        other_cols = [col for col in combined_call_df.columns if col not in base_cols + detail_cols]
                        final_cols = base_cols + detail_cols + other_cols
                        combined_call_df = combined_call_df[[col for col in final_cols if col in combined_call_df.columns]]

                        combined_call_df.to_excel(writer, sheet_name='话单类频率表', index=False)
                        worksheet = writer.sheets['话单类频率表']
                        self.formatter.set_column_widths(worksheet, combined_call_df)
                        self.formatter.format_sheet(worksheet, combined_call_df, writer.book)

                    # 3. 综合分析表（交叉分析）
                    if call_frequency_dfs or bill_frequency_dfs:
                        self._generate_comprehensive_analysis(writer, call_frequency_dfs, bill_frequency_dfs)

                # 4. 平台原始数据表（银行分析原始、微信分析原始、支付宝分析原始）
                if add_raw_data:
                    self.logger.info("正在生成平台原始数据表...")
                    self.export_platform_raw_data(writer, data_models, analysis_results)

                # 5. 高级分析表（最后一个表格）
                if advanced_analysis_dfs:
                    combined_advanced_df = pd.concat(advanced_analysis_dfs, ignore_index=True)
                    # 重新排列列的顺序，将数据来源放在前面
                    if '数据来源' in combined_advanced_df.columns:
                        cols = ['数据来源'] + [col for col in combined_advanced_df.columns if col != '数据来源']
                        combined_advanced_df = combined_advanced_df[cols]

                    combined_advanced_df.to_excel(writer, sheet_name='高级分析', index=False)
                    worksheet = writer.sheets['高级分析']
                    self.formatter.set_column_widths(worksheet, combined_advanced_df)
                    self.formatter.format_sheet(worksheet, combined_advanced_df, writer.book)

                # 注意：按照用户要求，高级分析之后的所有表格都不再生成
                # 包括：其他分析结果、重点收支数据表等

                self.logger.info(f"已按指定顺序生成核心分析表格，共 {len(analysis_results)} 个分析结果")

            self.logger.info(f"所有分析结果已成功导出到: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"导出到Excel文件 '{filepath}' 时出错: {e}", exc_info=True)
            raise ExportError(f"导出Excel文件失败: {str(e)}")

    def _extract_company_info(self, call_data: pd.DataFrame) -> Dict[str, str]:
        """
        从通话数据中提取对方单位信息
        
        Parameters:
        -----------
        call_data : pd.DataFrame
            通话数据
            
        Returns:
        --------
        Dict[str, str]
            对方姓名到单位信息的映射
        """
        if call_data is None or call_data.empty:
            return {}
            
        company_position_map = {}
        
        # 尝试不同可能的列名
        unit_columns = ['对方单位名称', '对方单位', '对方公司']
        person_columns = ['对方姓名', '本方姓名']
        
        unit_col = next((col for col in unit_columns if col in call_data.columns), None)
        person_col = next((col for col in person_columns if col in call_data.columns), None)
        
        if unit_col and person_col:
            # 创建姓名到单位的映射
            company_map = call_data.groupby(person_col)[unit_col].agg(
                lambda x: '|'.join(sorted(set(x.dropna().astype(str))))
            ).to_dict()
            company_position_map.update(company_map)
            
        return company_position_map

    def _add_company_info(self, df: pd.DataFrame, company_info: Dict[str, str]) -> pd.DataFrame:
        """
        为DataFrame添加单位信息列
        
        Parameters:
        -----------
        df : pd.DataFrame
            要添加信息的DataFrame
        company_info : Dict[str, str]
            对方姓名到单位信息的映射
            
        Returns:
        --------
        pd.DataFrame
            添加了单位信息的DataFrame
        """
        if '对方姓名' not in df.columns or not company_info:
            return df
            
        df_copy = df.copy()
        
        # 添加单位信息列
        if '对方单位' not in df_copy.columns:
            df_copy['对方单位'] = df_copy['对方姓名'].map(company_info)
            
            # 调整列顺序，将对方单位放在对方姓名后面
            cols = list(df_copy.columns)
            name_idx = cols.index('对方姓名')
            cols.remove('对方单位')
            cols.insert(name_idx + 1, '对方单位')
            df_copy = df_copy[cols]
            
        return df_copy

    def add_summary_sheets(self, writer: pd.ExcelWriter, bank_model: 'BankDataModel'):
        """
        添加分析汇总表，合并存取现汇总和转账汇总。

        Parameters:
        -----------
        writer : pd.ExcelWriter
            Excel写入器对象
        bank_model : BankDataModel
            包含银行数据的模型
        """
        if not bank_model or bank_model.data.empty:
            return

        full_data = bank_model.data.copy()

        group_keys = ['本方姓名']
        if '数据来源' in full_data.columns:
            group_keys.insert(0, '数据来源')

        # 存取现汇总
        cash_summary = full_data.groupby(group_keys).agg(
            存现金额=('收入金额', lambda x: x[full_data.loc[x.index, '存取现标识'] == '存现'].sum()),
            取现金额=('支出金额', lambda x: x[full_data.loc[x.index, '存取现标识'] == '取现'].sum())
        ).reset_index()
        cash_summary = cash_summary[(cash_summary['存现金额'] > 0) | (cash_summary['取现金额'] > 0)]

        # 为存取现汇总添加分析类型和平台字段
        if not cash_summary.empty:
            cash_summary['分析类型'] = '存取现'
            cash_summary['平台'] = '银行'
            # 重新排列列顺序
            cols = ['分析类型', '平台'] + [col for col in cash_summary.columns if col not in ['分析类型', '平台']]
            cash_summary = cash_summary[cols]

        # 转账汇总
        transfer_summary = full_data[full_data['存取现标识'] == '转账'].groupby(group_keys).agg(
            转入金额=('收入金额', 'sum'),
            转出金额=('支出金额', 'sum')
        ).reset_index()
        transfer_summary = transfer_summary[(transfer_summary['转入金额'] > 0) | (transfer_summary['转出金额'] > 0)]

        # 为转账汇总添加分析类型和平台字段，并统一列名
        if not transfer_summary.empty:
            transfer_summary['分析类型'] = '转账'
            transfer_summary['平台'] = '银行'
            # 统一列名以便合并
            transfer_summary['存现金额'] = 0
            transfer_summary['取现金额'] = 0
            # 重新排列列顺序
            cols = ['分析类型', '平台'] + [col for col in transfer_summary.columns if col not in ['分析类型', '平台']]
            transfer_summary = transfer_summary[cols]

        # 合并存取现汇总和转账汇总
        combined_summary_list = []
        if not cash_summary.empty:
            # 为存取现汇总添加转账相关的空列
            cash_summary['转入金额'] = 0
            cash_summary['转出金额'] = 0
            combined_summary_list.append(cash_summary)

        if not transfer_summary.empty:
            combined_summary_list.append(transfer_summary)

        if combined_summary_list:
            # 合并所有汇总数据
            combined_summary = pd.concat(combined_summary_list, ignore_index=True)

            # 确保列顺序一致
            base_cols = ['分析类型', '平台']
            if '数据来源' in combined_summary.columns:
                base_cols.append('数据来源')
            base_cols.extend(['本方姓名', '存现金额', '取现金额', '转入金额', '转出金额'])

            # 只保留存在的列
            final_cols = [col for col in base_cols if col in combined_summary.columns]
            combined_summary = combined_summary[final_cols]

            # 导出合并后的分析汇总表
            combined_summary.to_excel(writer, sheet_name='分析汇总表', index=False)
            worksheet = writer.sheets['分析汇总表']
            self.formatter.set_column_widths(worksheet, combined_summary)
            self.formatter.format_sheet(worksheet, combined_summary, writer.book)

        self.logger.info("已添加分析汇总表")

    def add_comprehensive_summary_sheets(self, writer: pd.ExcelWriter, data_models: Dict):
        """
        添加综合分析汇总表，包含银行、微信、支付宝的汇总数据。

        Parameters:
        -----------
        writer : pd.ExcelWriter
            Excel写入器对象
        data_models : Dict
            包含各种数据模型的字典
        """
        # 使用utils模块生成综合分析汇总数据
        combined_summary = self.summary_generator.generate_comprehensive_summary(data_models)

        if not combined_summary.empty:
            # 导出合并后的分析汇总表
            combined_summary.to_excel(writer, sheet_name='分析汇总表', index=False)
            worksheet = writer.sheets['分析汇总表']
            self.formatter.set_column_widths(worksheet, combined_summary)
            self.formatter.format_sheet(worksheet, combined_summary, writer.book)

            self.logger.info("已添加综合分析汇总表")

    def _get_bank_summary_data(self, bank_model) -> pd.DataFrame:
        """获取银行数据的汇总信息"""
        if not bank_model or bank_model.data.empty:
            return pd.DataFrame()

        full_data = bank_model.data.copy()

        group_keys = ['本方姓名']
        if '数据来源' in full_data.columns:
            group_keys.insert(0, '数据来源')

        # 存取现汇总
        cash_summary = full_data.groupby(group_keys).agg(
            存现金额=('收入金额', lambda x: x[full_data.loc[x.index, '存取现标识'] == '存现'].sum()),
            取现金额=('支出金额', lambda x: x[full_data.loc[x.index, '存取现标识'] == '取现'].sum())
        ).reset_index()
        cash_summary = cash_summary[(cash_summary['存现金额'] > 0) | (cash_summary['取现金额'] > 0)]

        # 转账汇总
        transfer_summary = full_data[full_data['存取现标识'] == '转账'].groupby(group_keys).agg(
            转入金额=('收入金额', 'sum'),
            转出金额=('支出金额', 'sum')
        ).reset_index()
        transfer_summary = transfer_summary[(transfer_summary['转入金额'] > 0) | (transfer_summary['转出金额'] > 0)]

        # 合并银行数据
        combined_bank_data = []

        if not cash_summary.empty:
            cash_summary['分析类型'] = '存取现'
            cash_summary['平台'] = '银行'
            cash_summary['转入金额'] = 0
            cash_summary['转出金额'] = 0
            combined_bank_data.append(cash_summary)

        if not transfer_summary.empty:
            transfer_summary['分析类型'] = '转账'
            transfer_summary['平台'] = '银行'
            transfer_summary['存现金额'] = 0
            transfer_summary['取现金额'] = 0
            combined_bank_data.append(transfer_summary)

        if combined_bank_data:
            result = pd.concat(combined_bank_data, ignore_index=True)
            # 统一列顺序
            base_cols = ['分析类型', '平台']
            if '数据来源' in result.columns:
                base_cols.append('数据来源')
            base_cols.extend(['本方姓名', '存现金额', '取现金额', '转入金额', '转出金额'])
            final_cols = [col for col in base_cols if col in result.columns]
            return result[final_cols]

        return pd.DataFrame()

    def _get_payment_summary_data(self, payment_model, platform_name: str) -> pd.DataFrame:
        """获取微信/支付宝数据的汇总信息"""
        if not payment_model or payment_model.data.empty:
            return pd.DataFrame()

        full_data = payment_model.data.copy()

        group_keys = ['本方姓名']
        if '数据来源' in full_data.columns:
            group_keys.insert(0, '数据来源')

        # 微信/支付宝转账汇总
        summary = full_data.groupby(group_keys).agg(
            转入金额=('收入金额', 'sum'),
            转出金额=('支出金额', 'sum')
        ).reset_index()
        summary = summary[(summary['转入金额'] > 0) | (summary['转出金额'] > 0)]

        if not summary.empty:
            summary['分析类型'] = '转账'
            summary['平台'] = platform_name
            summary['存现金额'] = 0
            summary['取现金额'] = 0

            # 统一列顺序
            base_cols = ['分析类型', '平台']
            if '数据来源' in summary.columns:
                base_cols.append('数据来源')
            base_cols.extend(['本方姓名', '存现金额', '取现金额', '转入金额', '转出金额'])
            final_cols = [col for col in base_cols if col in summary.columns]
            return summary[final_cols]

        return pd.DataFrame()

    def _standardize_frequency_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        统一频率表的字段结构，添加收入总额、收入占比、支出总额、支出占比字段

        Parameters:
        -----------
        df : pd.DataFrame
            原始频率表数据

        Returns:
        --------
        pd.DataFrame
            标准化后的频率表数据
        """
        df_copy = df.copy()

        # 确保必要的字段存在
        required_fields = ['总收入', '总支出', '交易次数']
        for field in required_fields:
            if field not in df_copy.columns:
                df_copy[field] = 0

        # 计算总金额
        if '交易总金额' not in df_copy.columns:
            df_copy['交易总金额'] = df_copy['总收入'] + df_copy['总支出']

        # 计算收入和支出占比
        total_income = df_copy['总收入'].sum()
        total_expense = df_copy['总支出'].sum()

        if total_income > 0:
            df_copy['收入占比'] = (df_copy['总收入'] / total_income * 100).round(2)
        else:
            df_copy['收入占比'] = 0

        if total_expense > 0:
            df_copy['支出占比'] = (df_copy['总支出'] / total_expense * 100).round(2)
        else:
            df_copy['支出占比'] = 0

        # 重命名字段以保持一致性
        rename_mapping = {
            '总收入': '收入总额',
            '总支出': '支出总额'
        }
        df_copy.rename(columns=rename_mapping, inplace=True)

        return df_copy

    def _standardize_call_frequency_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        统一话单类频率表的字段结构

        Parameters:
        -----------
        df : pd.DataFrame
            原始话单频率表数据

        Returns:
        --------
        pd.DataFrame
            标准化后的话单频率表数据
        """
        df_copy = df.copy()

        # 话单类频率表通常包含通话次数、通话时长等字段
        # 确保必要的字段存在
        if '通话次数' not in df_copy.columns:
            df_copy['通话次数'] = 0
        if '通话总时长(分钟)' not in df_copy.columns and '通话时长' not in df_copy.columns:
            df_copy['通话总时长(分钟)'] = 0

        # 计算通话占比
        total_calls = df_copy['通话次数'].sum()
        if total_calls > 0:
            df_copy['通话占比'] = (df_copy['通话次数'] / total_calls * 100).round(2)
        else:
            df_copy['通话占比'] = 0

        return df_copy

    def _generate_comprehensive_analysis(self, writer: pd.ExcelWriter, call_frequency_dfs: list, bill_frequency_dfs: list):
        """
        生成综合分析表，进行多种基准的交叉分析

        Parameters:
        -----------
        writer : pd.ExcelWriter
            Excel写入器对象
        call_frequency_dfs : list
            话单类频率表数据列表
        bill_frequency_dfs : list
            账单类频率表数据列表
        """
        # 合并话单类和账单类数据
        combined_call_df = pd.concat(call_frequency_dfs, ignore_index=True) if call_frequency_dfs else pd.DataFrame()
        combined_bill_df = pd.concat(bill_frequency_dfs, ignore_index=True) if bill_frequency_dfs else pd.DataFrame()

        comprehensive_results = []

        # 1. 以话单为基准的交叉分析（如果有话单和账单数据）
        if not combined_call_df.empty and not combined_bill_df.empty:
            call_based_analysis = self.cross_analyzer.cross_analyze_with_call_base(combined_call_df, combined_bill_df)
            if not call_based_analysis.empty:
                call_based_analysis['分析基准'] = '以话单为基准'
                comprehensive_results.append(call_based_analysis)

        # 2. 以账单类为基准的交叉分析（如果有话单和账单数据）
        if not combined_bill_df.empty and not combined_call_df.empty:
            bill_based_analysis = self.cross_analyzer.cross_analyze_with_bill_base(combined_bill_df, combined_call_df)
            if not bill_based_analysis.empty:
                bill_based_analysis['分析基准'] = '以账单类为基准'
                comprehensive_results.append(bill_based_analysis)

        # 3. 各平台为基准的交叉分析（始终执行，如果有多个数据源）
        if not combined_bill_df.empty:
            platform_based_analysis = self._generate_platform_based_analysis(combined_bill_df, combined_call_df)
            if platform_based_analysis:
                comprehensive_results.extend(platform_based_analysis)

        # 合并并导出综合分析结果
        if comprehensive_results:
            final_comprehensive_df = pd.concat(comprehensive_results, ignore_index=True)
            # 将分析基准列放在前面
            cols = ['分析基准'] + [col for col in final_comprehensive_df.columns if col != '分析基准']
            final_comprehensive_df = final_comprehensive_df[cols]

            final_comprehensive_df.to_excel(writer, sheet_name='综合分析', index=False)
            worksheet = writer.sheets['综合分析']
            self.formatter.set_column_widths(worksheet, final_comprehensive_df)
            self.formatter.format_sheet(worksheet, final_comprehensive_df, writer.book)

            self.logger.info("已生成综合分析表")



    def _generate_platform_based_analysis(self, combined_bill_df: pd.DataFrame, combined_call_df: pd.DataFrame) -> list:
        """
        生成以各平台为基准的交叉分析

        Parameters:
        -----------
        combined_bill_df : pd.DataFrame
            合并的账单类频率表数据
        combined_call_df : pd.DataFrame
            合并的话单类频率表数据

        Returns:
        --------
        list
            各平台为基准的交叉分析结果列表
        """
        results = []

        # 获取所有账单平台
        bill_platforms = combined_bill_df['平台'].unique() if not combined_bill_df.empty else []

        # 为每个账单平台生成以该平台为基准的交叉分析
        for base_platform in bill_platforms:
            # 使用utils模块的cross_analyze_with_platform_base方法
            cross_analysis = self.cross_analyzer.cross_analyze_with_platform_base(
                combined_bill_df, combined_call_df, base_platform
            )
            if not cross_analysis.empty:
                cross_analysis['分析基准'] = f'以{base_platform}为基准'
                results.append(cross_analysis)

        return results

    def _format_platform_details(self, group: pd.DataFrame) -> str:
        """格式化平台金额分布详情"""
        details = []
        for _, row in group.iterrows():
            platform = row['平台']
            
            # 安全地获取收入总额和支出总额
            income = 0
            expense = 0
            if '收入总额' in group.columns:
                income = row['收入总额']
            if '支出总额' in group.columns:
                expense = row['支出总额']
                
            if income > 0 or expense > 0:
                detail = f"{platform}(收入{income:.0f}元,支出{expense:.0f}元)"
                details.append(detail)
        return '; '.join(details) if details else '无'


        if '收入总额' in merged_df.columns:
            bill_summary_columns.extend(['收入总额', '支出总额', '交易次数'])
        if '平台' in merged_df.columns:
            bill_summary_columns.append('平台')
        if '平台金额分布' in merged_df.columns:
            bill_summary_columns.append('平台金额分布')

        # 话单相关列
        call_columns = []
        if '通话次数' in merged_df.columns:
            call_columns.append('通话次数')
        if '通话总时长(分钟)' in merged_df.columns:
            call_columns.append('通话总时长(分钟)')
        elif '通话时长' in merged_df.columns:
            call_columns.append('通话时长')

        # 各平台独立列（按平台名称排序）
        platform_columns = []
        platforms = ['银行', '微信', '支付宝']
        for platform in platforms:
            for field in ['收入总额', '支出总额', '交易次数']:
                col_name = f'{platform}_{field}'
                if col_name in merged_df.columns:
                    platform_columns.append(col_name)

        # 剩余列
        used_columns = base_columns + detail_columns + bill_summary_columns + call_columns + platform_columns
        remaining_columns = [col for col in merged_df.columns if col not in used_columns]

        final_columns = base_columns + detail_columns + bill_summary_columns + call_columns + platform_columns + remaining_columns
        merged_df = merged_df[[col for col in final_columns if col in merged_df.columns]]

        return merged_df

    def export_platform_raw_data(self, writer: pd.ExcelWriter, data_models: Dict, analysis_results: Dict):
        """
        导出平台原始数据表，将每个平台的所有原始表汇总到一个sheet中

        Parameters:
        -----------
        writer : pd.ExcelWriter
            Excel写入器对象
        data_models : Dict
            包含各种数据模型的字典
        analysis_results : Dict
            分析结果字典
        """
        # 处理银行原始数据
        if data_models and 'bank' in data_models and data_models['bank']:
            bank_raw_data = self._get_bank_raw_data(data_models['bank'], analysis_results)
            if not bank_raw_data.empty:
                bank_raw_data.to_excel(writer, sheet_name='银行分析原始', index=False)
                worksheet = writer.sheets['银行分析原始']
                self.formatter.set_column_widths(worksheet, bank_raw_data)
                self.formatter.format_sheet(worksheet, bank_raw_data, writer.book)

        # 处理微信原始数据
        if data_models and 'wechat' in data_models and data_models['wechat']:
            wechat_raw_data = self._get_payment_raw_data(data_models['wechat'], analysis_results, '微信')
            if not wechat_raw_data.empty:
                wechat_raw_data.to_excel(writer, sheet_name='微信分析原始', index=False)
                worksheet = writer.sheets['微信分析原始']
                self.formatter.set_column_widths(worksheet, wechat_raw_data)
                self.formatter.format_sheet(worksheet, wechat_raw_data, writer.book)

        # 处理支付宝原始数据
        if data_models and 'alipay' in data_models and data_models['alipay']:
            alipay_raw_data = self._get_payment_raw_data(data_models['alipay'], analysis_results, '支付宝')
            if not alipay_raw_data.empty:
                alipay_raw_data.to_excel(writer, sheet_name='支付宝分析原始', index=False)
                worksheet = writer.sheets['支付宝分析原始']
                self.formatter.set_column_widths(worksheet, alipay_raw_data)
                self.formatter.format_sheet(worksheet, alipay_raw_data, writer.book)

        self.logger.info("已生成平台原始数据表")

    def _get_bank_raw_data(self, bank_model, analysis_results: Dict) -> pd.DataFrame:
        """获取银行平台的原始数据"""
        if not bank_model or bank_model.data.empty:
            return pd.DataFrame()

        all_raw_data = []

        # 1. 转账数据
        transfer_data = bank_model.data[bank_model.data['存取现标识'] == '转账'].copy()
        if not transfer_data.empty:
            transfer_data['分析类型'] = '转账数据'
            all_raw_data.append(transfer_data)

        # 2. 存现数据
        deposit_data = bank_model.data[bank_model.data['存取现标识'] == '存现'].copy()
        if not deposit_data.empty:
            deposit_data['分析类型'] = '存现数据'
            all_raw_data.append(deposit_data)

        # 3. 取现数据
        withdraw_data = bank_model.data[bank_model.data['存取现标识'] == '取现'].copy()
        if not withdraw_data.empty:
            withdraw_data['分析类型'] = '取现数据'
            all_raw_data.append(withdraw_data)

        # 4. 特殊金额数据（从分析结果中获取）
        special_amount_data = self._get_special_data_from_results(analysis_results, '特殊金额', '银行')
        if not special_amount_data.empty:
            special_amount_data['分析类型'] = '特殊金额'
            all_raw_data.append(special_amount_data)

        # 5. 整数金额数据（从分析结果中获取）
        integer_amount_data = self._get_special_data_from_results(analysis_results, '整数金额', '银行')
        if not integer_amount_data.empty:
            integer_amount_data['分析类型'] = '整百数金额'
            all_raw_data.append(integer_amount_data)

        # 6. 特殊日期数据（从分析结果中获取）
        special_date_data = self._get_special_data_from_results(analysis_results, '特殊日期', '银行')
        if not special_date_data.empty:
            special_date_data['分析类型'] = '特殊日期'
            all_raw_data.append(special_date_data)

        # 7. 重点收入数据（从分析结果中获取）
        key_income_data = self._get_key_transaction_data(bank_model, '收入')
        if not key_income_data.empty:
            key_income_data['分析类型'] = '重点收入'
            all_raw_data.append(key_income_data)

        # 8. 重点支出数据（从分析结果中获取）
        key_expense_data = self._get_key_transaction_data(bank_model, '支出')
        if not key_expense_data.empty:
            key_expense_data['分析类型'] = '重点支出'
            all_raw_data.append(key_expense_data)

        # 合并所有数据
        if all_raw_data:
            combined_data = pd.concat(all_raw_data, ignore_index=True)
            # 将分析类型列放在第一列
            cols = ['分析类型'] + [col for col in combined_data.columns if col != '分析类型']
            return combined_data[cols]

        return pd.DataFrame()

    def _get_payment_raw_data(self, payment_model, analysis_results: Dict, platform_name: str) -> pd.DataFrame:
        """获取微信/支付宝平台的原始数据"""
        if not payment_model or payment_model.data.empty:
            return pd.DataFrame()

        all_raw_data = []

        # 1. 转账数据（所有交易数据）
        transfer_data = payment_model.data.copy()
        if not transfer_data.empty:
            transfer_data['分析类型'] = '转账数据'
            all_raw_data.append(transfer_data)

        # 2. 特殊金额数据（从分析结果中获取）
        special_amount_data = self._get_special_data_from_results(analysis_results, '特殊金额', platform_name)
        if not special_amount_data.empty:
            special_amount_data['分析类型'] = '特殊金额'
            all_raw_data.append(special_amount_data)

        # 3. 整数金额数据（从分析结果中获取）
        integer_amount_data = self._get_special_data_from_results(analysis_results, '整数金额', platform_name)
        if not integer_amount_data.empty:
            integer_amount_data['分析类型'] = '整百数金额'
            all_raw_data.append(integer_amount_data)

        # 4. 特殊日期数据（从分析结果中获取）
        special_date_data = self._get_special_data_from_results(analysis_results, '特殊日期', platform_name)
        if not special_date_data.empty:
            special_date_data['分析类型'] = '特殊日期'
            all_raw_data.append(special_date_data)

        # 5. 重点收入数据
        key_income_data = self._get_key_transaction_data(payment_model, '收入')
        if not key_income_data.empty:
            key_income_data['分析类型'] = '重点收入'
            all_raw_data.append(key_income_data)

        # 6. 重点支出数据
        key_expense_data = self._get_key_transaction_data(payment_model, '支出')
        if not key_expense_data.empty:
            key_expense_data['分析类型'] = '重点支出'
            all_raw_data.append(key_expense_data)

        # 合并所有数据
        if all_raw_data:
            combined_data = pd.concat(all_raw_data, ignore_index=True)
            # 将分析类型列放在第一列
            cols = ['分析类型'] + [col for col in combined_data.columns if col != '分析类型']
            return combined_data[cols]

        return pd.DataFrame()

    def _get_special_data_from_results(self, analysis_results: Dict, data_type: str, platform: str) -> pd.DataFrame:
        """从分析结果中获取特殊数据"""
        for result_name, df in analysis_results.items():
            if (data_type in result_name and
                (platform == '银行' or platform in result_name or
                 ('微信' in platform and 'Wechat' in result_name) or
                 ('支付宝' in platform and 'Alipay' in result_name))):
                return df.copy()
        return pd.DataFrame()

    def _get_key_transaction_data(self, data_model, transaction_type: str) -> pd.DataFrame:
        """获取重点交易数据"""
        try:
            from src.utils.model.key_transactions import KeyTransactionEngine

            # 获取配置对象
            config = getattr(data_model, 'config', None)
            key_engine = KeyTransactionEngine(config)

            if not key_engine.enabled:
                return pd.DataFrame()

            # 识别重点收支
            if hasattr(data_model, 'summary_column'):
                # 银行数据
                key_data = key_engine.identify_key_transactions(
                    data_model.data,
                    data_model.summary_column,
                    data_model.remark_column,
                    getattr(data_model, 'type_column', None),
                    data_model.amount_column,
                    data_model.opposite_name_column
                )
            else:
                # 微信/支付宝数据
                key_data = key_engine.identify_key_transactions(
                    data_model.data,
                    None,
                    getattr(data_model, 'remark_column', None),
                    getattr(data_model, 'type_column', None),
                    data_model.amount_column,
                    data_model.opposite_name_column
                )

            # 筛选出重点收支数据
            key_transactions = key_data[key_data['是否重点收支']].copy()

            if transaction_type == '收入':
                return key_transactions[key_transactions[data_model.amount_column] > 0]
            elif transaction_type == '支出':
                return key_transactions[key_transactions[data_model.amount_column] < 0]
            else:
                return key_transactions

        except Exception as e:
            self.logger.error(f"获取重点交易数据时出错: {e}")
            return pd.DataFrame()

    def export_raw_bank_data(self, writer: pd.ExcelWriter, bank_model: 'BankDataModel'):
        """
        导出原始的银行转账、存现、取现数据到不同的sheet。

        Parameters:
        -----------
        writer : pd.ExcelWriter
            Excel写入器对象
        bank_model : BankDataModel
            包含银行数据的模型
        """
        if not bank_model or bank_model.data.empty:
            return

        full_data = bank_model.data.copy()

        # 筛选不同类型的数据
        transfer_data = full_data[full_data['存取现标识'] == '转账']
        deposit_data = full_data[full_data['存取现标识'] == '存现']
        withdrawal_data = full_data[full_data['存取现标识'] == '取现']

        # 导出到不同的sheet
        if not transfer_data.empty:
            transfer_data.to_excel(writer, sheet_name='转账数据(原始)', index=False)
            worksheet = writer.sheets['转账数据(原始)']
            self.formatter.set_column_widths(worksheet, transfer_data)
            self.formatter.format_sheet(worksheet, transfer_data, writer.book)

        if not deposit_data.empty:
            deposit_data.to_excel(writer, sheet_name='存现数据(原始)', index=False)
            worksheet = writer.sheets['存现数据(原始)']
            self.formatter.set_column_widths(worksheet, deposit_data)
            self.formatter.format_sheet(worksheet, deposit_data, writer.book)
        
        if not withdrawal_data.empty:
            withdrawal_data.to_excel(writer, sheet_name='取现数据(原始)', index=False)
            worksheet = writer.sheets['取现数据(原始)']
            self.formatter.set_column_widths(worksheet, withdrawal_data)
            self.formatter.format_sheet(worksheet, withdrawal_data, writer.book)

        self.logger.info("已添加原始数据表")

    def export_key_transactions(self, writer: pd.ExcelWriter, data_model: 'BaseDataModel', data_type: str = 'bank'):
        """
        导出重点收支数据到不同的sheet

        Parameters:
        -----------
        writer : pd.ExcelWriter
            Excel写入器对象
        data_model : BaseDataModel
            数据模型（银行、微信或支付宝）
        data_type : str
            数据类型，可选值：'bank', 'wechat', 'alipay'
        """
        if not data_model or data_model.data.empty:
            return

        try:
            # 导入重点收支识别引擎
            from src.utils.model.key_transactions import KeyTransactionEngine

            # 初始化重点收支识别引擎
            key_engine = KeyTransactionEngine(self.config)

            if not key_engine.enabled:
                self.logger.info("重点收支识别功能已禁用，跳过重点收支数据导出")
                return

            # 根据数据类型获取相应的列名
            if data_type == 'bank':
                summary_column = getattr(data_model, 'summary_column', None)
                remark_column = getattr(data_model, 'remark_column', None)
                type_column = getattr(data_model, 'type_column', None)
            else:
                # 微信和支付宝没有摘要列，使用备注列作为匹配文本
                summary_column = None
                remark_column = getattr(data_model, 'remark_column', None)
                type_column = getattr(data_model, 'type_column', None)

            # 识别重点收支
            key_data = key_engine.identify_key_transactions(
                data_model.data,
                summary_column,
                remark_column,
                type_column,
                data_model.amount_column,
                data_model.opposite_name_column
            )

            # 筛选出重点收支数据
            key_transactions = key_data[key_data['是否重点收支']].copy()

            if not key_transactions.empty:
                # 根据数据类型生成sheet名称
                data_type_name = {'bank': '银行', 'wechat': '微信', 'alipay': '支付宝'}.get(data_type, data_type)
                raw_sheet_name = f'{data_type_name}重点收支(原始)'
                stats_sheet_name = f'{data_type_name}重点收支(统计)'

                # 导出重点收支原始数据
                key_transactions.to_excel(writer, sheet_name=raw_sheet_name, index=False)
                worksheet = writer.sheets[raw_sheet_name]
                self.formatter.set_column_widths(worksheet, key_transactions)
                self.formatter.format_sheet(worksheet, key_transactions, writer.book)

                # 生成重点收支统计数据
                key_stats = key_engine.generate_statistics(
                    key_data,
                    data_model.name_column,
                    data_model.amount_column,
                    data_model.date_column,
                    data_model.opposite_name_column
                )

                if not key_stats.empty:
                    # 导出重点收支统计数据
                    key_stats.to_excel(writer, sheet_name=stats_sheet_name, index=False)
                    worksheet = writer.sheets[stats_sheet_name]
                    self.formatter.set_column_widths(worksheet, key_stats)
                    self.formatter.format_sheet(worksheet, key_stats, writer.book)

                self.logger.info(f"已添加{data_type_name}重点收支数据表，原始数据 {len(key_transactions)} 笔，统计数据 {len(key_stats)} 人")
            else:
                data_type_name = {'bank': '银行', 'wechat': '微信', 'alipay': '支付宝'}.get(data_type, data_type)
                self.logger.info(f"未发现{data_type_name}重点收支数据，跳过重点收支数据导出")

        except Exception as e:
            self.logger.error(f"导出重点收支数据时出错: {e}", exc_info=True)

    def _set_column_widths(self, worksheet, df: pd.DataFrame):
        """
        根据列内容设置Excel列宽。

        Parameters:
        -----------
        worksheet : worksheet
            Excel工作表对象
        df : pd.DataFrame
            数据框
        """
        default_width = self.excel_config.get('default_width', 15)
        
        for idx, col in enumerate(df.columns):
            # 计算列宽
            max_len = max(
                df[col].astype(str).apply(lambda x: len(str(x))).max(),  # 数据的最大长度
                len(str(col))  # 列名的长度
            ) + 2  # 添加一些额外空间
            
            # 限制最大/最小宽度
            col_width = min(max(max_len, 8), 50)
            
            # 特定类型列的默认宽度
            if '日期' in col or 'date' in col.lower():
                col_width = max(col_width, 12)
            elif '金额' in col or '价格' in col or 'amount' in col.lower() or 'price' in col.lower():
                col_width = max(col_width, 12)
            elif '姓名' in col or 'name' in col.lower():
                col_width = max(col_width, 10)
            elif '描述' in col or '说明' in col or '备注' in col or 'description' in col.lower():
                col_width = max(col_width, 30)
            
            worksheet.set_column(idx, idx, col_width)

    def _format_sheet(self, worksheet, df: pd.DataFrame, workbook):
        """
        为工作表添加格式，如表头格式、数值格式等。

        Parameters:
        -----------
        worksheet : worksheet
            Excel工作表对象
        df : pd.DataFrame
            数据框
        workbook : workbook
            Excel工作簿对象
        """
        # 创建格式
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'bg_color': '#D9E1F2',
            'border': 1
        })

        # 不同类型的数值格式
        money_format = workbook.add_format({'num_format': '#,##0.00'})  # 金额格式
        integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
        phone_format = workbook.add_format({'num_format': '@'})  # 文本格式
        date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})

        # 应用表头格式
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # 为不同类型的列应用相应格式
        for col_num, col_name in enumerate(df.columns):
            col_name_lower = col_name.lower()

            # 序号、次数、数量等应该是整数
            if any(keyword in col_name for keyword in ['序号', '次数', '数量', '笔数', '个数', '排名']):
                worksheet.set_column(col_num, col_num, None, integer_format)
            # 电话号码、银行卡号、身份证号等应该是文本
            elif any(keyword in col_name for keyword in ['电话', '号码', '手机', '银行卡', '身份证', '卡号']):
                worksheet.set_column(col_num, col_num, None, phone_format)
            # 金额相关列
            elif any(keyword in col_name for keyword in ['金额', '总额', '收入', '支出', '余额', '价格']):
                worksheet.set_column(col_num, col_num, None, money_format)
            # 日期列
            elif col_name in df and pd.api.types.is_datetime64_any_dtype(df[col_name]):
                worksheet.set_column(col_num, col_num, None, date_format)
            # 其他数值列但不是金额的，检查是否应该是整数
            elif col_name in df and pd.api.types.is_numeric_dtype(df[col_name]):
                # 检查数据是否都是整数
                if df[col_name].notna().any():
                    sample_values = df[col_name].dropna().head(10)
                    if all(float(val).is_integer() for val in sample_values if pd.notna(val)):
                        worksheet.set_column(col_num, col_num, None, integer_format)
                    else:
                        worksheet.set_column(col_num, col_num, None, money_format)
        
        # 添加条件格式
        if self.excel_config.get('conditional_formatting', True):
            self._add_conditional_formatting(worksheet, df, workbook)
        
        # 添加自动筛选
        if self.excel_config.get('auto_filter', True):
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
            
        # 冻结窗格（冻结表头）
        if self.excel_config.get('freeze_panes', True):
            worksheet.freeze_panes(1, 0)  # 冻结第一行
    
    def _add_conditional_formatting(self, worksheet, df: pd.DataFrame, workbook):
        """
        为工作表添加条件格式，例如给金额列添加色阶。

        Parameters:
        -----------
        worksheet : worksheet
            Excel工作表对象
        df : pd.DataFrame
            数据框
        workbook : workbook
            Excel工作簿对象
        """
        # 为金额列添加色阶
        for col_num, col_name in enumerate(df.columns):
            # 检查是否为数值列且列名包含"金额"或"金"
            if (col_name in df and 
                pd.api.types.is_numeric_dtype(df[col_name]) and 
                ('金额' in col_name or '金' in col_name or 'amount' in col_name.lower())):
                
                # 使用绿色色阶
                worksheet.conditional_format(1, col_num, len(df), col_num, {
                    'type': '3_color_scale',
                    'min_color': '#FFFFFF',
                    'mid_color': '#9BDC91',
                    'max_color': '#01933B'
                })

        # 筛选负值
        neg_format = workbook.add_format({'font_color': 'red'})
        worksheet.conditional_format(1, 0, len(df), len(df.columns) - 1, {
            'type': 'cell',
            'criteria': '<',
            'value': 0,
            'format': neg_format
        })
    
    # Obsolete functions removed.
    # The methods get_output_path, export_all_to_excel, export_raw_data,
    # export_cash_operations, and export_bank_transactions were here.