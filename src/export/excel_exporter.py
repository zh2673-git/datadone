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
from src.base import BaseDataModel
from src.datasource import BankDataModel
from src.utils.config import Config
from src.utils.constants import FilePath
from src.utils.exceptions import ExportError

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
        
        # 进度显示
        print("开始生成Excel报告...")
        total_steps = 8
        current_step = 0
        
        def update_progress(step_name):
            nonlocal current_step
            current_step += 1
            progress = (current_step / total_steps) * 100
            print(f"[{current_step}/{total_steps}] {step_name}... ({progress:.1f}%)")
        
        try:
            # 性能优化：配置Excel写入器选项，提升大数据量写入性能
            with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                # 获取话单数据中的对方单位信息
                update_progress("提取单位信息")
                company_position_map = {}
                if include_unit_info and data_models and 'call' in data_models and data_models['call']:
                    company_position_map = self._extract_company_info(data_models['call'].data)

                # 收集和分类分析结果
                update_progress("分类分析结果")
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
                            df_copy = self._standardize_call_frequency_table(df_copy)
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
                            df_copy = self._standardize_frequency_table(df_copy)

                        frequency_analysis_dfs.append(df_copy)
                        continue

                    # 检查是否是以XX为基准的综合分析结果，如果是则跳过（因为已有综合分析总表）
                    if '综合分析_以' in result_name and '为基准' in result_name:
                        self.logger.info(f"跳过冗余的综合分析表: {result_name}")
                        continue

                    # 其他分析结果
                    if include_unit_info and '对方姓名' in df.columns:
                        df = self._add_company_info(df, company_position_map)
                    other_results[result_name] = df

                # 按照指定顺序生成Excel表：分析汇总、频率（账单类、话单类分开）、综合分析、银行分析原始、微信分析原始、支付宝分析原始、高级分析

                # 1. 分析汇总表
                if add_summaries:
                    update_progress("生成分析汇总表")
                    self.logger.info("正在生成分析汇总表...")
                    self.add_comprehensive_summary_sheets(writer, data_models)

                # 2. 频率表（账单类、话单类分开）
                if frequency_analysis_dfs:
                    update_progress("生成频率分析表")
                    # 分离话单类和账单类频率表
                    call_frequency_dfs = [df for df in frequency_analysis_dfs if df['平台'].iloc[0] == '话单']
                    bill_frequency_dfs = [df for df in frequency_analysis_dfs if df['平台'].iloc[0] != '话单']

                    # 生成账单类频率表
                    if bill_frequency_dfs:
                        # 性能优化：使用更高效的DataFrame合并方式
                        combined_bill_df = pd.concat(bill_frequency_dfs, ignore_index=True, sort=False)
                        base_cols = ['平台', '数据来源', '本方姓名', '对方姓名']
                        other_cols = [col for col in combined_bill_df.columns if col not in base_cols]
                        final_cols = base_cols + other_cols
                        combined_bill_df = combined_bill_df[[col for col in final_cols if col in combined_bill_df.columns]]

                        # 性能优化：设置Excel写入选项，提升大数据量写入性能
                        combined_bill_df.to_excel(writer, sheet_name='账单类频率表', index=False, engine='xlsxwriter')
                        worksheet = writer.sheets['账单类频率表']
                        self._set_column_widths(worksheet, combined_bill_df)
                        self._format_sheet(worksheet, combined_bill_df, writer.book)

                    # 生成话单类频率表
                    if call_frequency_dfs:
                        # 性能优化：使用更高效的DataFrame合并方式
                        combined_call_df = pd.concat(call_frequency_dfs, ignore_index=True, sort=False)
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

                        # 性能优化：设置Excel写入选项，提升大数据量写入性能
                        combined_call_df.to_excel(writer, sheet_name='话单类频率表', index=False, engine='xlsxwriter')
                        worksheet = writer.sheets['话单类频率表']
                        self._set_column_widths(worksheet, combined_call_df)
                        self._format_sheet(worksheet, combined_call_df, writer.book)

                    # 3. 综合分析表（交叉分析）
                    if call_frequency_dfs or bill_frequency_dfs:
                        update_progress("生成综合分析表")
                        self._generate_comprehensive_analysis(writer, call_frequency_dfs, bill_frequency_dfs)

                # 4. 平台原始数据表（银行分析原始、微信分析原始、支付宝分析原始）
                if add_raw_data:
                    update_progress("生成平台原始数据表")
                    self.logger.info("正在生成平台原始数据表...")
                    self.export_platform_raw_data(writer, data_models, analysis_results)

                # 5. 高级分析表
                if advanced_analysis_dfs:
                    update_progress("生成高级分析表")
                    # 性能优化：使用更高效的DataFrame合并方式
                    combined_advanced_df = pd.concat(advanced_analysis_dfs, ignore_index=True, sort=False)
                    # 重新排列列的顺序，将数据来源放在前面
                    if '数据来源' in combined_advanced_df.columns:
                        cols = ['数据来源'] + [col for col in combined_advanced_df.columns if col != '数据来源']
                        combined_advanced_df = combined_advanced_df[cols]

                    # 性能优化：设置Excel写入选项，提升大数据量写入性能
                    combined_advanced_df.to_excel(writer, sheet_name='高级分析', index=False, engine='xlsxwriter')
                    worksheet = writer.sheets['高级分析']
                    self._set_column_widths(worksheet, combined_advanced_df)
                    self._format_sheet(worksheet, combined_advanced_df, writer.book)

                # 6. 大额资金追踪表（新增功能）
                if data_models:
                    update_progress("生成大额资金跟踪表")
                    self.logger.info("正在生成大额资金追踪表...")
                    fund_tracking_df = self._generate_fund_tracking_sheet(data_models)
                    if not fund_tracking_df.empty:
                        # 性能优化：设置Excel写入选项，提升大数据量写入性能
                        fund_tracking_df.to_excel(writer, sheet_name='大额资金跟踪', index=False, engine='xlsxwriter')
                        worksheet = writer.sheets['大额资金跟踪']
                        self._set_column_widths(worksheet, fund_tracking_df)
                        self._format_sheet(worksheet, fund_tracking_df, writer.book)
                        self.logger.info("已添加大额资金追踪表")

                # 注意：按照用户要求，大额资金追踪表之后的所有表格都不再生成
                # 包括：其他分析结果、重点收支数据表等

                self.logger.info(f"已按指定顺序生成核心分析表格，共 {len(analysis_results)} 个分析结果")

            update_progress("完成Excel报告生成")
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
            self._set_column_widths(worksheet, combined_summary)
            self._format_sheet(worksheet, combined_summary, writer.book)

        self.logger.info("已添加分析汇总表")

    def add_comprehensive_summary_sheets(self, writer: pd.ExcelWriter, data_models: Dict):
        """
        添加综合分析汇总表，包含银行、微信、支付宝的汇总数据。
        按本方姓名和平台类型进行汇总统计，解决同一人在不同账单文件中重复出现的问题。

        Parameters:
        -----------
        writer : pd.ExcelWriter
            Excel写入器对象
        data_models : Dict
            包含各种数据模型的字典
        """
        all_summary_data = []

        # 处理银行数据
        if data_models and 'bank' in data_models and data_models['bank']:
            bank_summary = self._get_bank_summary_data_by_person_platform(data_models['bank'])
            if not bank_summary.empty:
                all_summary_data.append(bank_summary)

        # 处理微信数据
        if data_models and 'wechat' in data_models and data_models['wechat']:
            wechat_summary = self._get_payment_summary_data_by_person_platform(data_models['wechat'], '微信')
            if not wechat_summary.empty:
                all_summary_data.append(wechat_summary)

        # 处理支付宝数据
        if data_models and 'alipay' in data_models and data_models['alipay']:
            alipay_summary = self._get_payment_summary_data_by_person_platform(data_models['alipay'], '支付宝')
            if not alipay_summary.empty:
                all_summary_data.append(alipay_summary)

        # 合并所有汇总数据
        if all_summary_data:
            combined_summary = pd.concat(all_summary_data, ignore_index=True)

            # 导出合并后的分析汇总表
            combined_summary.to_excel(writer, sheet_name='分析汇总表', index=False)
            worksheet = writer.sheets['分析汇总表']
            self._set_column_widths(worksheet, combined_summary)
            self._format_sheet(worksheet, combined_summary, writer.book)

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

    def _get_bank_summary_data_by_person_platform(self, bank_model) -> pd.DataFrame:
        """
        按本方姓名和平台类型获取银行数据的汇总信息
        解决同一人在不同账单文件中重复出现的问题
        
        Parameters:
        -----------
        bank_model : object
            银行数据模型
            
        Returns:
        --------
        pd.DataFrame
            按人员平台汇总的银行数据
        """
        if not bank_model or bank_model.data.empty:
            return pd.DataFrame()

        # 性能优化：避免不必要的DataFrame复制，直接使用原始数据
        full_data = bank_model.data
        group_keys = ['本方姓名']

        # 性能优化：使用向量化操作和条件筛选，避免lambda函数
        # 存取现汇总
        cash_deposit_mask = full_data['存取现标识'] == '存现'
        cash_withdraw_mask = full_data['存取现标识'] == '取现'
        
        cash_summary = full_data.groupby(group_keys).agg({
            '收入金额': lambda x: x[cash_deposit_mask[x.index]].sum(),
            '支出金额': lambda x: x[cash_withdraw_mask[x.index]].sum()
        }).reset_index()
        
        # 重命名列
        cash_summary.columns = ['本方姓名', '存现金额', '取现金额']
        cash_summary = cash_summary[(cash_summary['存现金额'] > 0) | (cash_summary['取现金额'] > 0)]

        # 性能优化：先筛选再分组，减少计算量
        transfer_data = full_data[full_data['存取现标识'] == '转账']
        if not transfer_data.empty:
            transfer_summary = transfer_data.groupby(group_keys).agg({
                '收入金额': 'sum',
                '支出金额': 'sum'
            }).reset_index()
            transfer_summary.columns = ['本方姓名', '转入金额', '转出金额']
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
            base_cols = ['分析类型', '平台', '本方姓名', '存现金额', '取现金额', '转入金额', '转出金额']
            final_cols = [col for col in base_cols if col in result.columns]
            return result[final_cols]

        return pd.DataFrame()

    def _get_payment_summary_data_by_person_platform(self, payment_model, platform_name: str) -> pd.DataFrame:
        """
        按本方姓名和平台类型获取微信/支付宝数据的汇总信息
        解决同一人在不同账单文件中重复出现的问题
        
        Parameters:
        -----------
        payment_model : object
            支付数据模型
        platform_name : str
            平台名称
            
        Returns:
        --------
        pd.DataFrame
            按人员平台汇总的支付数据
        """
        if not payment_model or payment_model.data.empty:
            return pd.DataFrame()

        full_data = payment_model.data.copy()

        # 按本方姓名和平台类型进行分组统计（忽略数据来源）
        group_keys = ['本方姓名']

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
            base_cols = ['分析类型', '平台', '本方姓名', '存现金额', '取现金额', '转入金额', '转出金额']
            final_cols = [col for col in base_cols if col in summary.columns]
            return summary[final_cols]

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
            call_based_analysis = self._cross_analyze_with_call_base(combined_call_df, combined_bill_df)
            if not call_based_analysis.empty:
                call_based_analysis['分析基准'] = '以话单为基准'
                comprehensive_results.append(call_based_analysis)

        # 2. 以账单类为基准的交叉分析（如果有话单和账单数据）
        if not combined_bill_df.empty and not combined_call_df.empty:
            bill_based_analysis = self._cross_analyze_with_bill_base(combined_bill_df, combined_call_df)
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
            self._set_column_widths(worksheet, final_comprehensive_df)
            self._format_sheet(worksheet, final_comprehensive_df, writer.book)

            self.logger.info("已生成综合分析表")

    def _cross_analyze_with_call_base(self, call_df: pd.DataFrame, bill_df: pd.DataFrame) -> pd.DataFrame:
        """以话单为基准进行交叉分析，支持跨数据源对手信息显示"""
        # 以话单数据为基础，不创建额外组合

        # 基于对方姓名进行匹配，并计算各平台的金额分布
        bill_platform_summary = bill_df.groupby(['本方姓名', '对方姓名', '平台']).agg({
            '收入总额': 'sum',
            '支出总额': 'sum',
            '交易次数': 'sum'
        }).reset_index()

        # 计算总金额
        bill_total_summary = bill_df.groupby(['本方姓名', '对方姓名']).agg({
            '收入总额': 'sum',
            '支出总额': 'sum',
            '交易次数': 'sum',
            '平台': lambda x: '、'.join(x.unique())
        }).reset_index()

        # 计算各平台的金额分布
        platform_details = bill_platform_summary.groupby(['本方姓名', '对方姓名']).apply(
            lambda group: self._format_platform_details(group)
        ).reset_index(name='平台金额分布')

        # 为每个平台创建独立字段
        platform_individual_data = {}
        if not bill_platform_summary.empty:
            platforms = bill_platform_summary['平台'].unique()
            for platform in platforms:
                platform_data = bill_platform_summary[bill_platform_summary['平台'] == platform]
                platform_summary = platform_data.groupby(['本方姓名', '对方姓名']).agg({
                    '收入总额': 'sum',
                    '支出总额': 'sum',
                    '交易次数': 'sum'
                }).reset_index()

                # 重命名列以区分不同平台
                platform_summary = platform_summary.rename(columns={
                    '收入总额': f'{platform}_收入总额',
                    '支出总额': f'{platform}_支出总额',
                    '交易次数': f'{platform}_交易次数'
                })

                platform_individual_data[platform] = platform_summary

        # 合并总金额和平台详情
        if not bill_total_summary.empty and not platform_details.empty:
            bill_summary_with_details = pd.merge(bill_total_summary, platform_details, on=['本方姓名', '对方姓名'])
        else:
            bill_summary_with_details = bill_total_summary.copy() if not bill_total_summary.empty else pd.DataFrame()

        # 获取话单中的对方详细信息
        agg_dict = {
            '通话次数': 'sum',
            '数据来源': 'first'
        }

        # 检查通话时长列名
        if '通话总时长(分钟)' in call_df.columns:
            agg_dict['通话总时长(分钟)'] = 'sum'
        elif '通话时长' in call_df.columns:
            agg_dict['通话时长'] = 'sum'

        # 安全地添加可选字段
        if '对方号码' in call_df.columns:
            agg_dict['对方号码'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        # 检查带lambda后缀的字段名（来自话单频率分析）
        if '对方单位名称_<lambda>' in call_df.columns:
            agg_dict['对方单位名称_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        elif '对方单位名称' in call_df.columns:
            agg_dict['对方单位名称'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        if '对方职务_<lambda>' in call_df.columns:
            agg_dict['对方职务_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        elif '对方职务' in call_df.columns:
            agg_dict['对方职务'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''

        call_details = call_df.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()

        # 以话单数据为基础进行合并
        merged_df = call_details.copy()

        # 与账单数据合并 - 严格匹配，禁止跨人员关联
        if not bill_summary_with_details.empty:
            # 只进行完全匹配（本方姓名+对方姓名），禁止跨人员匹配
            merged_df = pd.merge(
                merged_df,
                bill_summary_with_details,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

        # 与各平台独立数据合并 - 严格匹配，禁止跨人员关联
        for platform, platform_data in platform_individual_data.items():
            # 只进行完全匹配（本方姓名+对方姓名），禁止跨人员匹配
            merged_df = pd.merge(
                merged_df,
                platform_data,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

        # 填充空值
        merged_df['收入总额'] = merged_df['收入总额'].fillna(0)
        merged_df['支出总额'] = merged_df['支出总额'].fillna(0)
        merged_df['交易次数'] = merged_df['交易次数'].fillna(0)
        merged_df['平台'] = merged_df['平台'].fillna('无')
        merged_df['平台金额分布'] = merged_df['平台金额分布'].fillna('无')

        # 填充各平台的金额字段
        for col in merged_df.columns:
            if any(platform in col for platform in ['银行', '微信', '支付宝']) and any(field in col for field in ['收入总额', '支出总额', '交易次数']):
                merged_df[col] = merged_df[col].fillna(0)

        # 安全地填充可选字段的空值
        if '对方号码' in merged_df.columns:
            merged_df['对方号码'] = merged_df['对方号码'].fillna('')
        if '对方单位名称_<lambda>' in merged_df.columns:
            merged_df['对方单位名称_<lambda>'] = merged_df['对方单位名称_<lambda>'].fillna('')
        elif '对方单位名称' in merged_df.columns:
            merged_df['对方单位名称'] = merged_df['对方单位名称'].fillna('')
        if '对方职务_<lambda>' in merged_df.columns:
            merged_df['对方职务_<lambda>'] = merged_df['对方职务_<lambda>'].fillna('')
        elif '对方职务' in merged_df.columns:
            merged_df['对方职务'] = merged_df['对方职务'].fillna('')

        # 重新排列列的顺序，将对方详细信息放在对方姓名后面
        base_columns = ['本方姓名', '对方姓名']
        detail_columns = []

        # 安全地添加存在的详细信息字段，优先使用带lambda后缀的字段
        if '对方号码' in merged_df.columns:
            detail_columns.append('对方号码')
        if '对方单位名称_<lambda>' in merged_df.columns:
            detail_columns.append('对方单位名称_<lambda>')
        elif '对方单位名称' in merged_df.columns:
            detail_columns.append('对方单位名称')
        if '对方职务_<lambda>' in merged_df.columns:
            detail_columns.append('对方职务_<lambda>')
        elif '对方职务' in merged_df.columns:
            detail_columns.append('对方职务')

        # 话单相关列
        call_columns = []
        if '通话次数' in merged_df.columns:
            call_columns.append('通话次数')
        if '通话总时长(分钟)' in merged_df.columns:
            call_columns.append('通话总时长(分钟)')
        elif '通话时长' in merged_df.columns:
            call_columns.append('通话时长')
        if '数据来源' in merged_df.columns:
            call_columns.append('数据来源')

        # 账单汇总列
        bill_summary_columns = []
        if '收入总额' in merged_df.columns:
            bill_summary_columns.extend(['收入总额', '支出总额', '交易次数'])
        if '平台' in merged_df.columns:
            bill_summary_columns.append('平台')
        if '平台金额分布' in merged_df.columns:
            bill_summary_columns.append('平台金额分布')

        # 各平台独立列（按平台名称排序）
        platform_columns = []
        platforms = ['银行', '微信', '支付宝']
        for platform in platforms:
            for field in ['收入总额', '支出总额', '交易次数']:
                col_name = f'{platform}_{field}'
                if col_name in merged_df.columns:
                    platform_columns.append(col_name)

        # 剩余列
        used_columns = base_columns + detail_columns + call_columns + bill_summary_columns + platform_columns
        remaining_columns = [col for col in merged_df.columns if col not in used_columns]

        final_columns = base_columns + detail_columns + call_columns + bill_summary_columns + platform_columns + remaining_columns
        merged_df = merged_df[[col for col in final_columns if col in merged_df.columns]]

        return merged_df

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
            base_platform_df = combined_bill_df[combined_bill_df['平台'] == base_platform]

            # 获取其他数据源（其他账单平台 + 话单）
            other_bill_platforms_df = combined_bill_df[combined_bill_df['平台'] != base_platform]

            # 检查是否有其他数据源可以进行交叉分析
            has_other_bill_data = not other_bill_platforms_df.empty
            has_call_data = not combined_call_df.empty

            if has_other_bill_data or has_call_data:
                cross_analysis = self._cross_analyze_with_platform_base(
                    base_platform_df, other_bill_platforms_df, combined_call_df, base_platform
                )
                if not cross_analysis.empty:
                    cross_analysis['分析基准'] = f'以{base_platform}为基准'
                    results.append(cross_analysis)

        return results

    def _cross_analyze_with_platform_base(self, base_platform_df: pd.DataFrame, other_bill_platforms_df: pd.DataFrame,
                                          call_df: pd.DataFrame, base_platform: str) -> pd.DataFrame:
        """
        以特定平台为基准进行交叉分析（与其他账单平台和话单数据）

        Parameters:
        -----------
        base_platform_df : pd.DataFrame
            基准平台的数据
        other_bill_platforms_df : pd.DataFrame
            其他账单平台的数据
        call_df : pd.DataFrame
            话单数据
        base_platform : str
            基准平台名称

        Returns:
        --------
        pd.DataFrame
            交叉分析结果
        """
        # 获取基准平台的详细信息
        agg_dict = {
            '收入总额': 'sum',
            '支出总额': 'sum',
            '交易次数': 'sum'
        }

        # 安全地添加数据来源字段
        if '数据来源' in base_platform_df.columns:
            agg_dict['数据来源'] = 'first'

        base_details = base_platform_df.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()

        # 重命名基准平台的列以区分
        base_details = base_details.rename(columns={
            '收入总额': f'{base_platform}_收入总额',
            '支出总额': f'{base_platform}_支出总额',
            '交易次数': f'{base_platform}_交易次数'
        })

        # 处理其他账单平台数据，为每个平台创建独立字段
        other_platforms_data = {}
        platform_details_list = []

        if not other_bill_platforms_df.empty:
            # 获取所有其他平台
            other_platforms = other_bill_platforms_df['平台'].unique()

            # 为每个其他平台创建独立的汇总数据
            for platform in other_platforms:
                platform_data = other_bill_platforms_df[other_bill_platforms_df['平台'] == platform]
                platform_summary = platform_data.groupby(['本方姓名', '对方姓名']).agg({
                    '收入总额': 'sum',
                    '支出总额': 'sum',
                    '交易次数': 'sum'
                }).reset_index()

                # 重命名列以区分不同平台
                platform_summary = platform_summary.rename(columns={
                    '收入总额': f'{platform}_收入总额',
                    '支出总额': f'{platform}_支出总额',
                    '交易次数': f'{platform}_交易次数'
                })

                other_platforms_data[platform] = platform_summary

            # 计算其他平台汇总信息（用于"平台"字段）
            other_bill_total_summary = other_bill_platforms_df.groupby(['本方姓名', '对方姓名']).agg({
                '平台': lambda x: '、'.join(x.unique())
            }).reset_index()

            # 计算平台金额分布详情
            other_bill_platform_summary = other_bill_platforms_df.groupby(['本方姓名', '对方姓名', '平台']).agg({
                '收入总额': 'sum',
                '支出总额': 'sum',
                '交易次数': 'sum'
            }).reset_index()

            platform_details = other_bill_platform_summary.groupby(['本方姓名', '对方姓名']).apply(
                lambda group: self._format_platform_details(group)
            ).reset_index(name='其他账单平台金额分布')

            # 合并平台信息和平台详情
            other_platform_info = pd.merge(other_bill_total_summary, platform_details, on=['本方姓名', '对方姓名'])
            platform_details_list.append(other_platform_info)

        # 处理话单数据
        call_summary = pd.DataFrame()
        if not call_df.empty:
            # 获取话单中的对方详细信息
            agg_dict = {
                '通话次数': 'sum'
            }

            # 检查通话时长列名
            if '通话总时长(分钟)' in call_df.columns:
                agg_dict['通话总时长(分钟)'] = 'sum'
            elif '通话时长' in call_df.columns:
                agg_dict['通话时长'] = 'sum'

            # 安全地添加可选字段
            if '对方号码' in call_df.columns:
                agg_dict['对方号码'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            # 检查带lambda后缀的字段名（来自话单频率分析）
            if '对方单位名称_<lambda>' in call_df.columns:
                agg_dict['对方单位名称_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            elif '对方单位名称' in call_df.columns:
                agg_dict['对方单位名称'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            if '对方职务_<lambda>' in call_df.columns:
                agg_dict['对方职务_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            elif '对方职务' in call_df.columns:
                agg_dict['对方职务'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''

            call_summary = call_df.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()

        # 开始合并数据
        merged_df = base_details.copy()

        # 与每个其他账单平台数据合并
        for platform, platform_data in other_platforms_data.items():
            merged_df = pd.merge(
                merged_df,
                platform_data,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

        # 合并平台金额分布详情
        if platform_details_list:
            for platform_details in platform_details_list:
                merged_df = pd.merge(
                    merged_df,
                    platform_details,
                    on=['本方姓名', '对方姓名'],
                    how='left'
                )

        # 与话单数据合并
        if not call_summary.empty:
            merged_df = pd.merge(
                merged_df,
                call_summary,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

        # 填充空值
        # 填充各平台的金额字段
        for col in merged_df.columns:
            if any(platform in col for platform in ['银行', '微信', '支付宝']) and any(field in col for field in ['收入总额', '支出总额', '交易次数']):
                merged_df[col] = merged_df[col].fillna(0)

        # 填充其他字段
        if '平台' in merged_df.columns:
            merged_df['平台'] = merged_df['平台'].fillna('无')
        if '平台金额分布' in merged_df.columns:
            merged_df['平台金额分布'] = merged_df['平台金额分布'].fillna('无')
        if '其他账单平台金额分布' in merged_df.columns:
            merged_df['其他账单平台金额分布'] = merged_df['其他账单平台金额分布'].fillna('无')
        if '通话次数' in merged_df.columns:
            merged_df['通话次数'] = merged_df['通话次数'].fillna(0)
        if '通话总时长(分钟)' in merged_df.columns:
            merged_df['通话总时长(分钟)'] = merged_df['通话总时长(分钟)'].fillna(0)
        elif '通话时长' in merged_df.columns:
            merged_df['通话时长'] = merged_df['通话时长'].fillna(0)
        if '数据来源' in merged_df.columns:
            merged_df['数据来源'] = merged_df['数据来源'].fillna('未知')

        # 安全地填充可选字段的空值
        if '对方号码' in merged_df.columns:
            merged_df['对方号码'] = merged_df['对方号码'].fillna('')
        if '对方单位名称_<lambda>' in merged_df.columns:
            merged_df['对方单位名称_<lambda>'] = merged_df['对方单位名称_<lambda>'].fillna('')
        elif '对方单位名称' in merged_df.columns:
            merged_df['对方单位名称'] = merged_df['对方单位名称'].fillna('')
        if '对方职务_<lambda>' in merged_df.columns:
            merged_df['对方职务_<lambda>'] = merged_df['对方职务_<lambda>'].fillna('')
        elif '对方职务' in merged_df.columns:
            merged_df['对方职务'] = merged_df['对方职务'].fillna('')

        # 重新排列列的顺序
        base_columns = ['本方姓名', '对方姓名']

        # 添加对方详细信息字段，优先使用带lambda后缀的字段
        detail_columns = []
        if '对方号码' in merged_df.columns:
            detail_columns.append('对方号码')
        if '对方单位名称_<lambda>' in merged_df.columns:
            detail_columns.append('对方单位名称_<lambda>')
        elif '对方单位名称' in merged_df.columns:
            detail_columns.append('对方单位名称')
        if '对方职务_<lambda>' in merged_df.columns:
            detail_columns.append('对方职务_<lambda>')
        elif '对方职务' in merged_df.columns:
            detail_columns.append('对方职务')

        # 基准平台列
        base_platform_columns = []
        for col in [f'{base_platform}_收入总额', f'{base_platform}_支出总额', f'{base_platform}_交易次数']:
            if col in merged_df.columns:
                base_platform_columns.append(col)

        # 其他平台列（按平台名称排序）
        other_platform_columns = []
        platforms = ['银行', '微信', '支付宝']
        for platform in platforms:
            if platform != base_platform:  # 排除基准平台
                for field in ['收入总额', '支出总额', '交易次数']:
                    col_name = f'{platform}_{field}'
                    if col_name in merged_df.columns:
                        other_platform_columns.append(col_name)

        # 话单相关列
        call_columns = []
        if '通话次数' in merged_df.columns:
            call_columns.append('通话次数')
        if '通话总时长(分钟)' in merged_df.columns:
            call_columns.append('通话总时长(分钟)')
        elif '通话时长' in merged_df.columns:
            call_columns.append('通话时长')

        # 其他信息列
        info_columns = []
        if '平台' in merged_df.columns:
            info_columns.append('平台')
        if '平台金额分布' in merged_df.columns:
            info_columns.append('平台金额分布')
        if '其他账单平台金额分布' in merged_df.columns:
            info_columns.append('其他账单平台金额分布')
        if '数据来源' in merged_df.columns:
            info_columns.append('数据来源')

        # 剩余列
        used_columns = base_columns + detail_columns + base_platform_columns + other_platform_columns + call_columns + info_columns
        remaining_columns = [col for col in merged_df.columns if col not in used_columns]

        final_columns = base_columns + detail_columns + base_platform_columns + other_platform_columns + call_columns + info_columns + remaining_columns
        merged_df = merged_df[[col for col in final_columns if col in merged_df.columns]]

        return merged_df



    def _format_platform_details(self, group: pd.DataFrame) -> str:
        """格式化平台金额分布详情"""
        details = []
        for _, row in group.iterrows():
            platform = row['平台']
            income = row['收入总额']
            expense = row['支出总额']
            if income > 0 or expense > 0:
                detail = f"{platform}(收入{income:.0f}元,支出{expense:.0f}元)"
                details.append(detail)
        return '; '.join(details) if details else '无'

    def _cross_analyze_with_bill_base(self, bill_df: pd.DataFrame, call_df: pd.DataFrame) -> pd.DataFrame:
        """以账单类为基准进行交叉分析，支持跨数据源对手信息显示"""
        # 以账单数据为基础，不创建额外组合

        # 对账单类数据按对方姓名进行金额累计和去重，并计算平台分布
        bill_platform_summary = bill_df.groupby(['本方姓名', '对方姓名', '平台']).agg({
            '收入总额': 'sum',
            '支出总额': 'sum',
            '交易次数': 'sum'
        }).reset_index()

        # 计算总金额
        bill_total_summary = bill_df.groupby(['本方姓名', '对方姓名']).agg({
            '收入总额': 'sum',
            '支出总额': 'sum',
            '交易次数': 'sum',
            '平台': lambda x: '、'.join(x.unique())
        }).reset_index()

        # 计算各平台的金额分布
        platform_details = bill_platform_summary.groupby(['本方姓名', '对方姓名']).apply(
            lambda group: self._format_platform_details(group)
        ).reset_index(name='平台金额分布')

        # 为每个平台创建独立字段
        platform_individual_data = {}
        if not bill_platform_summary.empty:
            platforms = bill_platform_summary['平台'].unique()
            for platform in platforms:
                platform_data = bill_platform_summary[bill_platform_summary['平台'] == platform]
                platform_summary = platform_data.groupby(['本方姓名', '对方姓名']).agg({
                    '收入总额': 'sum',
                    '支出总额': 'sum',
                    '交易次数': 'sum'
                }).reset_index()

                # 重命名列以区分不同平台
                platform_summary = platform_summary.rename(columns={
                    '收入总额': f'{platform}_收入总额',
                    '支出总额': f'{platform}_支出总额',
                    '交易次数': f'{platform}_交易次数'
                })

                platform_individual_data[platform] = platform_summary

        # 合并总金额和平台详情
        if not bill_total_summary.empty and not platform_details.empty:
            bill_summary_with_details = pd.merge(bill_total_summary, platform_details, on=['本方姓名', '对方姓名'])
        else:
            bill_summary_with_details = bill_total_summary.copy() if not bill_total_summary.empty else pd.DataFrame()

        # 获取话单中的对方详细信息
        agg_dict = {
            '通话次数': 'sum'
        }

        # 检查通话时长列名
        if '通话总时长(分钟)' in call_df.columns:
            agg_dict['通话总时长(分钟)'] = 'sum'
        elif '通话时长' in call_df.columns:
            agg_dict['通话时长'] = 'sum'

        # 安全地添加可选字段
        if '对方号码' in call_df.columns:
            agg_dict['对方号码'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        # 检查带lambda后缀的字段名（来自话单频率分析）
        if '对方单位名称_<lambda>' in call_df.columns:
            agg_dict['对方单位名称_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        elif '对方单位名称' in call_df.columns:
            agg_dict['对方单位名称'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        if '对方职务_<lambda>' in call_df.columns:
            agg_dict['对方职务_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        elif '对方职务' in call_df.columns:
            agg_dict['对方职务'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''

        call_details = call_df.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()

        # 以账单数据为基础进行合并
        if not bill_summary_with_details.empty:
            merged_df = bill_summary_with_details.copy()
        else:
            merged_df = pd.DataFrame()

        # 与话单数据合并 - 支持跨人员匹配
        if not call_details.empty and not merged_df.empty:
            # 首先尝试完全匹配
            merged_df = pd.merge(
                merged_df,
                call_details,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

            # 对于没有匹配到的记录，尝试基于对方姓名匹配
            # 创建话单数据的对方姓名汇总
            call_agg_dict = {
                '通话次数': 'sum'
            }
            # 检查通话时长列名
            if '通话总时长(分钟)' in call_details.columns:
                call_agg_dict['通话总时长(分钟)'] = 'sum'
            elif '通话时长' in call_details.columns:
                call_agg_dict['通话时长'] = 'sum'

            call_contact_summary = call_details.groupby('对方姓名').agg(call_agg_dict).reset_index()

            # 添加单位信息字段
            if '对方单位名称_<lambda>' in call_details.columns:
                call_contact_summary = pd.merge(
                    call_contact_summary,
                    call_details.groupby('对方姓名')['对方单位名称_<lambda>'].first().reset_index(),
                    on='对方姓名'
                )
            elif '对方单位名称' in call_details.columns:
                call_contact_summary = pd.merge(
                    call_contact_summary,
                    call_details.groupby('对方姓名')['对方单位名称'].first().reset_index(),
                    on='对方姓名'
                )

            # 找出没有话单数据的账单记录
            no_call_mask = merged_df['通话次数'].isna()
            if no_call_mask.any():
                # 基于对方姓名进行跨人员匹配
                cross_match = pd.merge(
                    merged_df[no_call_mask][['本方姓名', '对方姓名']],
                    call_contact_summary,
                    on='对方姓名',
                    how='left'
                )

                # 更新没有匹配到的记录
                for idx, row in cross_match.iterrows():
                    if pd.notna(row['通话次数']):
                        mask = (merged_df['本方姓名'] == row['本方姓名']) & (merged_df['对方姓名'] == row['对方姓名'])
                        merged_df.loc[mask, '通话次数'] = row['通话次数']

                        # 更新通话时长（检查列名）
                        if '通话总时长(分钟)' in row and pd.notna(row['通话总时长(分钟)']):
                            merged_df.loc[mask, '通话总时长(分钟)'] = row['通话总时长(分钟)']
                        elif '通话时长' in row and pd.notna(row['通话时长']):
                            merged_df.loc[mask, '通话时长'] = row['通话时长']

                        # 更新单位信息
                        if '对方单位名称_<lambda>' in row and pd.notna(row['对方单位名称_<lambda>']):
                            merged_df.loc[mask, '对方单位名称_<lambda>'] = row['对方单位名称_<lambda>']
                        elif '对方单位名称' in row and pd.notna(row['对方单位名称']):
                            merged_df.loc[mask, '对方单位名称'] = row['对方单位名称']

        # 与各平台独立数据合并
        for platform, platform_data in platform_individual_data.items():
            merged_df = pd.merge(
                merged_df,
                platform_data,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

        # 填充空值
        merged_df['通话次数'] = merged_df['通话次数'].fillna(0)
        if '通话总时长(分钟)' in merged_df.columns:
            merged_df['通话总时长(分钟)'] = merged_df['通话总时长(分钟)'].fillna(0)
        elif '通话时长' in merged_df.columns:
            merged_df['通话时长'] = merged_df['通话时长'].fillna(0)

        # 填充各平台的金额字段
        for col in merged_df.columns:
            if any(platform in col for platform in ['银行', '微信', '支付宝']) and any(field in col for field in ['收入总额', '支出总额', '交易次数']):
                merged_df[col] = merged_df[col].fillna(0)

        # 安全地填充可选字段的空值
        if '对方号码' in merged_df.columns:
            merged_df['对方号码'] = merged_df['对方号码'].fillna('')
        if '对方单位名称_<lambda>' in merged_df.columns:
            merged_df['对方单位名称_<lambda>'] = merged_df['对方单位名称_<lambda>'].fillna('')
        elif '对方单位名称' in merged_df.columns:
            merged_df['对方单位名称'] = merged_df['对方单位名称'].fillna('')
        if '对方职务_<lambda>' in merged_df.columns:
            merged_df['对方职务_<lambda>'] = merged_df['对方职务_<lambda>'].fillna('')
        elif '对方职务' in merged_df.columns:
            merged_df['对方职务'] = merged_df['对方职务'].fillna('')

        # 重新排列列的顺序，将对方详细信息放在对方姓名后面
        base_columns = ['本方姓名', '对方姓名']
        detail_columns = []

        # 安全地添加存在的详细信息字段，优先使用带lambda后缀的字段
        if '对方号码' in merged_df.columns:
            detail_columns.append('对方号码')
        if '对方单位名称_<lambda>' in merged_df.columns:
            detail_columns.append('对方单位名称_<lambda>')
        elif '对方单位名称' in merged_df.columns:
            detail_columns.append('对方单位名称')
        if '对方职务_<lambda>' in merged_df.columns:
            detail_columns.append('对方职务_<lambda>')
        elif '对方职务' in merged_df.columns:
            detail_columns.append('对方职务')

        # 账单汇总列
        bill_summary_columns = []
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
                self._set_column_widths(worksheet, bank_raw_data)
                self._format_sheet(worksheet, bank_raw_data, writer.book)

        # 处理微信原始数据
        if data_models and 'wechat' in data_models and data_models['wechat']:
            wechat_raw_data = self._get_payment_raw_data(data_models['wechat'], analysis_results, '微信')
            if not wechat_raw_data.empty:
                wechat_raw_data.to_excel(writer, sheet_name='微信分析原始', index=False)
                worksheet = writer.sheets['微信分析原始']
                self._set_column_widths(worksheet, wechat_raw_data)
                self._format_sheet(worksheet, wechat_raw_data, writer.book)

        # 处理支付宝原始数据
        if data_models and 'alipay' in data_models and data_models['alipay']:
            alipay_raw_data = self._get_payment_raw_data(data_models['alipay'], analysis_results, '支付宝')
            if not alipay_raw_data.empty:
                alipay_raw_data.to_excel(writer, sheet_name='支付宝分析原始', index=False)
                worksheet = writer.sheets['支付宝分析原始']
                self._set_column_widths(worksheet, alipay_raw_data)
                self._format_sheet(worksheet, alipay_raw_data, writer.book)

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
            from src.utils.key_transactions import KeyTransactionEngine

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
            self._set_column_widths(worksheet, transfer_data)
            self._format_sheet(worksheet, transfer_data, writer.book)

        if not deposit_data.empty:
            deposit_data.to_excel(writer, sheet_name='存现数据(原始)', index=False)
            worksheet = writer.sheets['存现数据(原始)']
            self._set_column_widths(worksheet, deposit_data)
            self._format_sheet(worksheet, deposit_data, writer.book)
        
        if not withdrawal_data.empty:
            withdrawal_data.to_excel(writer, sheet_name='取现数据(原始)', index=False)
            worksheet = writer.sheets['取现数据(原始)']
            self._set_column_widths(worksheet, withdrawal_data)
            self._format_sheet(worksheet, withdrawal_data, writer.book)

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
            from src.utils.key_transactions import KeyTransactionEngine

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
                self._set_column_widths(worksheet, key_transactions)
                self._format_sheet(worksheet, key_transactions, writer.book)

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
                    self._set_column_widths(worksheet, key_stats)
                    self._format_sheet(worksheet, key_stats, writer.book)

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
        # 性能优化：缓存格式对象，避免重复创建
        if not hasattr(self, '_format_cache'):
            self._format_cache = {}
        
        # 创建格式（使用缓存）
        if 'header_format' not in self._format_cache:
            self._format_cache['header_format'] = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'bg_color': '#D9E1F2',
                'border': 1
            })

        # 不同类型的数值格式（使用缓存）
        if 'money_format' not in self._format_cache:
            self._format_cache['money_format'] = workbook.add_format({'num_format': '#,##0.00'})
        if 'integer_format' not in self._format_cache:
            self._format_cache['integer_format'] = workbook.add_format({'num_format': '0'})
        if 'phone_format' not in self._format_cache:
            self._format_cache['phone_format'] = workbook.add_format({'num_format': '@'})
        if 'date_format' not in self._format_cache:
            self._format_cache['date_format'] = workbook.add_format({'num_format': 'yyyy-mm-dd'})

        # 性能优化：批量应用表头格式
        header_format = self._format_cache['header_format']
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # 获取格式对象
        money_format = self._format_cache['money_format']
        integer_format = self._format_cache['integer_format']
        phone_format = self._format_cache['phone_format']
        date_format = self._format_cache['date_format']

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

    def _generate_fund_tracking_sheet(self, data_models: Dict) -> pd.DataFrame:
        """
        生成大额资金追踪表（包含大额资金追踪和存取现与话单匹配）
        
        Args:
            data_models: 数据模型字典，格式为 {'bank': bank_model, 'wechat': wechat_model, ...}
            
        Returns:
            pd.DataFrame: 大额资金追踪结果
        """
        try:
            # 获取大额资金追踪结果
            fund_tracking_results = self._get_fund_tracking_results(data_models)
            
            # 获取存取现与话单匹配结果
            cash_call_results = self._analyze_cash_call_matching(data_models)
            
            # 合并结果
            all_results = []
            
            if not fund_tracking_results.empty:
                # 添加分析类型字段
                fund_tracking_results['分析类型'] = '大额资金追踪'
                all_results.append(fund_tracking_results)
            
            if not cash_call_results.empty:
                # 确保cash_call_results有所有必要的列
                if '交易类型' not in cash_call_results.columns:
                    cash_call_results['交易类型'] = ''
                all_results.append(cash_call_results)
            
            if not all_results:
                self.logger.info("没有找到任何追踪结果")
                return pd.DataFrame()
            
            # 合并所有结果
            combined_df = pd.concat(all_results, ignore_index=True)
            
            # 重新排列列的顺序，将分析类型放在第一列
            column_order = ['分析类型'] + [col for col in combined_df.columns if col != '分析类型']
            combined_df = combined_df[column_order]
            
            # 按交易日期排序
            if '交易日期' in combined_df.columns:
                # 指定日期格式避免警告，支持常见的中文日期格式
                combined_df['交易日期'] = pd.to_datetime(combined_df['交易日期'], 
                                                       format='mixed', 
                                                       errors='coerce')
                combined_df = combined_df.sort_values('交易日期', ascending=False)
            
            self.logger.info(f"生成追踪表成功，共{len(combined_df)}条记录")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"生成大额资金追踪表时出错: {e}")
            return pd.DataFrame()
    
    def _get_fund_tracking_results(self, data_models: Dict) -> pd.DataFrame:
        """
        获取大额资金追踪结果（性能优化版本）
        
        Args:
            data_models: 数据模型字典
            
        Returns:
            pd.DataFrame: 大额资金追踪结果
        """
        try:
            # 导入大额资金追踪引擎
            from ..utils.fund_tracking import FundTrackingEngine
            
            # 初始化追踪引擎
            tracking_engine = FundTrackingEngine()
            
            # 执行大额资金追踪
            tracking_results = tracking_engine.track_large_funds(data_models)
            
            if tracking_results.empty:
                self.logger.info("未发现大额资金交易")
                return pd.DataFrame()
            
            # 性能优化：批量处理所有数据，避免逐行操作
            if not tracking_results.empty:
                # 批量获取话单匹配信息（优化版本）
                call_matches = self._get_call_record_match_batch_optimized(
                    tracking_results['核心人员'].tolist(),
                    tracking_results['交易日期'].tolist(),
                    data_models
                )
                
                # 性能优化：使用向量化操作构建DataFrame
                formatted_df = tracking_results.copy()
                
                # 批量生成追踪ID
                formatted_df['追踪ID'] = [f"TRK{i:04d}" for i in range(len(tracking_results))]
                
                # 性能优化：使用map替代apply进行交易类型转换
                # 注意：这里只转换交易类型，不覆盖数据来源字段中的具体银行名称
                transaction_type_map = {
                    '银行': '银行',
                    '微信': '微信', 
                    '支付宝': '支付宝'
                }
                formatted_df['交易类型'] = formatted_df['数据来源'].map(
                    lambda x: transaction_type_map.get(x, x) if any(keyword in x for keyword in ['银行', '微信', '支付宝']) else x
                )
                
                # 确保数据来源字段保持原样（包含具体的银行名称）
                # 不进行任何转换，直接使用fund_tracking.py中设置的银行名称
                
                formatted_df['话单匹配'] = call_matches
                
                # 性能优化：批量重命名列
                column_mapping = {
                    'core_person': '核心人员',
                    '关联人员': '对方人员',
                    '追踪层级': '追踪深度',
                    '追踪说明': '备注'
                }
                
                # 只重命名存在的列
                for old_col, new_col in column_mapping.items():
                    if old_col in formatted_df.columns:
                        formatted_df[new_col] = formatted_df[old_col]
                
                # 性能优化：批量确保所有必需的列都存在
                required_columns = ['追踪ID', '核心人员', '交易日期', '交易金额', '交易类型', 
                                 '交易方向', '对方人员', '数据来源', '大额级别', '追踪深度', '备注', '话单匹配']
                
                for col in required_columns:
                    if col not in formatted_df.columns:
                        formatted_df[col] = ''
                
                return formatted_df[required_columns]
            
            return pd.DataFrame()
            
        except ImportError:
            self.logger.warning("大额资金追踪模块未找到，跳过大额资金追踪")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"获取大额资金追踪结果时出错: {e}")
            return pd.DataFrame()
    
    def _get_transaction_type(self, data_source: str) -> str:
        """
        根据数据源获取交易类型
        
        Args:
            data_source: 数据源名称
            
        Returns:
            str: 交易类型（银行、微信、支付宝等）
        """
        if '银行' in data_source:
            return '银行'
        elif '微信' in data_source:
            return '微信'
        elif '支付宝' in data_source:
            return '支付宝'
        else:
            return data_source
    
    def _get_call_record_match_batch_optimized(self, person_names: List[str], transaction_dates: List[str], data_models: Dict) -> List[str]:
        """
        批量获取话单匹配信息（高性能优化版本）
        
        Args:
            person_names: 人员姓名列表
            transaction_dates: 交易日期列表
            data_models: 数据模型字典
            
        Returns:
            List[str]: 话单匹配信息列表
        """
        if not person_names or not transaction_dates:
            return [''] * len(person_names)
        
        # 检查是否有话单数据模型
        if 'call' not in data_models or data_models['call'] is None:
            return [''] * len(person_names)
        
        call_model = data_models['call']
        if call_model.data.empty:
            return [''] * len(person_names)
        
        try:
            # 性能优化：避免不必要的DataFrame复制，直接使用原始数据
            call_data = call_model.data
            date_column = call_model.date_column if hasattr(call_model, 'date_column') else '呼叫日期'
            
            # 性能优化：只对需要的列进行类型转换，避免全量复制
            if date_column not in call_data.columns:
                return [''] * len(person_names)
            
            # 性能优化：使用向量化操作一次性转换所有日期
            call_data_processed = call_data[[date_column, '本方姓名', '对方姓名']].copy()
            call_data_processed['temp_date'] = pd.to_datetime(
                call_data_processed[date_column].astype(str), 
                errors='coerce', 
                format='mixed'
            )
            
            # 性能优化：只处理有效日期的数据
            valid_mask = pd.notna(call_data_processed['temp_date'])
            if not valid_mask.any():
                return [''] * len(person_names)
            
            call_data_valid = call_data_processed[valid_mask].copy()
            call_data_valid['date_key'] = call_data_valid['temp_date'].dt.date
            
            # 性能优化：使用更高效的groupby操作，避免循环
            date_groups = call_data_valid.groupby('date_key')
            
            # 预转换交易日期，使用向量化操作
            tx_dates_series = pd.Series(transaction_dates)
            tx_dates_converted = pd.to_datetime(tx_dates_series, errors='coerce', format='mixed')
            
            # 性能优化：批量处理，减少循环开销
            results = []
            for i, (person_name, tx_date) in enumerate(zip(person_names, tx_dates_converted)):
                if pd.isna(tx_date):
                    results.append('')
                    continue
                    
                date_key = tx_date.date()
                if date_key not in date_groups.groups:
                    results.append('')
                    continue
                
                # 性能优化：直接使用groupby结果，避免额外的索引操作
                same_day_calls = date_groups.get_group(date_key)
                
                if same_day_calls.empty:
                    results.append('')
                    continue
                
                # 性能优化：使用向量化操作筛选人员记录
                person_name_str = str(person_name)
                caller_mask = same_day_calls['本方姓名'].astype(str) == person_name_str
                callee_mask = same_day_calls['对方姓名'].astype(str) == person_name_str
                combined_mask = caller_mask | callee_mask
                
                if not combined_mask.any():
                    results.append('')
                    continue
                
                person_calls = same_day_calls[combined_mask]
                
                # 性能优化：使用集合操作收集联系人，避免多次DataFrame操作
                contacted_persons = set()
                
                # 本方是目标人员的情况
                caller_records = person_calls[caller_mask[combined_mask]]
                if not caller_records.empty:
                    contacted_persons.update(caller_records['对方姓名'].astype(str).unique())
                
                # 对方是目标人员的情况
                callee_records = person_calls[callee_mask[combined_mask]]
                if not callee_records.empty:
                    contacted_persons.update(callee_records['本方姓名'].astype(str).unique())
                
                # 移除空值和本人
                contacted_persons = {p for p in contacted_persons if p and p != person_name_str}
                
                if contacted_persons:
                    contacted_persons_str = sorted(contacted_persons)
                    results.append(f"本方{person_name}，对方{','.join(contacted_persons_str)}")
                else:
                    results.append('')
            
            return results
            
        except Exception as e:
            self.logger.warning(f"批量获取话单匹配信息时出错: {e}")
            return [''] * len(person_names)
    
    def _get_call_record_match_batch(self, person_names: List[str], transaction_dates: List[str], data_models: Dict) -> List[str]:
        """
        批量获取话单匹配信息（性能优化版本）
        
        Args:
            person_names: 人员姓名列表
            transaction_dates: 交易日期列表
            data_models: 数据模型字典
            
        Returns:
            List[str]: 话单匹配信息列表
        """
        if not person_names or not transaction_dates:
            return [''] * len(person_names)
        
        # 检查是否有话单数据模型
        if 'call' not in data_models or data_models['call'] is None:
            return [''] * len(person_names)
        
        call_model = data_models['call']
        if call_model.data.empty:
            return [''] * len(person_names)
        
        try:
            # 预处理话单数据，只处理一次
            call_data = call_model.data.copy()
            date_column = call_model.date_column if hasattr(call_model, 'date_column') else '呼叫日期'
            
            # 优化：一次性转换日期列
            if date_column in call_data.columns:
                call_data[date_column] = call_data[date_column].astype(str)
            
            call_data['temp_date'] = pd.to_datetime(call_data.get(date_column, ''), errors='coerce')
            
            # 创建日期索引，加速日期查找
            date_groups = {}
            for idx, row in call_data.iterrows():
                if pd.notna(row['temp_date']):
                    date_key = row['temp_date'].date()
                    if date_key not in date_groups:
                        date_groups[date_key] = []
                    date_groups[date_key].append(idx)
            
            # 批量处理所有记录
            results = []
            for person_name, transaction_date in zip(person_names, transaction_dates):
                results.append(self._get_single_call_record_match(
                    person_name, transaction_date, call_data, date_groups
                ))
            
            return results
            
        except Exception as e:
            self.logger.warning(f"批量获取话单匹配信息时出错: {e}")
            return [''] * len(person_names)
    
    def _get_single_call_record_match(self, person_name: str, transaction_date: str, 
                                     call_data: pd.DataFrame, date_groups: Dict) -> str:
        """
        获取单条记录的话单匹配信息（优化版本）
        
        Args:
            person_name: 人员姓名
            transaction_date: 交易日期
            call_data: 预处理的话单数据
            date_groups: 日期索引字典
            
        Returns:
            str: 话单匹配信息
        """
        try:
            # 转换交易日期
            if not isinstance(transaction_date, str):
                transaction_date = str(transaction_date)
            tx_date = pd.to_datetime(transaction_date, errors='coerce')
            if pd.isna(tx_date):
                return ''
            
            # 使用日期索引快速查找当天记录
            date_key = tx_date.date()
            if date_key not in date_groups:
                return ''
            
            # 获取当天记录的索引
            same_day_indices = date_groups[date_key]
            same_day_calls = call_data.loc[same_day_indices]
            
            if same_day_calls.empty:
                return ''
            
            # 性能优化：使用向量化操作筛选人员记录
            person_name_str = str(person_name)
            caller_mask = same_day_calls['本方姓名'].astype(str) == person_name_str
            callee_mask = same_day_calls['对方姓名'].astype(str) == person_name_str
            # 修复Boolean Series reindexed警告：确保布尔索引与DataFrame索引对齐
            combined_mask = caller_mask | callee_mask
            # 使用.reset_index(drop=True)确保索引对齐
            person_calls = same_day_calls.loc[combined_mask.values] if len(combined_mask) == len(same_day_calls) else same_day_calls[combined_mask]
            
            if person_calls.empty:
                return ''
            
            # 优化：使用向量化操作收集联系人
            contacted_persons = set()
            
            # 本方是目标人员的情况
            caller_records = person_calls[caller_mask]
            if not caller_records.empty:
                contacted_persons.update(caller_records['对方姓名'].astype(str).unique())
            
            # 对方是目标人员的情况
            callee_records = person_calls[callee_mask]
            if not callee_records.empty:
                contacted_persons.update(callee_records['本方姓名'].astype(str).unique())
            
            # 移除空值和本人
            contacted_persons = {p for p in contacted_persons if p and p != person_name_str}
            
            if contacted_persons:
                contacted_persons_str = sorted(contacted_persons)
                return f"本方{person_name}，对方{','.join(contacted_persons_str)}"
            else:
                return ''
                
        except Exception as e:
            self.logger.warning(f"获取单条话单匹配信息时出错: {e}")
            return ''
    
    def _get_call_record_match(self, person_name: str, transaction_date: str, data_models: Dict) -> str:
        """
        获取话单匹配信息（单条记录版本，兼容旧代码）
        
        Args:
            person_name: 人员姓名
            transaction_date: 交易日期
            data_models: 数据模型字典
            
        Returns:
            str: 话单匹配信息，格式为"本方XX，对方XX,XX,XX"
        """
        # 使用批量处理方法的简化版本
        return self._get_call_record_match_batch([person_name], [transaction_date], data_models)[0]
    
    def _analyze_cash_call_matching(self, data_models: Dict, min_amount: float = 10000) -> pd.DataFrame:
        """
        分析存取现与话单匹配（性能优化版本）
        
        Args:
            data_models: 数据模型字典
            min_amount: 最小金额阈值（默认1万）
            
        Returns:
            pd.DataFrame: 存取现与话单匹配结果
        """
        try:
            # 检查是否有银行数据模型
            if 'bank' not in data_models or data_models['bank'] is None:
                self.logger.warning("没有银行数据模型，跳过存取现与话单匹配分析")
                return pd.DataFrame()
            
            bank_model = data_models['bank']
            if bank_model.data.empty:
                self.logger.warning("银行数据为空，跳过存取现与话单匹配分析")
                return pd.DataFrame()
            
            # 检查是否有话单数据模型（键名为'call'）
            if 'call' not in data_models or data_models['call'] is None:
                self.logger.warning("没有话单数据模型，跳过存取现与话单匹配分析")
                return pd.DataFrame()
            
            call_model = data_models['call']
            if call_model.data.empty:
                self.logger.warning("话单数据为空，跳过存取现与话单匹配分析")
                return pd.DataFrame()
            
            # 性能优化：筛选存取现交易（金额大于等于阈值）
            cash_data = bank_model.data.copy()
            
            # 确保有存取现标识列
            if '存取现标识' not in cash_data.columns:
                self.logger.warning("银行数据中没有存取现标识列，跳过存取现与话单匹配分析")
                return pd.DataFrame()
            
            # 性能优化：使用向量化操作筛选存取现交易
            cash_mask = (cash_data['存取现标识'].isin(['存现', '取现'])) & \
                       (cash_data[bank_model.amount_column].abs() >= min_amount)
            cash_transactions = cash_data[cash_mask]
            
            if cash_transactions.empty:
                self.logger.info(f"没有找到金额大于等于{min_amount}元的存取现交易")
                return pd.DataFrame()
            
            self.logger.info(f"找到{len(cash_transactions)}笔金额大于等于{min_amount}元的存取现交易")
            
            # 性能优化：批量处理话单匹配信息
            person_names = []
            transaction_dates = []
            
            for idx, transaction in cash_transactions.iterrows():
                # 获取交易信息
                person_name = transaction.get(bank_model.name_column, '')
                if not person_name:
                    # 如果本方姓名为空，尝试使用对方姓名
                    person_name = transaction.get(bank_model.opposite_name_column, '')
                
                transaction_date = transaction.get('交易日期', '')
                person_names.append(person_name)
                transaction_dates.append(transaction_date)
            
            # 性能优化：批量获取话单匹配信息
            call_matches = self._get_call_record_match_batch_optimized(person_names, transaction_dates, data_models)
            
            # 性能优化：批量构建结果
            results = []
            for i, (idx, transaction) in enumerate(cash_transactions.iterrows()):
                person_name = person_names[i]
                transaction_date = transaction_dates[i]
                amount = transaction.get(bank_model.amount_column, 0)
                cash_type = transaction.get('存取现标识', '')
                summary = transaction.get(bank_model.summary_column, '')
                remark = transaction.get(bank_model.remark_column, '')
                
                # 构建结果记录
                # 优先使用银行名称字段，如果不存在则使用数据来源字段
                bank_name = transaction.get('银行类型', transaction.get('银行名称', transaction.get('交易机构名称', '')))
                if not bank_name or bank_name == '':
                    bank_name = transaction.get('数据来源', '')
                
                result_record = {
                    '分析类型': '存取现与话单匹配',
                    '追踪ID': f"CASH_{idx}",
                    '核心人员': person_name,
                    '交易日期': transaction_date,
                    '交易金额': amount,
                    '交易方向': '收入' if cash_type == '存现' else '支出',
                    '交易类型': cash_type,
                    '对方人员': '',  # 存取现通常没有对方人员
                    '数据来源': bank_name,  # 使用具体的银行名称而不是文件名称
                    '大额级别': '',  # 不适用
                    '追踪深度': '',  # 不适用
                    '备注': f"{summary} {remark}".strip(),
                    '话单匹配': call_matches[i]
                }
                
                results.append(result_record)
            
            # 转换为DataFrame
            if results:
                return pd.DataFrame(results)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"分析存取现与话单匹配时出错: {e}")
            return pd.DataFrame()
    
    # Obsolete functions removed.
    # The methods get_output_path, export_all_to_excel, export_raw_data,
    # export_cash_operations, and export_bank_transactions were here.