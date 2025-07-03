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
        
        try:
            with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                # 获取话单数据中的对方单位信息
                company_position_map = {}
                if include_unit_info and data_models and 'call' in data_models and data_models['call']:
                    company_position_map = self._extract_company_info(data_models['call'].data)

                # 1. 写入所有基础分析结果
                for result_name, df in analysis_results.items():
                    if not self.validate_data(df):
                        continue

                    # 若需要且可能，添加对方单位信息
                    if include_unit_info and '对方姓名' in df.columns:
                        df = self._add_company_info(df, company_position_map)

                    clean_sheet_name = self._sanitize_sheet_name(result_name)
                    df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                    worksheet = writer.sheets[clean_sheet_name]
                    self._set_column_widths(worksheet, df)
                    self._format_sheet(worksheet, df, writer.book)
                
                self.logger.info(f"已将 {len(analysis_results)} 个分析结果写入 sheets")

                # 2. 写入特定数据源的汇总和原始数据
                if add_summaries and data_models and 'bank' in data_models and data_models['bank']:
                    self.logger.info("正在为银行数据添加额外的汇总表...")
                    bank_model = data_models['bank']
                    self.add_summary_sheets(writer, bank_model)

                    # 3. 若需要，添加原始数据表
                    if add_raw_data:
                        self.export_raw_bank_data(writer, bank_model)

                    # 4. 添加重点收支数据表
                    self.export_key_transactions(writer, bank_model)

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
        添加存取现汇总和转账汇总总表。

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
        
        if not cash_summary.empty:
            cash_summary.to_excel(writer, sheet_name='存取现汇总', index=False)
            worksheet = writer.sheets['存取现汇总']
            self._set_column_widths(worksheet, cash_summary)
            self._format_sheet(worksheet, cash_summary, writer.book)

        # 转账汇总
        transfer_summary = full_data[full_data['存取现标识'] == '转账'].groupby(group_keys).agg(
            转入金额=('收入金额', 'sum'),
            转出金额=('支出金额', 'sum')
        ).reset_index()
        transfer_summary = transfer_summary[(transfer_summary['转入金额'] > 0) | (transfer_summary['转出金额'] > 0)]

        if not transfer_summary.empty:
            transfer_summary.to_excel(writer, sheet_name='转账汇总', index=False)
            worksheet = writer.sheets['转账汇总']
            self._set_column_widths(worksheet, transfer_summary)
            self._format_sheet(worksheet, transfer_summary, writer.book)

        self.logger.info("已添加汇总总表")

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

    def export_key_transactions(self, writer: pd.ExcelWriter, bank_model: 'BankDataModel'):
        """
        导出重点收支数据到不同的sheet

        Parameters:
        -----------
        writer : pd.ExcelWriter
            Excel写入器对象
        bank_model : BankDataModel
            包含银行数据的模型
        """
        if not bank_model or bank_model.data.empty:
            return

        try:
            # 导入重点收支识别引擎
            from src.utils.key_transactions import KeyTransactionEngine

            # 初始化重点收支识别引擎
            key_engine = KeyTransactionEngine(self.config)

            if not key_engine.enabled:
                self.logger.info("重点收支识别功能已禁用，跳过重点收支数据导出")
                return

            # 识别重点收支
            key_data = key_engine.identify_key_transactions(
                bank_model.data,
                bank_model.summary_column,
                bank_model.remark_column,
                bank_model.type_column,
                bank_model.amount_column,
                bank_model.opposite_name_column
            )

            # 筛选出重点收支数据
            key_transactions = key_data[key_data['是否重点收支']].copy()

            if not key_transactions.empty:
                # 导出重点收支原始数据
                key_transactions.to_excel(writer, sheet_name='重点收支(原始)', index=False)
                worksheet = writer.sheets['重点收支(原始)']
                self._set_column_widths(worksheet, key_transactions)
                self._format_sheet(worksheet, key_transactions, writer.book)

                # 生成重点收支统计数据
                key_stats = key_engine.generate_statistics(
                    key_data,
                    bank_model.name_column,
                    bank_model.amount_column,
                    bank_model.date_column,
                    bank_model.opposite_name_column
                )

                if not key_stats.empty:
                    # 导出重点收支统计数据
                    key_stats.to_excel(writer, sheet_name='重点收支(统计)', index=False)
                    worksheet = writer.sheets['重点收支(统计)']
                    self._set_column_widths(worksheet, key_stats)
                    self._format_sheet(worksheet, key_stats, writer.book)

                self.logger.info(f"已添加重点收支数据表，原始数据 {len(key_transactions)} 笔，统计数据 {len(key_stats)} 人")
            else:
                self.logger.info("未发现重点收支数据，跳过重点收支数据导出")

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