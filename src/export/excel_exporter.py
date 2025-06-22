#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import logging
from typing import Dict, List, Optional, Union
import xlsxwriter
from datetime import datetime
import numpy as np

from src.base import BaseDataModel
from src.datasource import BankDataModel

class ExcelExporter:
    """
    Excel导出器，用于将分析结果导出为Excel文件
    """
    def __init__(self, output_dir: str = 'output'):
        """
        初始化Excel导出器
        
        Parameters:
        -----------
        output_dir : str, optional
            输出目录
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir
        
        # 创建输出目录
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def export(self, analysis_results: Dict[str, pd.DataFrame], data_models: Dict[str, BaseDataModel], filename: Optional[str] = None) -> Optional[str]:
        """
        将所有分析结果导出到单个Excel文件中，每个结果一个sheet。
        
        Parameters:
        -----------
        analysis_results : Dict[str, pd.DataFrame]
            分析结果字典，键为 "分析类型"，值为分析结果的DataFrame。
        data_models : Dict[str, BaseDataModel]
            包含原始数据的数据模型字典。
        filename : str, optional
            输出的Excel文件名。如果未提供，将自动生成一个带时间戳的文件名。
            
        Returns:
        --------
        str or None
            成功导出的文件路径，如果失败则返回None。
        """
        if not analysis_results:
            self.logger.warning("没有分析结果可供导出。")
            return None
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"分析结果_{timestamp}.xlsx"
    
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                # 获取话单数据中的对方单位信息
                company_position_map = {}
                if 'call' in data_models and data_models['call'] and not data_models['call'].data.empty:
                    call_data = data_models['call'].data
                    if '对方单位名称' in call_data.columns:
                        # 创建一个映射，用对方姓名作为键
                        company_map = call_data.groupby('对方姓名')['对方单位名称'].agg(
                            lambda x: '|'.join(sorted(set(x.dropna().astype(str))))
                        ).to_dict()
                        company_position_map.update(company_map)

                # 1. 写入所有基础分析结果
                for result_name, df in analysis_results.items():
                    if df.empty:
                        continue

                    # 如果结果包含对方姓名列，添加对方单位列
                    if '对方姓名' in df.columns:
                        if '对方单位' not in df.columns:
                            df['对方单位'] = df['对方姓名'].map(company_position_map)
                            # 调整列顺序，将对方单位放在对方姓名后面
                            cols = list(df.columns)
                            name_idx = cols.index('对方姓名')
                            cols.remove('对方单位')
                            cols.insert(name_idx + 1, '对方单位')
                            df = df[cols]

                    clean_sheet_name = self._clean_sheet_name(result_name)
                    df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                    worksheet = writer.sheets[clean_sheet_name]
                    self._set_column_widths(worksheet, df)
                    self._format_sheet(worksheet, df, writer.book)
                
                self.logger.info(f"已将 {len(analysis_results)} 个分析结果写入 sheets.")

                # 2. 写入特定数据源的汇总和原始数据
                if 'bank' in data_models and data_models['bank'] and not data_models['bank'].data.empty:
                    self.logger.info("正在为银行数据添加额外的汇总和原始数据表...")
                    bank_model = data_models['bank']
                    self.add_summary_sheets(writer, bank_model)
                    self.export_raw_bank_data(writer, bank_model)

            self.logger.info(f"所有分析结果已成功导出到: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"导出到Excel文件 '{filepath}' 时出错: {e}", exc_info=True)
            return None

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

        self.logger.info("已添加汇总总表。")

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

        self.logger.info("已将原始银行交易数据导出到单独的sheets。")

    def _clean_sheet_name(self, name: str) -> str:
        """
        清理工作表名，确保符合Excel的要求
        
        Parameters:
        -----------
        name : str
            原始工作表名
            
        Returns:
        --------
        str
            清理后的工作表名
        """
        # 移除非法字符
        illegal_chars = [':', '/', '\\', '?', '*', '[', ']']
        for char in illegal_chars:
            name = name.replace(char, '_')
        
        # 限制长度为31个字符
        if len(name) > 31:
            name = name[:31]
        
        return name
    
    def _set_column_widths(self, worksheet, df: pd.DataFrame):
        """
        设置Excel列宽
        
        Parameters:
        -----------
        worksheet : xlsxwriter.worksheet.Worksheet
            工作表对象
        df : pd.DataFrame
            数据
        """
        for i, col in enumerate(df.columns):
            # 计算列宽
            max_len = max(
                df[col].astype(str).map(len).max(),  # 数据中的最大长度
                len(str(col))  # 列名的长度
            ) + 2  # 额外的空间
            
            # 限制最大列宽
            col_width = min(max_len, 50)
            
            # 设置列宽
            worksheet.set_column(i, i, col_width)
    
    def _format_sheet(self, worksheet, df: pd.DataFrame, workbook):
        """
        格式化工作表
        
        Parameters:
        -----------
        worksheet : xlsxwriter.worksheet.Worksheet
            工作表对象
        df : pd.DataFrame
            数据
        workbook : xlsxwriter.Workbook
            工作簿对象
        """
        # 添加表头格式
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D8E4BC',
            'border': 1
        })
        
        # 应用表头格式
        for i, value in enumerate(df.columns.values):
            worksheet.write(0, i, value, header_format)
            
        # 添加条件格式
        self._add_conditional_formatting(worksheet, df, workbook)
        
        # 冻结首行
        worksheet.freeze_panes(1, 0)
    
    def _add_conditional_formatting(self, worksheet, df: pd.DataFrame, workbook):
        """
        添加条件格式
        
        Parameters:
        -----------
        worksheet : xlsxwriter.worksheet.Worksheet
            工作表对象
        df : pd.DataFrame
            数据
        workbook : xlsxwriter.Workbook
            工作簿对象
        """
        # 定义金额相关列的关键词
        amount_keywords = ['金额', '总额', '收入', '支出']
        
        # 定义时间相关列的关键词
        time_keywords = ['时间跨度', '天数', '次数']
        
        # 遍历所有列
        for i, col in enumerate(df.columns):
            # 检查是否是金额列
            is_amount_col = any(keyword in col for keyword in amount_keywords)
            
            # 检查是否是时间跨度列
            is_time_col = any(keyword in col for keyword in time_keywords)
            
            # 获取列的范围（从第2行开始，跳过表头）
            col_range = f'{chr(65 + i)}2:{chr(65 + i)}{len(df) + 1}'
            
            if is_amount_col:
                # 为金额列添加条件格式
                worksheet.conditional_format(col_range, {
                    'type': '3_color_scale',
                    'min_color': '#FFFFFF',
                    'mid_color': '#FFEB9C',
                    'max_color': '#F8CBAD'
                })
            
            elif is_time_col:
                # 为时间跨度列添加条件格式
                worksheet.conditional_format(col_range, {
                    'type': '3_color_scale',
                    'min_color': '#FFFFFF',
                    'mid_color': '#DDEBF7',
                    'max_color': '#9BC2E6'
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