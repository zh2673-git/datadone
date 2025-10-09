"""
Excel格式化工具模块

提供Excel表格格式化的相关功能，包括列宽设置、单元格格式、条件格式等。
"""

import pandas as pd
from typing import Dict, Any


class ExcelFormatter:
    """Excel格式化工具类"""
    
    def __init__(self, excel_config: Dict[str, Any] = None):
        """
        初始化Excel格式化工具
        
        Parameters:
        -----------
        excel_config : Dict[str, Any], optional
            Excel配置参数，默认包含默认列宽等设置
        """
        self.excel_config = excel_config or {
            'default_width': 15,
            'conditional_formatting': True,
            'auto_filter': True,
            'freeze_panes': True
        }
    
    def set_column_widths(self, worksheet, df: pd.DataFrame) -> None:
        """
        根据列内容设置Excel列宽
        
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
    
    def format_sheet(self, worksheet, df: pd.DataFrame, workbook) -> None:
        """
        为工作表添加格式，如表头格式、数值格式等
        
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
            self.add_conditional_formatting(worksheet, df, workbook)
        
        # 添加自动筛选
        if self.excel_config.get('auto_filter', True):
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
            
        # 冻结窗格（冻结表头）
        if self.excel_config.get('freeze_panes', True):
            worksheet.freeze_panes(1, 0)  # 冻结第一行
    
    def add_conditional_formatting(self, worksheet, df: pd.DataFrame, workbook) -> None:
        """
        为工作表添加条件格式，例如给金额列添加色阶
        
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