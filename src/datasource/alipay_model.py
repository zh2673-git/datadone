#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import os

from src.base import BaseDataModel
from src.utils.config import Config

class AlipayDataModel(BaseDataModel):
    """
    支付宝数据模型，用于加载和处理支付宝交易数据
    """
    def __init__(self, data_path=None, data=None, config=None):
        """
        初始化支付宝数据模型
        
        Parameters:
        -----------
        data_path : str, optional
            数据文件路径，如果提供则从文件加载数据
        data : pd.DataFrame, optional
            直接提供的数据，如果提供则使用此数据
        config : Config, optional
            配置对象，如果不提供则使用默认配置
        """
        self.config = config or Config()
        
        # 定义列名配置
        self.name_column = self.config.get('data_sources.alipay.name_column', '本方姓名')
        self.date_column = self.config.get('data_sources.alipay.date_column', '交易日期')
        self.time_column = self.config.get('data_sources.alipay.time_column', '交易时间')
        self.amount_column = self.config.get('data_sources.alipay.amount_column', '交易金额')
        self.type_column = self.config.get('data_sources.alipay.type_column', '交易型')
        self.direction_column = self.config.get('data_sources.alipay.direction_column', '借贷标识')
        self.opposite_name_column = self.config.get('data_sources.alipay.opposite_name_column', '对方姓名')
        self.special_date_column = self.config.get('data_sources.alipay.special_date_column', '特殊日期名称')
        self.credit_flag = self.config.get('data_sources.alipay.credit_flag', '收入')  # 收入
        self.debit_flag = self.config.get('data_sources.alipay.debit_flag', '支出')    # 支出
        self.transaction_type_column = self.config.get('data_sources.alipay.transaction_type_column', '交易类型')
        self.remark_column = self.config.get('data_sources.alipay.remark_column', '交易备注')
        self.transaction_status_column = self.config.get('data_sources.alipay.transaction_status_column', '交易状态')
        
        # 定义必需的列
        self.required_columns = [
            self.name_column,
            self.date_column,
            self.amount_column
        ]
        
        # 调用父类初始化
        super().__init__(data_path, data)
    
    def preprocess(self):
        """
        数据预处理
        
        根据示例数据:
        - 交易日期格式为 "YYYY/MM/DD HH:MM:SS"
        - 交易金额格式为 "0.05" 或 "-25.00"，带有空格
        - 借贷标识为 "收入" 或 "支出"
        """
        # 确保日期列为日期类型
        if self.date_column in self.data.columns:
            # 示例数据中日期格式为 "YYYY/MM/DD HH:MM:SS"
            self.data[self.date_column] = pd.to_datetime(self.data[self.date_column], errors='coerce')
        
        # 处理交易金额列
        if self.amount_column in self.data.columns:
            # 示例数据中金额可能带有空格，如 "0.05 " 或 "-25.00 "
            # 先去除空格，然后转换为数值
            self.data[self.amount_column] = self.data[self.amount_column].astype(str).str.strip()
            self.data[self.amount_column] = pd.to_numeric(self.data[self.amount_column], errors='coerce')
        
        # 添加收入和支出列
        self.add_income_expense_columns()
        
        # 添加数据来源列
        if self.file_path and '数据来源' not in self.data.columns:
            source_name = os.path.splitext(os.path.basename(self.file_path))[0]
            self.data['数据来源'] = source_name
            self.logger.info(f"已添加 '数据来源' 列，值为 '{source_name}'")
        elif '数据来源' not in self.data.columns:
            self.data['数据来源'] = '支付宝数据' # 默认值
            self.logger.info("未找到文件路径，添加 '数据来源' 列，值为 '支付宝数据'")
        
        self.logger.info("支付宝数据预处理完成")
    
    def add_income_expense_columns(self):
        """
        添加收入和支出列
        
        根据示例数据:
        - 借贷标识为 "收入" 或 "支出"
        - 收入的交易金额为正数，如 "0.05"
        - 支出的交易金额为负数，如 "-25.00"
        """
        # 初始化收入和支出列
        self.data['收入金额'] = 0.0
        self.data['支出金额'] = 0.0
        
        # 根据借贷标识和交易金额填充收入和支出金额
        if self.direction_column in self.data.columns and self.amount_column in self.data.columns:
            # 收入条件：借贷标识为"收入"
            income_mask = self.data[self.direction_column] == self.credit_flag
            # 支出条件：借贷标识为"支出"
            expense_mask = self.data[self.direction_column] == self.debit_flag
            
            # 处理收入金额
            if any(income_mask):
                # 确保金额为正数
                self.data.loc[income_mask, '收入金额'] = self.data.loc[income_mask, self.amount_column].abs()
            
            # 处理支出金额
            if any(expense_mask):
                # 确保金额为正数
                self.data.loc[expense_mask, '支出金额'] = self.data.loc[expense_mask, self.amount_column].abs()
        
        # 如果没有借贷标识列，则根据交易金额的正负判断
        elif self.amount_column in self.data.columns:
            # 收入条件：交易金额大于0
            income_mask = self.data[self.amount_column] > 0
            # 支出条件：交易金额小于0
            expense_mask = self.data[self.amount_column] < 0
            
            # 处理收入金额
            if any(income_mask):
                self.data.loc[income_mask, '收入金额'] = self.data.loc[income_mask, self.amount_column]
            
            # 处理支出金额
            if any(expense_mask):
                # 取绝对值，确保金额为正数
                self.data.loc[expense_mask, '支出金额'] = self.data.loc[expense_mask, self.amount_column].abs()
    
    def get_persons(self) -> List[str]:
        """
        获取所有人名
        
        Returns:
        --------
        List[str]
            所有人名列表
        """
        if self.name_column not in self.data.columns:
            return []
        
        return self.data[self.name_column].dropna().unique().tolist()
    
    def get_opposite_persons(self) -> List[str]:
        """
        获取所有对方人名
        
        Returns:
        --------
        List[str]
            所有对方人名列表
        """
        if self.opposite_name_column not in self.data.columns:
            return []
        
        return self.data[self.opposite_name_column].dropna().unique().tolist()
    
    def get_data_by_person(self, person_name: str) -> pd.DataFrame:
        """
        按人名筛选数据
        
        Parameters:
        -----------
        person_name : str
            人名
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        return self.filter_by_value(self.name_column, person_name)
    
    def get_data_by_opposite_person(self, opposite_person: str) -> pd.DataFrame:
        """
        按对方人名筛选数据
        
        Parameters:
        -----------
        opposite_person : str
            对方人名
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        if self.opposite_name_column not in self.data.columns:
            return pd.DataFrame()
        
        return self.filter_by_value(self.opposite_name_column, opposite_person)
    
    def get_income_data(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取收入数据
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只返回该人的收入数据
            
        Returns:
        --------
        pd.DataFrame
            收入数据
        """
        # 收入条件
        income_mask = self.data['收入金额'] > 0
        
        # 如果提供了人名，再按人名筛选
        if person_name:
            person_mask = self.data[self.name_column] == person_name
            income_mask = income_mask & person_mask
        
        return self.data[income_mask]
    
    def get_expense_data(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取支出数据
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只返回该人的支出数据
            
        Returns:
        --------
        pd.DataFrame
            支出数据
        """
        # 支出条件
        expense_mask = self.data['支出金额'] > 0
        
        # 如果提供了人名，再按人名筛选
        if person_name:
            person_mask = self.data[self.name_column] == person_name
            expense_mask = expense_mask & person_mask
        
        return self.data[expense_mask]
    
    def get_transaction_stats_by_opposite(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        按对方人名统计交易情况
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只统计该人的交易记录
            
        Returns:
        --------
        pd.DataFrame
            交易统计结果
        """
        # 筛选数据
        if person_name:
            data = self.get_data_by_person(person_name)
        else:
            data = self.data
        
        if data.empty or self.opposite_name_column not in data.columns:
            return pd.DataFrame()
        
        # 按对方人名分组统计
        result = data.groupby(self.opposite_name_column).agg({
            self.amount_column: ['sum', 'count'],
            '收入金额': 'sum',
            '支出金额': 'sum'
        }).reset_index()
        
        # 重命名列
        result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
        rename_dict = {
            f'{self.opposite_name_column}': '对方姓名',
            f'{self.amount_column}_sum': '交易总金额',
            f'{self.amount_column}_count': '交易次数',
            '收入金额_sum': '收入金额',
            '支出金额_sum': '支出金额'
        }
        result.rename(columns=rename_dict, inplace=True)
        
        # 计算收入和支出占比
        total_amount = result['交易总金额'].sum()
        if total_amount > 0:
            result['收入占比'] = (result['收入金额'] / total_amount * 100).round(2)
            result['支出占比'] = (result['支出金额'] / total_amount * 100).round(2)
        else:
            result['收入占比'] = 0
            result['支出占比'] = 0
        
        # 计算特殊时间次数
        if self.special_date_column in data.columns:
            special_date_counts = data[data[self.special_date_column].notna()].groupby(self.opposite_name_column).size().reset_index(name='特殊时间次数')
            result = pd.merge(result, special_date_counts, on='对方姓名', how='left')
            result['特殊时间次数'] = result['特殊时间次数'].fillna(0).astype(int)
        else:
            result['特殊时间次数'] = 0
        
        return result 