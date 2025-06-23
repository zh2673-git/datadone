#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import os

from src.base import BaseDataModel
from src.utils.config import Config
from src.utils.constants import ColumnName
from src.utils.exceptions import DataValidationError

class PaymentDataModel(BaseDataModel):
    """
    支付类数据模型基类，为支付宝、微信等支付类数据提供共同的处理逻辑
    """
    def __init__(self, data_path=None, data=None, config=None, data_source_type=None):
        """
        初始化支付数据模型
        
        Parameters:
        -----------
        data_path : str, optional
            数据文件路径，如果提供则从文件加载数据
        data : pd.DataFrame, optional
            直接提供的数据，如果提供则使用此数据
        config : Config, optional
            配置对象，如果不提供则使用默认配置
        data_source_type : str, optional
            数据源类型，例如'alipay', 'wechat'，用于从配置中获取相应的列名
        """
        # 初始化配置和数据源类型
        self.config = config or Config()
        self.data_source_type = data_source_type
        
        if not self.data_source_type:
            raise ValueError("必须指定数据源类型")
        
        # 配置路径前缀
        config_prefix = f'data_sources.{self.data_source_type}'
        
        # 定义通用列名配置
        self.name_column = self.config.get(f'{config_prefix}.name_column', ColumnName.NAME)
        self.date_column = self.config.get(f'{config_prefix}.date_column', ColumnName.DATE)
        self.time_column = self.config.get(f'{config_prefix}.time_column', '交易时间')
        self.amount_column = self.config.get(f'{config_prefix}.amount_column', ColumnName.AMOUNT)
        self.balance_column = self.config.get(f'{config_prefix}.balance_column', ColumnName.BALANCE)
        self.direction_column = self.config.get(f'{config_prefix}.direction_column', ColumnName.DIRECTION)
        self.opposite_name_column = self.config.get(f'{config_prefix}.opposite_name_column', ColumnName.OPPOSITE_NAME)
        self.special_date_column = self.config.get(f'{config_prefix}.special_date_column', ColumnName.SPECIAL_DATE)
        
        # 借贷标识
        self.credit_flag = self.config.get(f'{config_prefix}.credit_flag', '收入')  # 收入
        self.debit_flag = self.config.get(f'{config_prefix}.debit_flag', '支出')    # 支出
        
        # 定义必需的列
        self.required_columns = [
            self.name_column,
            self.date_column,
            self.amount_column
        ]
        
        # 确保所有子类特有的属性都已经初始化
        # 对于支付宝
        if self.data_source_type == 'alipay':
            self.transaction_type_column = self.config.get('data_sources.alipay.transaction_type_column', '交易类型')
            self.remark_column = self.config.get('data_sources.alipay.remark_column', '交易备注')
            self.transaction_status_column = self.config.get('data_sources.alipay.transaction_status_column', '交易状态')
        
        # 对于微信
        if self.data_source_type == 'wechat':
            self.account_column = self.config.get('data_sources.wechat.account_column', '本方微信账号')
            self.opposite_account_column = self.config.get('data_sources.wechat.opposite_account_column', '对方微信账号')
            self.type_column = self.config.get('data_sources.wechat.type_column', '交易类型')
        
        # 最后调用父类初始化
        super().__init__(data_path, data)
    
    def preprocess(self):
        """
        公共数据预处理逻辑
        
        子类应该调用此方法，然后添加自己特定的处理逻辑
        """
        # 确保日期列为日期类型
        if self.date_column in self.data.columns:
            self.data[self.date_column] = pd.to_datetime(self.data[self.date_column], errors='coerce')
        
        # 处理交易金额列
        if self.amount_column in self.data.columns:
            # 清理并转换为数值
            if self.data[self.amount_column].dtype == object:
                self.data[self.amount_column] = self.data[self.amount_column].astype(str).str.strip()
                self.data[self.amount_column] = pd.to_numeric(self.data[self.amount_column], errors='coerce')
        
        # 处理账户余额列
        if self.balance_column in self.data.columns:
            if self.data[self.balance_column].dtype == object:
                self.data[self.balance_column] = self.data[self.balance_column].astype(str).str.strip()
                self.data[self.balance_column] = pd.to_numeric(self.data[self.balance_column], errors='coerce')
        
        # 添加收入和支出列
        self.add_income_expense_columns()
        
        # 添加数据来源列
        if self.file_path and ColumnName.DATA_SOURCE not in self.data.columns:
            source_name = os.path.splitext(os.path.basename(self.file_path))[0]
            self.data[ColumnName.DATA_SOURCE] = source_name
            self.logger.info(f"已添加 '{ColumnName.DATA_SOURCE}' 列，值为 '{source_name}'")
        elif ColumnName.DATA_SOURCE not in self.data.columns:
            self.data[ColumnName.DATA_SOURCE] = f'{self.data_source_type}数据'  # 默认值
            self.logger.info(f"未找到文件路径，添加 '{ColumnName.DATA_SOURCE}' 列，值为 '{self.data_source_type}数据'")
        
        self.logger.info(f"{self.data_source_type}数据预处理完成")
    
    def add_income_expense_columns(self):
        """
        添加收入和支出列
        
        此方法可能会被子类覆盖，以适应不同的数据格式
        """
        # 初始化收入和支出列
        self.data[ColumnName.INCOME_AMOUNT] = 0.0
        self.data[ColumnName.EXPENSE_AMOUNT] = 0.0
        
        # 根据借贷标识和交易金额填充收入和支出金额
        processed_with_direction = False
        if self.direction_column in self.data.columns and self.amount_column in self.data.columns:
            # 收入条件：借贷标识为信用标识（如"收入"、"入"）
            income_mask = self.data[self.direction_column] == self.credit_flag
            # 支出条件：借贷标识为借方标识（如"支出"、"出"）
            expense_mask = self.data[self.direction_column] == self.debit_flag
            
            # 只有当至少有一个匹配时，才认为此方法有效
            if income_mask.any() or expense_mask.any():
                # 处理收入金额
                if income_mask.any():
                    self.data.loc[income_mask, ColumnName.INCOME_AMOUNT] = self.data.loc[income_mask, self.amount_column].abs()
                
                # 处理支出金额
                if expense_mask.any():
                    self.data.loc[expense_mask, ColumnName.EXPENSE_AMOUNT] = self.data.loc[expense_mask, self.amount_column].abs()
                
                processed_with_direction = True
        
        # 如果没有借贷标识列，或者借贷标识列没有产生任何有效结果，则根据交易金额的正负判断
        if not processed_with_direction and self.amount_column in self.data.columns:
            self.logger.info("未找到有效的借贷标识，尝试根据交易金额正负判断收支")
            # 收入条件：交易金额大于0
            income_mask = self.data[self.amount_column] > 0
            # 支出条件：交易金额小于0
            expense_mask = self.data[self.amount_column] < 0
            
            # 处理收入金额
            if income_mask.any():
                self.data.loc[income_mask, ColumnName.INCOME_AMOUNT] = self.data.loc[income_mask, self.amount_column]
            
            # 处理支出金额
            if expense_mask.any():
                # 取绝对值，确保金额为正数
                self.data.loc[expense_mask, ColumnName.EXPENSE_AMOUNT] = self.data.loc[expense_mask, self.amount_column].abs()
    
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
        income_mask = self.data[ColumnName.INCOME_AMOUNT] > 0
        
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
        expense_mask = self.data[ColumnName.EXPENSE_AMOUNT] > 0
        
        # 如果提供了人名，再按人名筛选
        if person_name:
            person_mask = self.data[self.name_column] == person_name
            expense_mask = expense_mask & person_mask
        
        return self.data[expense_mask]
    
    def get_transaction_stats_by_opposite(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取与对方的交易统计数据
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只统计该人的交易数据
            
        Returns:
        --------
        pd.DataFrame
            交易统计数据，包含收入次数、收入金额、支出次数、支出金额等
        """
        # 筛选数据
        data_to_analyze = self.data
        if person_name:
            data_to_analyze = self.get_data_by_person(person_name)
        
        if data_to_analyze.empty:
            return pd.DataFrame()
        
        # 按对方姓名分组统计
        group_by = [self.opposite_name_column]
        
        # 统计收入
        income_stats = data_to_analyze[data_to_analyze[ColumnName.INCOME_AMOUNT] > 0].groupby(group_by).agg({
            ColumnName.INCOME_AMOUNT: ['count', 'sum', 'mean'],
            self.date_column: ['min', 'max']
        })
        
        # 统计支出
        expense_stats = data_to_analyze[data_to_analyze[ColumnName.EXPENSE_AMOUNT] > 0].groupby(group_by).agg({
            ColumnName.EXPENSE_AMOUNT: ['count', 'sum', 'mean'],
            self.date_column: ['min', 'max']
        })
        
        # 重命名列
        income_stats.columns = ['收入次数', '收入总额', '平均收入', '首次收入日期', '最近收入日期']
        expense_stats.columns = ['支出次数', '支出总额', '平均支出', '首次支出日期', '最近支出日期']
        
        # 合并收入和支出统计
        result = pd.merge(income_stats, expense_stats, left_index=True, right_index=True, how='outer').fillna(0)
        
        # 计算总交易次数和净收支
        result['交易次数'] = result['收入次数'] + result['支出次数']
        result['净收支'] = result['收入总额'] - result['支出总额']
        
        # 计算日期范围
        date_min = pd.concat([
            result['首次收入日期'].dropna(),
            result['首次支出日期'].dropna()
        ]).min()
        
        date_max = pd.concat([
            result['最近收入日期'].dropna(),
            result['最近支出日期'].dropna()
        ]).max()
        
        if not pd.isna(date_min) and not pd.isna(date_max):
            result['交易日期范围'] = (date_max - date_min).days + 1
        else:
            result['交易日期范围'] = 0
        
        return result.reset_index().sort_values('交易次数', ascending=False)
    
    def get_data_sources(self) -> List[str]:
        """
        获取所有数据源名称
        
        Returns:
        --------
        List[str]
            所有数据源名称列表
        """
        if ColumnName.DATA_SOURCE not in self.data.columns:
            return []
        
        return self.data[ColumnName.DATA_SOURCE].dropna().unique().tolist() 