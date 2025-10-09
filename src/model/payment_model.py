#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional
import os

from .base_model import BaseDataModel
from ..utils.config import Config

class PaymentDataModel(BaseDataModel):
    """
    支付数据模型基类，用于加载和处理支付数据
    """
    def __init__(self, data_path=None, data=None, config=None):
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
        """
        self.config = config or Config()
        
        # 只有在子类没有设置列名配置时才设置默认配置
        # 这样可以避免覆盖子类的特定配置
        if not hasattr(self, 'name_column'):
            self.name_column = self.config.get('data_sources.payment.name_column', '姓名')
        if not hasattr(self, 'date_column'):
            self.date_column = self.config.get('data_sources.payment.date_column', '交易日期')
        if not hasattr(self, 'amount_column'):
            self.amount_column = self.config.get('data_sources.payment.amount_column', '交易金额')
        if not hasattr(self, 'type_column'):
            self.type_column = self.config.get('data_sources.payment.type_column', '交易类型')
        if not hasattr(self, 'opposite_column'):
            self.opposite_column = self.config.get('data_sources.payment.opposite_column', '对方信息')
        if not hasattr(self, 'opposite_name_column'):
            self.opposite_name_column = self.config.get('data_sources.payment.opposite_name_column', '对方信息')
        if not hasattr(self, 'remark_column'):
            self.remark_column = self.config.get('data_sources.payment.remark_column', '备注')
        
        # 定义必需的列
        self.required_columns = [
            self.name_column,
            self.date_column,
            self.amount_column,
            self.type_column
        ]
        
        # 调用父类初始化
        super().__init__(data_path, data, config)
    
    def preprocess(self):
        """
        数据预处理
        """
        # 1. 确保核心列存在且类型正确
        # 确保日期列为日期类型
        if self.date_column in self.data.columns:
            self.data[self.date_column] = pd.to_datetime(self.data[self.date_column], errors='coerce')

        # 确保金额列为数值类型
        if self.amount_column in self.data.columns:
            self.data[self.amount_column] = pd.to_numeric(self.data[self.amount_column], errors='coerce')

        # 确保可选字段存在（如果不存在则创建默认值）
        if self.remark_column not in self.data.columns:
            self.data[self.remark_column] = ''
            self.logger.info(f"创建缺失的字段 '{self.remark_column}'")

        if self.opposite_column not in self.data.columns:
            self.data[self.opposite_column] = '未知'
            self.logger.info(f"创建缺失的字段 '{self.opposite_column}'")
        
        # 2. 添加收入和支出金额列（为分析器准备）
        self.data['收入金额'] = 0.0
        self.data['支出金额'] = 0.0
        
        # 根据交易类型和金额正负判断收入和支出
        if self.amount_column in self.data.columns and self.type_column in self.data.columns:
            # 对于支付数据，通常收入为正数，支出为负数
            # 或者根据交易类型判断
            income_mask = self.data[self.amount_column] > 0
            expense_mask = self.data[self.amount_column] < 0
            
            self.data.loc[income_mask, '收入金额'] = self.data.loc[income_mask, self.amount_column]
            self.data.loc[expense_mask, '支出金额'] = self.data.loc[expense_mask, self.amount_column].abs()
            
            self.logger.info("收入和支出金额列处理完成")
        
        # 3. 添加数据来源列
        if self.file_path and '数据来源' not in self.data.columns:
            source_name = os.path.splitext(os.path.basename(self.file_path))[0]
            self.data['数据来源'] = source_name
            self.logger.info(f"已添加 '数据来源' 列，值为 '{source_name}'")
        elif '数据来源' not in self.data.columns:
            self.data['数据来源'] = '支付数据' # 默认值
            self.logger.info("未找到文件路径，添加 '数据来源' 列，值为 '支付数据'")

        self.logger.info("支付数据预处理完成")
    
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
    
    def get_payment_types(self) -> List[str]:
        """
        获取所有支付类型
        
        Returns:
        --------
        List[str]
            支付类型列表
        """
        if self.type_column not in self.data.columns:
            return []
        
        return self.data[self.type_column].dropna().unique().tolist()
    
    def get_payment_stats(self, person_name: Optional[str] = None) -> Dict:
        """
        获取支付统计信息
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只统计该人的支付数据
            
        Returns:
        --------
        Dict
            支付统计信息
        """
        if person_name:
            data = self.get_data_by_person(person_name)
        else:
            data = self.data
        
        if data.empty:
            return {}
        
        stats = {
            '总交易次数': len(data),
            '总交易金额': data[self.amount_column].sum() if self.amount_column in data.columns else 0,
            '平均交易金额': data[self.amount_column].mean() if self.amount_column in data.columns else 0,
            '支付类型分布': data[self.type_column].value_counts().to_dict() if self.type_column in data.columns else {}
        }
        
        return stats