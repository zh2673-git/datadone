#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional
import os

from .payment_model import PaymentDataModel
from ..utils.config import Config

class AlipayDataModel(PaymentDataModel):
    """
    支付宝数据模型，继承自支付数据模型
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
        
        # 支付宝特定列名配置
        self.name_column = self.config.get('data_sources.alipay.name_column', '本方姓名')
        self.date_column = self.config.get('data_sources.alipay.date_column', '交易日期')
        self.amount_column = self.config.get('data_sources.alipay.amount_column', '交易金额')
        self.type_column = self.config.get('data_sources.alipay.type_column', '交易类型')
        self.opposite_column = self.config.get('data_sources.alipay.opposite_column', '对方姓名')
        self.opposite_name_column = self.config.get('data_sources.alipay.opposite_name_column', '对方姓名')
        self.remark_column = self.config.get('data_sources.alipay.remark_column', '交易备注')
        self.status_column = self.config.get('data_sources.alipay.status_column', '交易状态')
        
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
        支付宝数据预处理
        """
        # 调用父类预处理
        super().preprocess()
        
        # 支付宝特定处理
        # 1. 处理交易状态列
        if self.status_column not in self.data.columns:
            self.data[self.status_column] = '成功'  # 默认值
            self.logger.info(f"创建缺失的字段 '{self.status_column}'")
        
        # 2. 标准化交易类型
        if self.type_column in self.data.columns:
            # 将常见的支付宝交易类型进行标准化
            type_mapping = {
                '转账': '转账',
                '收款': '收款',
                '消费': '消费',
                '提现': '提现',
                '充值': '充值',
                '红包': '红包',
                '退款': '退款'
            }
            
            self.data[self.type_column] = self.data[self.type_column].map(
                lambda x: type_mapping.get(x, x) if pd.notna(x) else x
            )
        
        self.logger.info("支付宝数据预处理完成")
    
    def get_alipay_specific_stats(self, person_name: Optional[str] = None) -> Dict:
        """
        获取支付宝特定统计信息
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只统计该人的数据
            
        Returns:
        --------
        Dict
            支付宝特定统计信息
        """
        if person_name:
            data = self.get_data_by_person(person_name)
        else:
            data = self.data
        
        if data.empty:
            return {}
        
        stats = self.get_payment_stats(person_name)
        
        # 添加支付宝特定统计
        if self.status_column in data.columns:
            stats['交易状态分布'] = data[self.status_column].value_counts().to_dict()
        
        # 添加红包相关统计
        if self.type_column in data.columns:
            red_packet_data = data[data[self.type_column].str.contains('红包', na=False)]
            stats['红包交易次数'] = len(red_packet_data)
            stats['红包总金额'] = red_packet_data[self.amount_column].sum() if not red_packet_data.empty else 0
        
        return stats