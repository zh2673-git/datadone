#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional
import os

from .payment_model import PaymentDataModel
from ..utils.config import Config

class WeChatDataModel(PaymentDataModel):
    """
    微信支付数据模型，继承自支付数据模型
    """
    def __init__(self, data_path=None, data=None, config=None):
        """
        初始化微信支付数据模型
        
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
        
        # 微信支付特定列名配置
        self.name_column = self.config.get('data_sources.wechat.name_column', '本方姓名')
        self.date_column = self.config.get('data_sources.wechat.date_column', '交易日期')
        self.amount_column = self.config.get('data_sources.wechat.amount_column', '交易金额')
        self.type_column = self.config.get('data_sources.wechat.type_column', '交易类型')
        self.opposite_column = self.config.get('data_sources.wechat.opposite_column', '对方姓名')
        self.opposite_name_column = self.config.get('data_sources.wechat.opposite_name_column', '对方姓名')
        self.remark_column = self.config.get('data_sources.wechat.remark_column', '标记备注')
        self.status_column = self.config.get('data_sources.wechat.status_column', '当前状态')
        
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
        微信支付数据预处理
        """
        # 调用父类预处理
        super().preprocess()
        
        # 微信支付特定处理
        # 1. 处理交易状态列
        if self.status_column not in self.data.columns:
            self.data[self.status_column] = '支付成功'  # 默认值
            self.logger.info(f"创建缺失的字段 '{self.status_column}'")
        
        # 2. 标准化交易类型
        if self.type_column in self.data.columns:
            # 将常见的微信支付交易类型进行标准化
            type_mapping = {
                '微信转账': '转账',
                '微信红包': '红包',
                '商户消费': '消费',
                '二维码收款': '收款',
                '零钱提现': '提现',
                '零钱充值': '充值'
            }
            
            self.data[self.type_column] = self.data[self.type_column].map(
                lambda x: type_mapping.get(x, x) if pd.notna(x) else x
            )
        
        self.logger.info("微信支付数据预处理完成")
    
    def get_wechat_specific_stats(self, person_name: Optional[str] = None) -> Dict:
        """
        获取微信支付特定统计信息
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只统计该人的数据
            
        Returns:
        --------
        Dict
            微信支付特定统计信息
        """
        if person_name:
            data = self.get_data_by_person(person_name)
        else:
            data = self.data
        
        if data.empty:
            return {}
        
        stats = self.get_payment_stats(person_name)
        
        # 添加微信支付特定统计
        if self.status_column in data.columns:
            stats['交易状态分布'] = data[self.status_column].value_counts().to_dict()
        
        # 添加红包相关统计
        if self.type_column in data.columns:
            red_packet_data = data[data[self.type_column].str.contains('红包', na=False)]
            stats['红包交易次数'] = len(red_packet_data)
            stats['红包总金额'] = red_packet_data[self.amount_column].sum() if not red_packet_data.empty else 0
            
            # 微信转账统计
            transfer_data = data[data[self.type_column].str.contains('转账', na=False)]
            stats['转账交易次数'] = len(transfer_data)
            stats['转账总金额'] = transfer_data[self.amount_column].sum() if not transfer_data.empty else 0
        
        return stats