#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional
import os

from .base_model import BaseDataModel
from ..utils.config import Config

class CallDataModel(BaseDataModel):
    """
    通话数据模型，用于加载和处理通话记录数据
    """
    def __init__(self, data_path=None, data=None, config=None):
        """
        初始化通话数据模型
        
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
        self.name_column = self.config.get('data_sources.call.name_column', '姓名')
        self.date_column = self.config.get('data_sources.call.date_column', '通话日期')
        self.time_column = self.config.get('data_sources.call.time_column', '通话时间')
        self.duration_column = self.config.get('data_sources.call.duration_column', '通话时长')
        self.type_column = self.config.get('data_sources.call.type_column', '通话类型')
        self.opposite_column = self.config.get('data_sources.call.opposite_column', '对方号码')
        self.opposite_phone_column = self.config.get('data_sources.call.opposite_phone_column', '对方号码')
        self.opposite_name_column = self.config.get('data_sources.call.opposite_name_column', '对方姓名')
        self.opposite_unit_column = self.config.get('data_sources.call.opposite_unit_column', '对方单位名称')
        self.opposite_title_column = self.config.get('data_sources.call.opposite_title_column', '对方职务')
        self.remark_column = self.config.get('data_sources.call.remark_column', '备注')
        
        # 定义必需的列
        self.required_columns = [
            self.name_column,
            self.date_column,
            self.time_column,
            self.duration_column,
            self.type_column,
            self.opposite_column
        ]
        
        # 调用父类初始化
        super().__init__(data_path, data)
    
    def preprocess(self):
        """
        数据预处理
        """
        # 1. 确保核心列存在且类型正确
        # 确保日期列为日期类型
        if self.date_column in self.data.columns:
            self.data[self.date_column] = pd.to_datetime(self.data[self.date_column], errors='coerce')

        # 确保通话时长为数值类型（秒）
        if self.duration_column in self.data.columns:
            self.data[self.duration_column] = pd.to_numeric(self.data[self.duration_column], errors='coerce')

        # 确保可选字段存在（如果不存在则创建默认值）
        if self.remark_column not in self.data.columns:
            self.data[self.remark_column] = ''
            self.logger.info(f"创建缺失的字段 '{self.remark_column}'")
        
        # 2. 添加数据来源列
        if self.file_path and '数据来源' not in self.data.columns:
            source_name = os.path.splitext(os.path.basename(self.file_path))[0]
            self.data['数据来源'] = source_name
            self.logger.info(f"已添加 '数据来源' 列，值为 '{source_name}'")
        elif '数据来源' not in self.data.columns:
            self.data['数据来源'] = '通话数据' # 默认值
            self.logger.info("未找到文件路径，添加 '数据来源' 列，值为 '通话数据'")

        self.logger.info("通话数据预处理完成")
    
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
    
    def get_call_types(self) -> List[str]:
        """
        获取所有通话类型
        
        Returns:
        --------
        List[str]
            通话类型列表
        """
        if self.type_column not in self.data.columns:
            return []
        
        return self.data[self.type_column].dropna().unique().tolist()
    
    def get_call_stats(self, person_name: Optional[str] = None) -> Dict:
        """
        获取通话统计信息
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只统计该人的通话数据
            
        Returns:
        --------
        Dict
            通话统计信息
        """
        if person_name:
            data = self.get_data_by_person(person_name)
        else:
            data = self.data
        
        if data.empty:
            return {}
        
        stats = {
            '总通话次数': len(data),
            '总通话时长': data[self.duration_column].sum() if self.duration_column in data.columns else 0,
            '平均通话时长': data[self.duration_column].mean() if self.duration_column in data.columns else 0,
            '通话类型分布': data[self.type_column].value_counts().to_dict() if self.type_column in data.columns else {}
        }
        
        return stats