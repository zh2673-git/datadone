#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import os

from src.base import BaseDataModel
from src.utils.config import Config

class CallDataModel(BaseDataModel):
    """
    话单数据模型，用于加载和处理通话记录数据
    """
    def __init__(self, data_path=None, data=None, config=None):
        """
        初始化话单数据模型
        
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
        self.name_column = self.config.get('data_sources.call.name_column', '本方姓名')
        self.phone_column = self.config.get('data_sources.call.phone_column', '本方号码')
        self.date_column = self.config.get('data_sources.call.date_column', '呼叫日期')
        self.duration_column = self.config.get('data_sources.call.duration_column', '通话时长')
        self.call_type_column = self.config.get('data_sources.call.call_type_column', '呼叫类型')
        self.opposite_name_column = self.config.get('data_sources.call.opposite_name_column', '对方姓名')
        self.opposite_phone_column = self.config.get('data_sources.call.opposite_number_column', '对方号码')
        self.opposite_location_column = self.config.get('data_sources.call.opposite_location_column', '对方号码归属地')
        self.opposite_company_column = self.config.get('data_sources.call.opposite_company_column', '对方单位名称')
        self.opposite_position_column = self.config.get('data_sources.call.opposite_position_column', '对方职务')
        self.special_date_column = self.config.get('data_sources.call.special_date_column', '特殊日期名称')
        self.location_column = self.config.get('data_sources.call.location_column', '通话地址')
        
        # 呼叫类型标识
        self.incoming_call = self.config.get('data_sources.call.incoming_call', '被叫')  # 被叫
        self.outgoing_call = self.config.get('data_sources.call.outgoing_call', '主叫')  # 主叫
        self.missed_call = self.config.get('data_sources.call.missed_call', '未接')      # 未接
        self.received_sms = self.config.get('data_sources.call.received_sms', '接收')    # 接收短信
        self.sent_sms = self.config.get('data_sources.call.sent_sms', '发送')            # 发送短信
        
        # 定义必需的列
        self.required_columns = [
            self.name_column,
            self.date_column,
            self.opposite_phone_column
        ]
        
        # 调用父类初始化
        super().__init__(data_path, data)
    
    def preprocess(self):
        """
        数据预处理
        
        根据示例数据:
        - 呼叫日期格式为 "YYYY/MM/DD HH:MM:SS"
        - 通话时长为秒数
        - 呼叫类型包括 "主叫"、"被叫"、"接收"
        """
        self.logger.info(f"话单数据模型预处理开始，文件路径: {self.file_path}")
        self.logger.info(f"预期 '本方姓名' 列名: {self.name_column}")
        self.logger.info(f"加载后数据列名: {self.data.columns.tolist()}")

        # 确保日期列为日期类型
        if self.date_column in self.data.columns:
            # 示例数据中日期格式为 "YYYY/MM/DD HH:MM:SS"
            self.data[self.date_column] = pd.to_datetime(self.data[self.date_column], errors='coerce')
        
        # 确保通话时长列为数值类型
        if self.duration_column in self.data.columns:
            self.data[self.duration_column] = pd.to_numeric(self.data[self.duration_column], errors='coerce')
            # 将NaN值替换为0
            self.data[self.duration_column].fillna(0, inplace=True)
        
        # 添加主叫次数和被叫次数列
        self.add_call_type_columns()
        
        # 添加数据来源列
        if self.file_path and '数据来源' not in self.data.columns:
            source_name = os.path.splitext(os.path.basename(self.file_path))[0]
            self.data['数据来源'] = source_name
            self.logger.info(f"已添加 '数据来源' 列，值为 '{source_name}'")
        elif '数据来源' not in self.data.columns:
            self.data['数据来源'] = '话单数据' # 默认值
            self.logger.info("未找到文件路径，添加 '数据来源' 列，值为 '话单数据'")
        
        self.logger.info("话单数据预处理完成")
    
    def add_call_type_columns(self):
        """
        添加主叫次数和被叫次数列
        
        根据示例数据:
        - 呼叫类型包括 "主叫"、"被叫"、"接收"
        - 接收表示短信，通话时长为0
        """
        # 初始化主叫次数和被叫次数列
        self.data['主叫次数'] = 0
        self.data['被叫次数'] = 0
        self.data['短信次数'] = 0
        
        # 根据呼叫类型填充主叫次数和被叫次数
        if self.call_type_column in self.data.columns:
            # 主叫条件
            outgoing_mask = self.data[self.call_type_column] == self.outgoing_call
            # 被叫条件
            incoming_mask = self.data[self.call_type_column] == self.incoming_call
            # 短信条件（接收或发送）
            sms_mask = (self.data[self.call_type_column] == self.received_sms) | (self.data[self.call_type_column] == self.sent_sms)
            
            # 填充主叫次数和被叫次数
            self.data.loc[outgoing_mask, '主叫次数'] = 1
            self.data.loc[incoming_mask, '被叫次数'] = 1
            self.data.loc[sms_mask, '短信次数'] = 1
            
            # 检查是否有未识别的呼叫类型
            unknown_types = set(self.data[self.call_type_column].unique()) - {self.outgoing_call, self.incoming_call, self.received_sms, self.sent_sms, self.missed_call, None, np.nan, ''}
            if unknown_types:
                self.logger.warning(f"发现未识别的呼叫类型: {unknown_types}")
        
        # 如果没有呼叫类型列，则根据通话时长判断是否为短信
        elif self.duration_column in self.data.columns:
            # 短信条件：通话时长为0
            sms_mask = self.data[self.duration_column] == 0
            self.data.loc[sms_mask, '短信次数'] = 1
            # 其他情况视为通话，但无法区分主叫和被叫
            self.logger.warning("数据中没有呼叫类型列，无法区分主叫和被叫")
    
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
    
    def get_data_by_opposite_number(self, opposite_number: str) -> pd.DataFrame:
        """
        按对方号码筛选数据
        
        Parameters:
        -----------
        opposite_number : str
            对方号码
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        if self.opposite_phone_column not in self.data.columns:
            return pd.DataFrame()
        
        return self.filter_by_value(self.opposite_phone_column, opposite_number)
    
    def get_outgoing_calls(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取主叫通话记录
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只返回该人的主叫记录
            
        Returns:
        --------
        pd.DataFrame
            主叫通话记录
        """
        # 主叫条件
        outgoing_mask = self.data['主叫次数'] > 0
        
        # 如果提供了人名，再按人名筛选
        if person_name:
            person_mask = self.data[self.name_column] == person_name
            outgoing_mask = outgoing_mask & person_mask
        
        return self.data[outgoing_mask]
    
    def get_incoming_calls(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取被叫通话记录
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只返回该人的被叫记录
            
        Returns:
        --------
        pd.DataFrame
            被叫通话记录
        """
        # 被叫条件
        incoming_mask = self.data['被叫次数'] > 0
        
        # 如果提供了人名，再按人名筛选
        if person_name:
            person_mask = self.data[self.name_column] == person_name
            incoming_mask = incoming_mask & person_mask
        
        return self.data[incoming_mask]
    
    def get_call_stats_by_opposite(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        按对方号码统计通话情况
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只统计该人的通话记录
            
        Returns:
        --------
        pd.DataFrame
            通话统计结果
        """
        # 筛选数据
        if person_name:
            data = self.get_data_by_person(person_name)
        else:
            data = self.data
        
        if data.empty:
            return pd.DataFrame()
        
        # 按对方号码分组统计
        group_columns = [self.opposite_phone_column]
        if self.opposite_name_column in data.columns:
            group_columns.append(self.opposite_name_column)
        
        if self.opposite_company_column in data.columns:
            group_columns.append(self.opposite_company_column)
        
        if self.opposite_location_column in data.columns:
            group_columns.append(self.opposite_location_column)
        
        result = data.groupby(group_columns).agg({
            self.duration_column: ['sum', 'count'],
            '主叫次数': 'sum',
            '被叫次数': 'sum'
        }).reset_index()
        
        # 重命名列
        result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
        rename_dict = {
            f'{self.opposite_phone_column}': '对方号码',
            f'{self.opposite_name_column}': '对方姓名',
            f'{self.opposite_company_column}': '对方单位',
            f'{self.opposite_location_column}': '对方号码归属地',
            f'{self.duration_column}_sum': '通话总时长(秒)',
            f'{self.duration_column}_count': '通话次数',
            '主叫次数_sum': '主叫次数',
            '被叫次数_sum': '被叫次数'
        }
        result.rename(columns={k: v for k, v in rename_dict.items() if k in result.columns}, inplace=True)
        
        # 添加分钟列
        if '通话总时长(秒)' in result.columns:
            result['通话总时长(分钟)'] = (result['通话总时长(秒)'] / 60).round(2)
        
        # 计算特殊时间次数
        if self.special_date_column in data.columns:
            special_date_counts = data[data[self.special_date_column].notna()].groupby(group_columns).size().reset_index(name='特殊时间次数')
            result = pd.merge(result, special_date_counts, on=group_columns, how='left')
            result['特殊时间次数'] = result['特殊时间次数'].fillna(0).astype(int)
        else:
            result['特殊时间次数'] = 0
        
        return result 