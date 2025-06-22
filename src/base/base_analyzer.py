#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
import logging
from datetime import datetime, timedelta
import os

class BaseAnalyzer(ABC):
    """
    分析器基类，所有分析器都应继承此类
    """
    def __init__(self, data_model, group_manager=None):
        """
        初始化分析器
        
        Parameters:
        -----------
        data_model : BaseDataModel
            数据模型对象
        group_manager : GroupManager, optional
            分组管理器对象，如果不提供则只能以个人为单位分析
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.data_model = data_model
        self.group_manager = group_manager
        self.results = {}  # 存储分析结果
        
        if data_model is None:
            self.logger.warning("未提供数据模型")
            raise ValueError("未提供数据模型")
    
    @abstractmethod
    def analyze(self, *args, **kwargs):
        """
        执行分析，子类必须实现
        """
        pass
    
    def analyze_by_person(self, person_name, person_column):
        """
        按人进行分析，子类可重写
        
        Parameters:
        -----------
        person_name : str
            人名
        person_column : str
            人名列
            
        Returns:
        --------
        pd.DataFrame
            分析结果
        """
        person_data = self.data_model.get_data_by_person(person_name, person_column)
        if person_data.empty:
            self.logger.warning(f"找不到 {person_name} 的数据")
            return pd.DataFrame()
        
        # 具体分析逻辑由子类实现
        return person_data
    
    def analyze_by_group(self, group_name):
        """
        按组进行分析，子类可重写
        
        Parameters:
        -----------
        group_name : str
            组名
            
        Returns:
        --------
        pd.DataFrame
            分析结果
        """
        if self.group_manager is None:
            self.logger.warning("未提供分组管理器，无法按组分析")
            raise ValueError("未提供分组管理器，无法按组分析")
        
        group_members = self.group_manager.get_group(group_name)
        if not group_members:
            self.logger.warning(f"找不到分组 {group_name}")
            return pd.DataFrame()
        
        # 具体分析逻辑由子类实现
        return pd.DataFrame()
    
    def get_result(self, result_key=None):
        """
        获取分析结果
        
        Parameters:
        -----------
        result_key : str, optional
            结果键，如果不提供则返回所有结果
            
        Returns:
        --------
        dict or pd.DataFrame
            分析结果
        """
        if result_key is None:
            return self.results
        
        if result_key not in self.results:
            self.logger.warning(f"结果 {result_key} 不存在")
            return pd.DataFrame()
        
        return self.results[result_key]
    
    def export_result(self, result_key=None, file_path=None, sheet_name='结果'):
        """
        导出分析结果
        
        Parameters:
        -----------
        result_key : str, optional
            结果键，如果不提供则导出所有结果
        file_path : str, optional
            导出文件路径，如果不提供则使用默认路径
        sheet_name : str, optional
            工作表名称
            
        Returns:
        --------
        str
            导出文件路径
        """
        if not self.results:
            self.logger.warning("没有可导出的结果")
            return None
        
        if file_path is None:
            # 创建输出目录
            os.makedirs('output', exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"output/{self.__class__.__name__}_{timestamp}.xlsx"
        
        try:
            result = self.get_result(result_key)
            
            if isinstance(result, pd.DataFrame):
                result.to_excel(file_path, sheet_name=sheet_name, index=False)
            elif isinstance(result, dict):
                with pd.ExcelWriter(file_path) as writer:
                    for key, df in result.items():
                        if isinstance(df, pd.DataFrame):
                            df.to_excel(writer, sheet_name=key[:31], index=False)  # Excel表名最长31个字符
            
            self.logger.info(f"结果已导出至 {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"导出结果失败: {str(e)}")
            return None
    
    def calculate_date_range(self, df, date_column):
        """
        计算日期范围
        
        Parameters:
        -----------
        df : pd.DataFrame
            数据
        date_column : str
            日期列
            
        Returns:
        --------
        tuple
            (最小日期, 最大日期, 跨度天数)
        """
        if date_column not in df.columns or df.empty:
            return None, None, 0
        
        # 确保日期列为日期类型
        df.loc[:, date_column] = pd.to_datetime(df[date_column], errors='coerce')
        
        min_date = df[date_column].min()
        max_date = df[date_column].max()
        
        if pd.isna(min_date) or pd.isna(max_date):
            return None, None, 0
        
        date_range = (max_date - min_date).days + 1
        
        return min_date, max_date, date_range
    
    def calculate_special_date_count(self, df, date_column, special_date_column):
        """
        计算特殊日期次数
        
        Parameters:
        -----------
        df : pd.DataFrame
            数据
        date_column : str
            日期列
        special_date_column : str
            特殊日期列
            
        Returns:
        --------
        int
            特殊日期次数
        """
        if special_date_column not in df.columns or df.empty:
            return 0
        
        return df[special_date_column].notna().sum()
    
    def __str__(self):
        """
        返回分析器描述
        """
        return f"{self.__class__.__name__}: {len(self.results)} 个结果" 