#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from abc import ABC, abstractmethod
import logging
from typing import List

class BaseDataModel(ABC):
    """
    数据模型基类，所有数据源模型都应继承此类
    """
    def __init__(self, data_path=None, data=None):
        """
        初始化数据模型
        
        Parameters:
        -----------
        data_path : str, optional
            数据文件路径，如果提供则从文件加载数据
        data : pd.DataFrame, optional
            直接提供的数据，如果提供则使用此数据
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if data is not None:
            self.data = data
        elif data_path is not None:
            self.load_data(data_path)
        else:
            self.data = pd.DataFrame()
        
        # 数据基本信息
        self.file_path = data_path
        self.required_columns = []  # 子类需重写，指定必须的列
        self.column_mappings = {}   # 子类可重写，指定列名映射关系
        
        if not self.data.empty:
            self.logger.info(f"BaseDataModel - 加载数据后的列名: {self.data.columns.tolist()}")
            self.logger.info(f"BaseDataModel - 必需的列: {self.required_columns}")
            self.validate()
            self.preprocess()
    
    def get_data_sources(self) -> List[str]:
        """
        获取数据中所有的数据来源
        
        Returns:
        --------
        List[str]
            数据来源列表
        """
        if '数据来源' in self.data.columns:
            return self.data['数据来源'].unique().tolist()
        return []
    
    def load_data(self, file_path):
        """
        从文件加载数据
        
        Parameters:
        -----------
        file_path : str
            数据文件路径
        """
        try:
            if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                self.data = pd.read_excel(file_path)
            elif file_path.endswith('.csv'):
                self.data = pd.read_csv(file_path)
            else:
                raise ValueError(f"不支持的文件格式: {file_path}")
            
            self.logger.info(f"成功加载数据，共 {len(self.data)} 行")
        except Exception as e:
            self.logger.error(f"加载数据失败: {str(e)}")
            raise
    
    def validate(self):
        """
        验证数据是否符合要求
        """
        # 检查必须的列是否存在
        missing_columns = [col for col in self.required_columns if col not in self.data.columns]
        if missing_columns:
            self.logger.warning(f"数据缺少必要的列: {missing_columns}")
            raise ValueError(f"数据缺少必要的列: {missing_columns}")
        
        # 检查数据是否为空
        if self.data.empty:
            self.logger.warning("数据为空")
            raise ValueError("数据为空")
        
        self.logger.info("数据验证通过")
        return True
    
    @abstractmethod
    def preprocess(self):
        """
        数据预处理，子类必须实现
        """
        pass
    
    def get_unique_values(self, column):
        """
        获取指定列的唯一值
        
        Parameters:
        -----------
        column : str
            列名
            
        Returns:
        --------
        list
            唯一值列表
        """
        if column not in self.data.columns:
            self.logger.warning(f"列 {column} 不存在")
            return []
        
        return self.data[column].dropna().unique().tolist()
    
    def filter_by_value(self, column, value):
        """
        按列值筛选数据
        
        Parameters:
        -----------
        column : str
            列名
        value : any
            筛选值
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        if column not in self.data.columns:
            self.logger.warning(f"列 {column} 不存在")
            return pd.DataFrame()
        
        return self.data[self.data[column] == value]
    
    def filter_by_values(self, column, values):
        """
        按列值列表筛选数据
        
        Parameters:
        -----------
        column : str
            列名
        values : list
            筛选值列表
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        if column not in self.data.columns:
            self.logger.warning(f"列 {column} 不存在")
            return pd.DataFrame()
        
        return self.data[self.data[column].isin(values)]
    
    def get_data_by_person(self, person_name, person_column):
        """
        按人名筛选数据
        
        Parameters:
        -----------
        person_name : str
            人名
        person_column : str
            人名列
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        return self.filter_by_value(person_column, person_name)
    
    def get_stats(self):
        """
        获取数据统计信息
        
        Returns:
        --------
        dict
            统计信息
        """
        stats = {
            "行数": len(self.data),
            "列数": len(self.data.columns),
            "列名": list(self.data.columns),
        }
        return stats
    
    def __len__(self):
        """
        获取数据行数
        """
        return len(self.data)
    
    def __str__(self):
        """
        返回数据描述
        """
        return f"{self.__class__.__name__}: {len(self.data)} 行, {len(self.data.columns)} 列" 