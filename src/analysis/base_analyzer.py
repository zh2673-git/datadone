#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Optional

from ..model.base_model import BaseDataModel
from ..utils.group import GroupManager

class BaseAnalyzer(ABC):
    """
    数据分析器基类，所有数据分析器都应继承此类
    """
    def __init__(self, data_model: BaseDataModel, group_manager: Optional[GroupManager] = None, config: Optional[Dict] = None):
        """
        初始化数据分析器
        
        Parameters:
        -----------
        data_model : BaseDataModel
            数据模型
        group_manager : GroupManager, optional
            分组管理器
        config : dict, optional
            配置字典
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.data_model = data_model
        self.group_manager = group_manager
        self.config = config or {}
        self.results = {}
    
    @abstractmethod
    def analyze(self, analysis_type: str = 'all', source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        执行数据分析
        
        Parameters:
        -----------
        analysis_type : str, optional
            分析类型
        source_name : str, optional
            数据来源名称
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果
        """
        pass
    
    def get_results(self) -> Dict[str, pd.DataFrame]:
        """
        获取分析结果
        
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果
        """
        return self.results
    
    def clear_results(self):
        """
        清除分析结果
        """
        self.results = {}
    
    def export_results(self, export_type: str = 'excel', file_path: str = None) -> bool:
        """
        导出分析结果
        
        Parameters:
        -----------
        export_type : str, optional
            导出类型，如'excel'、'word'
        file_path : str, optional
            导出文件路径
            
        Returns:
        --------
        bool
            导出是否成功
        """
        if not self.results:
            self.logger.warning("没有分析结果可导出")
            return False
        
        try:
            if export_type == 'excel':
                from ..export.excel_exporter import ExcelExporter
                exporter = ExcelExporter()
                return exporter.export(self.results, file_path)
            elif export_type == 'word':
                from ..export.word_exporter import WordExporter
                exporter = WordExporter()
                return exporter.export(self.results, file_path)
            else:
                self.logger.error(f"不支持的导出类型: {export_type}")
                return False
        except Exception as e:
            self.logger.error(f"导出失败: {str(e)}")
            return False
    
    def get_analysis_types(self) -> List[str]:
        """
        获取支持的分析类型
        
        Returns:
        --------
        List[str]
            支持的分析类型列表
        """
        return ['all', 'frequency', 'cash', 'special', 'advanced']
    
    def validate_analysis_type(self, analysis_type: str) -> bool:
        """
        验证分析类型是否有效
        
        Parameters:
        -----------
        analysis_type : str
            分析类型
            
        Returns:
        --------
        bool
            是否有效
        """
        valid_types = self.get_analysis_types()
        return analysis_type in valid_types