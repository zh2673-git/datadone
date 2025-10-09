#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
import pandas as pd

from ..utils.constants import FilePath
from ..utils.exceptions import ExportError

class BaseExporter(ABC):
    """
    导出器基类，定义所有导出器的通用接口
    """
    def __init__(self, output_dir: Optional[str] = None):
        """
        初始化导出器
        
        Parameters:
        -----------
        output_dir : str, optional
            输出目录，如果不提供则使用默认目录
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir or FilePath.OUTPUT_DIR
        
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
    
    @abstractmethod
    def export(self, analysis_results: Dict[str, pd.DataFrame], filename: str, **kwargs) -> str:
        """
        导出分析结果
        
        Parameters:
        -----------
        analysis_results : Dict[str, pd.DataFrame]
            分析结果，键为结果名，值为结果数据
        filename : str
            导出文件名（不含路径和扩展名）
        **kwargs
            其他导出参数
            
        Returns:
        --------
        str
            导出文件的完整路径
        """
        pass
    
    def _get_file_path(self, filename: str, extension: str) -> str:
        """
        生成完整的文件路径
        
        Parameters:
        -----------
        filename : str
            文件名（不含路径和扩展名）
        extension : str
            文件扩展名，不含点号
            
        Returns:
        --------
        str
            完整的文件路径
        """
        # 如果filename包含路径分隔符，认为它是一个相对或绝对路径
        if os.path.sep in filename:
            # 确保文件目录存在
            file_dir = os.path.dirname(filename)
            if file_dir:
                os.makedirs(file_dir, exist_ok=True)
            
            # 如果文件名不包含扩展名，添加扩展名
            if not filename.endswith(f'.{extension}'):
                return f"{filename}.{extension}"
            return filename
        
        # 普通文件名，拼接到输出目录
        return os.path.join(self.output_dir, f"{filename}.{extension}")
    
    def _sanitize_sheet_name(self, name: str, max_length: int = 31) -> str:
        """
        清理工作表名称，确保符合Excel的限制
        
        Parameters:
        -----------
        name : str
            原始工作表名称
        max_length : int, optional
            工作表名称最大长度，默认为31（Excel的限制）
            
        Returns:
        --------
        str
            清理后的工作表名称
        """
        # 移除Excel不允许的字符
        forbidden_chars = ['\\', '/', '?', '*', ':', '[', ']']
        result = name
        for char in forbidden_chars:
            result = result.replace(char, '_')
        
        # 截断到最大长度
        return result[:max_length]
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        验证数据是否可导出
        
        Parameters:
        -----------
        data : pd.DataFrame
            要验证的数据
            
        Returns:
        --------
        bool
            数据是否有效
        """
        return isinstance(data, pd.DataFrame) and not data.empty