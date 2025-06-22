#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import List, Dict, Union, Optional
from datetime import datetime

from src.base import BaseAnalyzer
from src.datasource import CallDataModel
from src.group import GroupManager

class CallAnalyzer(BaseAnalyzer):
    """
    话单数据分析器，用于分析通话记录数据
    """
    def __init__(self, data_model: CallDataModel, group_manager: Optional[GroupManager] = None):
        """
        初始化话单数据分析器
        
        Parameters:
        -----------
        data_model : CallDataModel
            话单数据模型
        group_manager : GroupManager, optional
            分组管理器
        """
        if not isinstance(data_model, CallDataModel):
            raise TypeError("data_model必须是CallDataModel类型")
        
        super().__init__(data_model, group_manager)
        self.call_model = data_model
    
    def analyze(self, source_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        执行话单数据分析, 按数据来源进行聚合.
        
        Parameters:
        -----------
        source_name : str, optional
            数据来源名称 (例如 '吴平一家明细.xlsx'). 如果提供, 只分析此来源.
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果, 键为结果名 (例如 '吴平一家明细.xlsx_通话频率'), 值为结果数据
        """
        all_results = {}
        
        if source_name:
            sources_to_analyze = [source_name]
        else:
            sources_to_analyze = self.call_model.get_data_sources()

        if not sources_to_analyze:
            self.logger.warning("没有找到可分析的数据来源.")
            return {}
            
        for source in sources_to_analyze:
            results = self._analyze_for_source(source)
            all_results.update(results)
        
        self.results = all_results
        return all_results

    def _analyze_for_source(self, source_name: str) -> Dict[str, pd.DataFrame]:
        """
        为指定的数据来源执行分析
        
        Parameters:
        -----------
        source_name : str
            数据来源名称
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            分析结果
        """
        results = {}
        source_data = self.call_model.data[self.call_model.data['数据来源'] == source_name]

        if source_data.empty:
            self.logger.warning(f"找不到数据来源 '{source_name}' 的数据")
            return results
        
        # 执行通话频率分析
        frequency_result = self.analyze_call_frequency(source_data)
        if not frequency_result.empty:
            results[f"{source_name}_通话频率"] = frequency_result
        
        return results

    def analyze_call_frequency(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析通话频率
        
        Parameters:
        -----------
        data : pd.DataFrame
            要分析的数据 (单一来源)
            
        Returns:
        --------
        pd.DataFrame
            通话频率分析结果
        """
        if data.empty:
            self.logger.warning("没有通话记录数据")
            return pd.DataFrame()
        
        # 获取相关列名
        name_col = self.call_model.name_column
        opposite_number_col = self.call_model.opposite_phone_column
        opposite_name_col = self.call_model.opposite_name_column
        opposite_company_col = self.call_model.opposite_company_column
        opposite_position_col = self.call_model.opposite_position_column
        date_col = self.call_model.date_column
        duration_col = self.call_model.duration_column
        
        # 定义分组键
        group_cols = [name_col, opposite_name_col, opposite_number_col]

        # 1. 聚合计算
        agg_operations = {
            duration_col: ['sum', 'count'],
            date_col: ['min', 'max']
        }
        
        # 如果存在对方单位和职务列，添加到聚合操作中
        if opposite_company_col in data.columns:
            agg_operations[opposite_company_col] = lambda x: '|'.join(sorted(set(x.dropna().astype(str))))
        if opposite_position_col in data.columns:
            agg_operations[opposite_position_col] = lambda x: '|'.join(sorted(set(x.dropna().astype(str))))
        
        # 按所有相关信息分组
        result = data.groupby(group_cols).agg(agg_operations)
        
        # 扁平化多级列索引
        result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
        result = result.reset_index()
        
        # 2. 计算衍生列
        duration_sum_col = f'{duration_col}_sum'
        date_min_col = f'{date_col}_min'
        date_max_col = f'{date_col}_max'
        
        if date_max_col in result.columns and date_min_col in result.columns:
            result['通话时间跨度'] = (result[date_max_col] - result[date_min_col]).dt.days + 1
        else:
            result['通话时间跨度'] = 1

        if duration_sum_col in result.columns:
            result['通话总时长(分钟)'] = (result[duration_sum_col] / 60).round(2)
        else:
            result['通话总时长(分钟)'] = 0

        # 3. 重命名列以满足输出格式
        rename_dict = {
            name_col: '本方姓名',
            opposite_name_col: '对方姓名',
            opposite_number_col: '对方号码',
            f'{duration_col}_count': '通话次数',
        }
        
        # 添加对方单位和职务的重命名
        if opposite_company_col in result.columns:
            rename_dict[f'{opposite_company_col}'] = '对方单位名称_<lambda>'
        if opposite_position_col in result.columns:
            rename_dict[f'{opposite_position_col}'] = '对方职务_<lambda>'

        result.rename(columns=rename_dict, inplace=True)
        
        # 4. 设置列顺序
        final_cols = [
            '本方姓名', '对方姓名', '对方号码', 
            '通话次数', '通话总时长(分钟)', '通话时间跨度'
        ]
        
        # 添加对方单位和职务列（如果存在）
        if '对方单位名称_<lambda>' in result.columns:
            final_cols.append('对方单位名称_<lambda>')
        if '对方职务_<lambda>' in result.columns:
            final_cols.append('对方职务_<lambda>')
        
        # 筛选出结果中实际存在的列
        ordered_cols = [col for col in final_cols if col in result.columns]
        
        # 将其他可能存在的列（比如旧的聚合列）也加上，但放在最后
        for col in result.columns:
            if col not in ordered_cols:
                ordered_cols.append(col)

        result = result[ordered_cols]
        result['数据来源'] = data['数据来源'].iloc[0]
        
        return result.sort_values(by=['本方姓名', '通话次数'], ascending=[True, False])
    
    def get_most_frequent_calls(self, person_name: Optional[str] = None, top_n: int = 10) -> pd.DataFrame:
        """
        获取最频繁通话的对方
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果不提供则分析所有人
        top_n : int
            返回的记录数量
            
        Returns:
        --------
        pd.DataFrame
            最频繁通话的对方记录
        """
        # 获取通话频率分析结果
        name_col = self.call_model.name_column
        if person_name:
            frequency_result = self.analyze_call_frequency(self.call_model.data[self.call_model.data[name_col] == person_name])
        else:
            # 分析所有人
            all_results = []
            for name in self.call_model.get_persons():
                result = self.analyze_call_frequency(self.call_model.data[self.call_model.data[name_col] == name])
                if not result.empty:
                    all_results.append(result)
            
            if not all_results:
                return pd.DataFrame()
            
            frequency_result = pd.concat(all_results, ignore_index=True)
        
        if frequency_result.empty:
            return pd.DataFrame()
        
        # 按通话次数排序
        sorted_result = frequency_result.sort_values(by='通话次数', ascending=False)
        
        # 返回前N条记录
        return sorted_result.head(top_n)
    
    def get_longest_duration_calls(self, person_name: Optional[str] = None, top_n: int = 10) -> pd.DataFrame:
        """
        获取通话时长最长的对方
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果不提供则分析所有人
        top_n : int
            返回的记录数量
            
        Returns:
        --------
        pd.DataFrame
            通话时长最长的对方记录
        """
        # 获取通话频率分析结果
        name_col = self.call_model.name_column
        if person_name:
            frequency_result = self.analyze_call_frequency(self.call_model.data[self.call_model.data[name_col] == person_name])
        else:
            # 分析所有人
            all_results = []
            for name in self.call_model.get_persons():
                result = self.analyze_call_frequency(self.call_model.data[self.call_model.data[name_col] == name])
                if not result.empty:
                    all_results.append(result)
            
            if not all_results:
                return pd.DataFrame()
            
            frequency_result = pd.concat(all_results, ignore_index=True)
        
        if frequency_result.empty:
            return pd.DataFrame()
        
        # 按通话时长排序
        sorted_result = frequency_result.sort_values(by='通话总时长', ascending=False)
        
        # 返回前N条记录
        return sorted_result.head(top_n) 