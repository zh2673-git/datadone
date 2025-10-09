#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import logging
import os
from typing import Dict, List, Union, Optional

class GroupManager:
    """
    分组管理类，负责管理人员分组
    """
    def __init__(self, group_dict: Optional[Dict[str, List[str]]] = None):
        """
        初始化分组管理器
        
        Parameters:
        -----------
        group_dict : Dict[str, List[str]], optional
            分组字典，键为分组名，值为分组成员列表
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.groups = group_dict or {}
    
    def add_group(self, group_name: str, members: List[str]) -> bool:
        """
        添加分组
        
        Parameters:
        -----------
        group_name : str
            分组名
        members : List[str]
            分组成员列表
            
        Returns:
        --------
        bool
            是否添加成功
        """
        if not group_name:
            self.logger.warning("分组名不能为空")
            return False
        
        if not members:
            self.logger.warning("分组成员不能为空")
            return False
        
        if group_name in self.groups:
            self.logger.warning(f"分组 {group_name} 已存在，将被覆盖")
        
        self.groups[group_name] = list(set(members))  # 去重
        self.logger.info(f"已添加分组 {group_name}，包含 {len(self.groups[group_name])} 个成员")
        return True
    
    def remove_group(self, group_name: str) -> bool:
        """
        删除分组
        
        Parameters:
        -----------
        group_name : str
            分组名
            
        Returns:
        --------
        bool
            是否删除成功
        """
        if group_name not in self.groups:
            self.logger.warning(f"分组 {group_name} 不存在")
            return False
        
        del self.groups[group_name]
        self.logger.info(f"已删除分组 {group_name}")
        return True
    
    def get_group(self, group_name: str) -> List[str]:
        """
        获取分组成员
        
        Parameters:
        -----------
        group_name : str
            分组名
            
        Returns:
        --------
        List[str]
            分组成员列表
        """
        if group_name not in self.groups:
            self.logger.warning(f"分组 {group_name} 不存在")
            return []
        
        return self.groups[group_name]
    
    def add_member(self, group_name: str, member: str) -> bool:
        """
        向分组添加成员
        
        Parameters:
        -----------
        group_name : str
            分组名
        member : str
            成员名
            
        Returns:
        --------
        bool
            是否添加成功
        """
        if group_name not in self.groups:
            self.logger.warning(f"分组 {group_name} 不存在")
            return False
        
        if member in self.groups[group_name]:
            self.logger.warning(f"成员 {member} 已在分组 {group_name} 中")
            return False
        
        self.groups[group_name].append(member)
        self.logger.info(f"已向分组 {group_name} 添加成员 {member}")
        return True
    
    def remove_member(self, group_name: str, member: str) -> bool:
        """
        从分组删除成员
        
        Parameters:
        -----------
        group_name : str
            分组名
        member : str
            成员名
            
        Returns:
        --------
        bool
            是否删除成功
        """
        if group_name not in self.groups:
            self.logger.warning(f"分组 {group_name} 不存在")
            return False
        
        if member not in self.groups[group_name]:
            self.logger.warning(f"成员 {member} 不在分组 {group_name} 中")
            return False
        
        self.groups[group_name].remove(member)
        self.logger.info(f"已从分组 {group_name} 删除成员 {member}")
        return True
    
    def get_all_groups(self) -> Dict[str, List[str]]:
        """
        获取所有分组
        
        Returns:
        --------
        Dict[str, List[str]]
            所有分组
        """
        return self.groups
    
    def get_group_names(self) -> List[str]:
        """
        获取所有分组名
        
        Returns:
        --------
        List[str]
            所有分组名
        """
        return list(self.groups.keys())
    
    def get_member_groups(self, member: str) -> List[str]:
        """
        获取成员所在的所有分组
        
        Parameters:
        -----------
        member : str
            成员名
            
        Returns:
        --------
        List[str]
            成员所在的所有分组名
        """
        return [group_name for group_name, members in self.groups.items() if member in members]
    
    def from_json(self, json_str: str) -> bool:
        """
        从JSON字符串加载分组
        
        Parameters:
        -----------
        json_str : str
            JSON字符串
            
        Returns:
        --------
        bool
            是否加载成功
        """
        try:
            group_dict = json.loads(json_str)
            if not isinstance(group_dict, dict):
                self.logger.warning("JSON格式错误，应为字典")
                return False
            
            self.groups = group_dict
            self.logger.info(f"已从JSON加载 {len(self.groups)} 个分组")
            return True
        except Exception as e:
            self.logger.error(f"从JSON加载分组失败: {str(e)}")
            return False
    
    def from_file(self, file_path: str) -> bool:
        """
        从文件加载分组
        
        Parameters:
        -----------
        file_path : str
            文件路径
            
        Returns:
        --------
        bool
            是否加载成功
        """
        try:
            if not os.path.exists(file_path):
                self.logger.warning(f"文件 {file_path} 不存在")
                return False
            
            if file_path.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    group_dict = json.load(f)
                    if not isinstance(group_dict, dict):
                        self.logger.warning("JSON格式错误，应为字典")
                        return False
                    
                    self.groups = group_dict
                    self.logger.info(f"已从JSON文件加载 {len(self.groups)} 个分组")
                    return True
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
                if len(df.columns) < 2:
                    self.logger.warning("CSV格式错误，至少应有两列：分组名和成员名")
                    return False
                
                group_col, member_col = df.columns[:2]
                self.groups = {}
                for _, row in df.iterrows():
                    group_name = row[group_col]
                    member = row[member_col]
                    if pd.isna(group_name) or pd.isna(member):
                        continue
                    
                    if group_name not in self.groups:
                        self.groups[group_name] = []
                    
                    self.groups[group_name].append(member)
                
                self.logger.info(f"已从CSV文件加载 {len(self.groups)} 个分组")
                return True
            elif file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                df = pd.read_excel(file_path)
                if len(df.columns) < 2:
                    self.logger.warning("Excel格式错误，至少应有两列：分组名和成员名")
                    return False
                
                group_col, member_col = df.columns[:2]
                self.groups = {}
                for _, row in df.iterrows():
                    group_name = row[group_col]
                    member = row[member_col]
                    if pd.isna(group_name) or pd.isna(member):
                        continue
                    
                    if group_name not in self.groups:
                        self.groups[group_name] = []
                    
                    self.groups[group_name].append(member)
                
                self.logger.info(f"已从Excel文件加载 {len(self.groups)} 个分组")
                return True
            else:
                self.logger.warning(f"不支持的文件格式: {file_path}")
                return False
        except Exception as e:
            self.logger.error(f"从文件加载分组失败: {str(e)}")
            return False
    
    def to_json(self) -> str:
        """
        导出为JSON字符串
        
        Returns:
        --------
        str
            JSON字符串
        """
        return json.dumps(self.groups, ensure_ascii=False, indent=4)
    
    def to_file(self, file_path: str) -> bool:
        """
        导出到文件
        
        Parameters:
        -----------
        file_path : str
            文件路径
            
        Returns:
        --------
        bool
            是否导出成功
        """
        try:
            if file_path.endswith('.json'):
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.groups, f, ensure_ascii=False, indent=4)
                
                self.logger.info(f"已导出到JSON文件 {file_path}")
                return True
            elif file_path.endswith('.csv'):
                data = []
                for group_name, members in self.groups.items():
                    for member in members:
                        data.append({'分组名': group_name, '成员名': member})
                
                df = pd.DataFrame(data)
                df.to_csv(file_path, index=False, encoding='utf-8')
                
                self.logger.info(f"已导出到CSV文件 {file_path}")
                return True
            elif file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                data = []
                for group_name, members in self.groups.items():
                    for member in members:
                        data.append({'分组名': group_name, '成员名': member})
                
                df = pd.DataFrame(data)
                df.to_excel(file_path, index=False)
                
                self.logger.info(f"已导出到Excel文件 {file_path}")
                return True
            else:
                self.logger.warning(f"不支持的文件格式: {file_path}")
                return False
        except Exception as e:
            self.logger.error(f"导出到文件失败: {str(e)}")
            return False
    
    def __str__(self) -> str:
        """
        返回分组管理器描述
        """
        return f"GroupManager: {len(self.groups)} 个分组" 