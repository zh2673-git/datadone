#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
from typing import Dict, List, Optional, Union, Any, Tuple
import pandas as pd

from src.utils.logger import setup_logger

class BaseInterface:
    """
    交互界面基类，定义与用户交互的基本方法
    """
    def __init__(self, logger=None):
        """
        初始化交互界面
        
        Parameters:
        -----------
        logger : logging.Logger, optional
            日志记录器，如果不提供则创建一个新的
        """
        # 设置日志
        self.logger = logger or setup_logger(self.__class__.__name__)
    
    def start(self):
        """
        启动交互界面
        """
        self.logger.info("启动交互界面")
        self.display_welcome()
    
    def display_welcome(self):
        """
        显示欢迎信息
        """
        print("="*80)
        print("                  多源数据分析系统                  ")
        print("="*80)
        print("本系统支持银行、话单、微信、支付宝等多源数据的分析")
        print("功能包括：用户分组、频率分析、存取现分析、综合分析等")
        print("="*80)
    
    def display_menu(self, options: List[str], title: str = "菜单") -> int:
        """
        显示菜单并获取用户选择
        
        Parameters:
        -----------
        options : List[str]
            菜单选项列表
        title : str, optional
            菜单标题
            
        Returns:
        --------
        int
            用户选择的选项索引(0-based)
        """
        print(f"\n{title}")
        print("-" * len(title))
        
        for i, option in enumerate(options, 1):
            print(f"{i}. {option}")
        
        # 自动添加返回选项
        if title != "主菜单":
            print(f"{len(options) + 1}. 返回")
        
        while True:
            try:
                choice = int(input("\n请输入选项编号: "))
                if 1 <= choice <= len(options):
                    return choice - 1  # 转换为0-based索引
                elif title != "主菜单" and choice == len(options) + 1:
                    return -1  # 返回上级菜单
                else:
                    max_option = len(options) + (1 if title != "主菜单" else 0)
                    print(f"无效的选项，请输入1-{max_option}之间的数字")
            except ValueError:
                print("请输入有效的数字")
    
    def get_input(self, prompt: str, default: Optional[str] = None, 
                 validator: Optional[callable] = None) -> str:
        """
        获取用户输入
        
        Parameters:
        -----------
        prompt : str
            提示信息
        default : str, optional
            默认值
        validator : callable, optional
            验证函数，如果提供则用于验证输入值
            
        Returns:
        --------
        str
            用户输入
        """
        # 如果有默认值，则显示在提示中
        if default is not None:
            prompt = f"{prompt} [{default}]: "
        else:
            prompt = f"{prompt}: "
        
        while True:
            value = input(prompt)
            
            # 如果输入为空且有默认值，则使用默认值
            if not value and default is not None:
                value = default
            
            # 如果有验证函数，则验证输入
            if validator is not None:
                try:
                    if validator(value):
                        return value
                    else:
                        print("输入无效，请重新输入")
                except Exception as e:
                    print(f"验证失败: {str(e)}")
            else:
                return value
    
    def get_path_input(self, prompt: str, must_exist: bool = True, 
                      is_dir: bool = False, default: Optional[str] = None) -> str:
        """
        获取文件或目录路径输入
        
        Parameters:
        -----------
        prompt : str
            提示信息
        must_exist : bool, optional
            路径是否必须存在
        is_dir : bool, optional
            是否为目录路径
        default : str, optional
            默认路径
            
        Returns:
        --------
        str
            用户输入的路径
        """
        def validate_path(path):
            # 验证路径是否存在
            if must_exist and not os.path.exists(path):
                print(f"路径不存在: {path}")
                return False
            
            # 验证是否为目录
            if must_exist and is_dir and not os.path.isdir(path):
                print(f"不是有效的目录: {path}")
                return False
            
            # 验证是否为文件
            if must_exist and not is_dir and not os.path.isfile(path):
                print(f"不是有效的文件: {path}")
                return False
            
            return True
        
        return self.get_input(prompt, default, validate_path)
    
    def get_multiple_choice(self, options: List[str], prompt: str = "请选择", 
                           multiple: bool = False, default_all: bool = False) -> Union[int, List[int]]:
        """
        获取用户的多选或单选输入
        
        Parameters:
        -----------
        options : List[str]
            选项列表
        prompt : str, optional
            提示信息
        multiple : bool, optional
            是否为多选
        default_all : bool, optional
            如果为True且multiple=True，则默认选择所有选项
            
        Returns:
        --------
        Union[int, List[int]]
            用户选择的选项索引(0-based)，如果multiple=True则返回索引列表
        """
        print(f"\n{prompt}")
        
        for i, option in enumerate(options, 1):
            print(f"{i}. {option}")
        
        if multiple:
            if default_all:
                print("\n可以选择多个选项，用逗号分隔，例如 1,3,5。直接回车选择全部。")
            else:
                print("\n可以选择多个选项，用逗号分隔，例如 1,3,5")
            
            while True:
                try:
                    selections = input("请输入选项编号: ")
                    
                    # 如果输入为空
                    if not selections.strip():
                        if default_all:
                            return list(range(len(options)))  # 选择所有选项
                        else:
                            return []
                    
                    # 解析选择
                    indices = [int(s.strip()) - 1 for s in selections.split(",")]
                    
                    # 验证选择
                    if all(0 <= idx < len(options) for idx in indices):
                        return indices
                    else:
                        print(f"无效的选项，请输入1-{len(options)}之间的数字")
                except ValueError:
                    print("请输入有效的数字，用逗号分隔")
        else:
            while True:
                try:
                    choice = int(input("请输入选项编号: "))
                    if 1 <= choice <= len(options):
                        return choice - 1  # 转换为0-based索引
                    else:
                        print(f"无效的选项，请输入1-{len(options)}之间的数字")
                except ValueError:
                    print("请输入有效的数字")
    
    def display_data(self, data: pd.DataFrame, title: str = "数据预览", 
                    max_rows: int = 10, max_cols: int = None):
        """
        显示数据预览
        
        Parameters:
        -----------
        data : pd.DataFrame
            要显示的数据
        title : str, optional
            标题
        max_rows : int, optional
            最大显示行数
        max_cols : int, optional
            最大显示列数
        """
        print(f"\n{title}")
        print("-" * len(title))
        
        if data is None or data.empty:
            print("没有数据可供显示")
            return
        
        # 保存原始显示设置
        original_max_rows = pd.get_option('display.max_rows')
        original_max_columns = pd.get_option('display.max_columns')
        original_width = pd.get_option('display.width')
        
        # 设置临时显示设置
        pd.set_option('display.max_rows', max_rows)
        if max_cols:
            pd.set_option('display.max_columns', max_cols)
        pd.set_option('display.width', 120)  # 增加宽度以显示更多列
        
        try:
            # 显示数据
            if len(data) > max_rows:
                print(f"显示前 {max_rows} 行数据（共 {len(data)} 行）：")
            
            print(data.head(max_rows))
            
            # 显示列数统计
            print(f"\n列数: {len(data.columns)}")
            
            # 显示数值列的统计信息
            numeric_cols = data.select_dtypes(include=['number']).columns
            if not numeric_cols.empty:
                print("\n数值列统计:")
                print(data[numeric_cols].describe().round(2))
        finally:
            # 恢复原始显示设置
            pd.set_option('display.max_rows', original_max_rows)
            pd.set_option('display.max_columns', original_max_columns)
            pd.set_option('display.width', original_width)
    
    def confirm(self, prompt: str = "是否继续？") -> bool:
        """
        获取用户确认
        
        Parameters:
        -----------
        prompt : str, optional
            提示信息
            
        Returns:
        --------
        bool
            用户是否确认
        """
        while True:
            response = input(f"{prompt} (y/n): ").strip().lower()
            if response == 'y' or response == 'yes':
                return True
            elif response == 'n' or response == 'no':
                return False
            else:
                print("请输入 y/yes 或 n/no")
    
    def display_error(self, message: str):
        """
        显示错误信息
        
        Parameters:
        -----------
        message : str
            错误信息
        """
        print(f"\n错误: {message}")
        self.logger.error(message)
    
    def display_success(self, message: str):
        """
        显示成功信息
        
        Parameters:
        -----------
        message : str
            成功信息
        """
        print(f"\n成功: {message}")
        self.logger.info(message)
    
    def display_warning(self, message: str):
        """
        显示警告信息
        
        Parameters:
        -----------
        message : str
            警告信息
        """
        print(f"\n警告: {message}")
        self.logger.warning(message)
    
    def display_info(self, message: str):
        """
        显示普通信息
        
        Parameters:
        -----------
        message : str
            信息内容
        """
        print(f"\n信息: {message}")
        self.logger.info(message)

    def display_message(self, message: str):
        """
        显示普通信息
        """
        print(f"\n{message}")

    def get_yes_no_input(self, prompt: str) -> bool:
        """
        获取用户的是/否输入
        
        Parameters:
        -----------
        prompt : str
            提示信息
            
        Returns:
        --------
        bool
            如果用户输入'y'或'Y'则返回True，否则返回False
        """
        while True:
            choice = input(f"{prompt} (y/n): ").lower()
            if choice == 'y':
                return True
            elif choice == 'n':
                return False
            else:
                print("无效输入，请输入 'y' 或 'n'") 