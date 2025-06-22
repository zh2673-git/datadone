#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import logging
from typing import Dict, Any, Optional

class Config:
    """
    配置管理类，负责管理应用配置
    """
    def __init__(self, config_file: Optional[str] = None):
        """
        初始化配置管理器
        
        Parameters:
        -----------
        config_file : str, optional
            配置文件路径，如果不提供则使用默认路径
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config_file = config_file or 'config.json'
        self.config = {}
        
        # 默认配置
        self.default_config = {
            # 数据源配置
            'data_sources': {
                'bank': {
                    'name_column': '本方姓名',
                    'date_column': '交易日期',
                    'amount_column': '交易金额',
                    'balance_column': '账户余额',
                    'type_column': '交易类型',
                    'direction_column': '借贷标识',
                    'opposite_name_column': '对方姓名',
                    'special_date_column': '特殊日期名称',
                    'credit_flag': '借',  # 借入（收入）标识
                    'debit_flag': '贷',   # 贷出（支出）标识
                },
                'call': {
                    'name_column': '本方姓名',
                    'date_column': '呼叫日期',
                    'duration_column': '通话时长(秒)',
                    'opposite_name_column': '对方姓名',
                    'opposite_number_column': '对方号码',
                    'call_type_column': '呼叫类型',
                    'special_date_column': '特殊日期名称',
                },
                'wechat': {
                    'name_column': '本方姓名',
                    'date_column': '交易日期',
                    'amount_column': '交易金额',
                    'balance_column': '账户余额',
                    'direction_column': '借贷标识',
                    'opposite_name_column': '对方姓名',
                    'special_date_column': '特殊日期名称',
                    'credit_flag': '收入',  # 收入标识
                    'debit_flag': '支出',   # 支出标识
                },
                'alipay': {
                    'name_column': '本方姓名',
                    'date_column': '交易日期',
                    'amount_column': '交易金额',
                    'direction_column': '借贷标识',
                    'opposite_name_column': '对方姓名',
                    'special_date_column': '特殊日期名称',
                    'credit_flag': '收入',  # 收入标识
                    'debit_flag': '支出',   # 支出标识
                }
            },
            
            # 分析配置
            'analysis': {
                'bank': {
                    'deposit_keywords': ['存', '现金存', '柜台存'],
                    'withdraw_keywords': ['取', '现金取', '柜台取', 'ATM取'],
                },
                'special_date': {
                    'keywords': ['节日', '假期', '周末'],
                }
            },
            
            # 导出配置
            'export': {
                'default_output_dir': 'output',
                'report_template': 'templates/report_template.docx',
            },
            
            # 应用配置
            'app': {
                'log_level': 'INFO',
                'log_file': 'logs/app.log',
            }
        }
        
        # 加载配置
        self.load()
    
    def load(self) -> bool:
        """
        加载配置
        
        Returns:
        --------
        bool
            是否加载成功
        """
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
                self.logger.info(f"已从 {self.config_file} 加载配置")
                return True
            else:
                self.logger.warning(f"配置文件 {self.config_file} 不存在，使用默认配置")
                self.config = self.default_config
                return False
        except Exception as e:
            self.logger.error(f"加载配置失败: {str(e)}")
            self.config = self.default_config
            return False
    
    def save(self) -> bool:
        """
        保存配置
        
        Returns:
        --------
        bool
            是否保存成功
        """
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=4)
            self.logger.info(f"已将配置保存到 {self.config_file}")
            return True
        except Exception as e:
            self.logger.error(f"保存配置失败: {str(e)}")
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置项
        
        Parameters:
        -----------
        key : str
            配置项键，支持点分隔的嵌套键，如 'data_sources.bank.name_column'
        default : Any, optional
            默认值，如果配置项不存在则返回此值
            
        Returns:
        --------
        Any
            配置项值
        """
        # 处理嵌套键
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        设置配置项
        
        Parameters:
        -----------
        key : str
            配置项键，支持点分隔的嵌套键，如 'data_sources.bank.name_column'
        value : Any
            配置项值
        """
        # 处理嵌套键
        keys = key.split('.')
        config = self.config
        
        # 处理中间键
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            elif not isinstance(config[k], dict):
                config[k] = {}
            
            config = config[k]
        
        # 设置最后一个键的值
        config[keys[-1]] = value
        self.logger.debug(f"已设置配置项 {key} = {value}")
    
    def reset(self) -> None:
        """
        重置为默认配置
        """
        self.config = self.default_config.copy()
        self.logger.info("已重置为默认配置")
    
    def __getitem__(self, key: str) -> Any:
        """
        通过字典语法获取配置项
        """
        return self.get(key)
    
    def __setitem__(self, key: str, value: Any) -> None:
        """
        通过字典语法设置配置项
        """
        self.set(key, value)
    
    def __contains__(self, key: str) -> bool:
        """
        检查配置项是否存在
        """
        return self.get(key) is not None 