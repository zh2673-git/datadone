#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import logging
import time
import threading
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime

from src.utils.exceptions import ConfigError
from src.utils.constants import FilePath

class Config:
    """
    配置管理类，负责管理应用配置
    支持配置热加载和观察者模式的变更通知
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
        self.config_file = config_file or FilePath.CONFIG
        self.config = {}
        self.last_modified_time = 0
        
        # 配置观察者列表
        self._observers = []
        
        # 热加载相关
        self._watch_thread = None
        self._stop_watching = threading.Event()
        self._watch_interval = 5  # 检查间隔，秒
        
        # 默认配置
        self.default_config = {
            # 配置元数据
            "meta": {
                "version": "1.0.0",
                "last_updated": datetime.now().isoformat(),
                "description": "应用配置文件"
            },
            
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
                    'summary_column': '交易摘要',
                    'remark_column': '交易备注',
                    'bank_name_column': '银行类型',
                    'account_column': '本方账号',
                    'credit_flag': '贷',  # 贷方（收入）标识
                    'debit_flag': '借',   # 借方（支出）标识
                },
                'call': {
                    'name_column': '本方姓名',
                    'date_column': '呼叫日期',
                    'time_column': '呼叫时间',
                    'duration_column': '通话时长(秒)',
                    'opposite_name_column': '对方姓名',
                    'opposite_number_column': '对方号码',
                    'opposite_unit_column': '对方单位',
                    'opposite_title_column': '对方职务',
                    'call_type_column': '呼叫类型',
                    'special_date_column': '特殊日期名称',
                    'call_in_flag': '被叫',   # 被叫标识
                    'call_out_flag': '主叫',  # 主叫标识
                    'sms_flag': '短信',       # 短信标识
                },
                'wechat': {
                    'name_column': '本方姓名',
                    'account_column': '本方微信账号',
                    'date_column': '交易日期',
                    'time_column': '交易时间',
                    'amount_column': '交易金额',
                    'balance_column': '账户余额',
                    'type_column': '交易类型',
                    'direction_column': '借贷标识',
                    'opposite_name_column': '对方姓名',
                    'opposite_account_column': '对方微信账号',
                    'special_date_column': '特殊日期名称',
                    'credit_flag': '入',  # 收入标识
                    'debit_flag': '出',   # 支出标识
                },
                'alipay': {
                    'name_column': '本方姓名',
                    'date_column': '交易日期',
                    'time_column': '交易时间',
                    'amount_column': '交易金额',
                    'type_column': '交易型',
                    'direction_column': '借贷标识',
                    'opposite_name_column': '对方姓名',
                    'special_date_column': '特殊日期名称',
                    'transaction_type_column': '交易类型',
                    'remark_column': '交易备注',
                    'transaction_status_column': '交易状态',
                    'credit_flag': '收入',  # 收入标识
                    'debit_flag': '支出',   # 支出标识
                }
            },
            
            # 分析配置
            'analysis': {
                'bank': {
                    'deposit_keywords': ['存', '现金存', '柜台存', '存款', '现金存入', '存现'],
                    'withdraw_keywords': ['取', '现金取', '柜台取', 'ATM取', '取款', '现金支取', '取现'],
                    'deposit_exclude_keywords': ['转存', '存息', '利息存入'],
                },
                'special_date': {
                    'keywords': ['节日', '假期', '周末', '春节', '中秋', '元旦', '重阳', '清明', '国庆'],
                }
            },
            
            # 导出配置
            'export': {
                'default_output_dir': 'output',
                'report_template': 'templates/report_template.docx',
                'excel': {
                    'conditional_formatting': True,
                    'auto_filter': True,
                    'freeze_panes': True,
                    'default_width': 15
                },
                'word': {
                    'title_style': 'Heading 1',
                    'subtitle_style': 'Heading 2',
                    'table_style': 'Table Grid',
                    'include_toc': True
                }
            },
            
            # 应用配置
            'app': {
                'log_level': 'INFO',
                'log_file': 'logs/app.log',
                'config_auto_reload': False,
                'config_reload_interval': 60  # 秒
            }
        }
        
        # 加载配置
        self.load()
    
    def register_observer(self, observer: Callable[[Dict], None]) -> None:
        """
        注册配置变更观察者
        
        Parameters:
        -----------
        observer : Callable[[Dict], None]
            当配置变更时要调用的回调函数，接收当前配置字典作为参数
        """
        if observer not in self._observers:
            self._observers.append(observer)
            self.logger.debug(f"已注册配置观察者: {observer.__name__}")
    
    def unregister_observer(self, observer: Callable[[Dict], None]) -> None:
        """
        注销配置变更观察者
        
        Parameters:
        -----------
        observer : Callable[[Dict], None]
            要注销的观察者函数
        """
        if observer in self._observers:
            self._observers.remove(observer)
            self.logger.debug(f"已注销配置观察者: {observer.__name__}")
    
    def notify_observers(self) -> None:
        """通知所有观察者配置已更新"""
        for observer in self._observers:
            try:
                observer(self.config)
            except Exception as e:
                self.logger.error(f"通知观察者 {observer.__name__} 时出错: {str(e)}")
    
    def start_watching(self) -> None:
        """开始监控配置文件变化"""
        if self._watch_thread is not None and self._watch_thread.is_alive():
            self.logger.warning("配置监控线程已在运行")
            return
        
        self._stop_watching.clear()
        self._watch_thread = threading.Thread(target=self._watch_config_file, daemon=True)
        self._watch_thread.start()
        self.logger.info(f"开始监控配置文件 {self.config_file}")
    
    def stop_watching(self) -> None:
        """停止监控配置文件变化"""
        if self._watch_thread is None or not self._watch_thread.is_alive():
            self.logger.warning("配置监控线程未在运行")
            return
        
        self._stop_watching.set()
        self._watch_thread.join(timeout=1.0)
        self.logger.info("停止监控配置文件")
    
    def _watch_config_file(self) -> None:
        """监控配置文件变化的线程函数"""
        while not self._stop_watching.is_set():
            try:
                if os.path.exists(self.config_file):
                    mtime = os.path.getmtime(self.config_file)
                    if mtime > self.last_modified_time:
                        self.logger.info(f"检测到配置文件 {self.config_file} 已更改，重新加载")
                        self.load()
                        self.notify_observers()
                        self.last_modified_time = mtime
            except Exception as e:
                self.logger.error(f"监控配置文件时出错: {str(e)}")
            
            # 等待指定时间间隔，或直到收到停止信号
            self._stop_watching.wait(self._watch_interval)
    
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
                self.last_modified_time = os.path.getmtime(self.config_file)
                
                # 自动升级旧版配置
                if "meta" not in self.config or "version" not in self.config["meta"]:
                    self._upgrade_config()
                
                # 检查是否应该自动监控配置变更
                if self.get('app.config_auto_reload', False):
                    self._watch_interval = self.get('app.config_reload_interval', 60)
                    self.start_watching()
                    
                return True
            else:
                self.logger.warning(f"配置文件 {self.config_file} 不存在，使用默认配置")
                self.config = self.default_config
                # 尝试创建默认配置文件
                self.save()
                return False
        except Exception as e:
            self.logger.error(f"加载配置失败: {str(e)}")
            self.config = self.default_config.copy()
            return False
    
    def _upgrade_config(self) -> None:
        """升级老版本配置到最新版本，填充缺失的字段"""
        self.logger.info("检测到旧版本配置文件，正在升级...")
        
        # 深度合并，保留用户自定义的值
        updated_config = self._deep_merge(self.default_config, self.config)
        
        # 更新元数据
        updated_config["meta"] = self.default_config["meta"].copy()
        updated_config["meta"]["last_updated"] = datetime.now().isoformat()
        
        self.config = updated_config
        self.save()
        self.logger.info("配置升级完成")
    
    def _deep_merge(self, default_dict: Dict, user_dict: Dict) -> Dict:
        """
        深度合并两个字典，保留用户字典中的值
        
        Parameters:
        -----------
        default_dict : Dict
            默认字典
        user_dict : Dict
            用户字典
            
        Returns:
        --------
        Dict
            合并后的字典
        """
        result = default_dict.copy()
        
        for key, value in user_dict.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def save(self) -> bool:
        """
        保存配置
        
        Returns:
        --------
        bool
            是否保存成功
        """
        try:
            # 更新最后修改时间
            if "meta" in self.config:
                self.config["meta"]["last_updated"] = datetime.now().isoformat()
            
            # 确保目录存在
            config_dir = os.path.dirname(self.config_file)
            if config_dir and not os.path.exists(config_dir):
                os.makedirs(config_dir)
                
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=4)
            
            self.last_modified_time = os.path.getmtime(self.config_file)
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
    
    def set(self, key: str, value: Any, save_to_file: bool = False) -> None:
        """
        设置配置项
        
        Parameters:
        -----------
        key : str
            配置项键，支持点分隔的嵌套键，如 'data_sources.bank.name_column'
        value : Any
            配置项值
        save_to_file : bool, optional
            是否立即保存到文件，默认False
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
        
        if save_to_file:
            self.save()
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        获取配置的整个部分
        
        Parameters:
        -----------
        section : str
            配置部分名称，如 'data_sources.bank'
            
        Returns:
        --------
        Dict[str, Any]
            配置部分，如果不存在则返回空字典
        """
        section_data = self.get(section, {})
        if not isinstance(section_data, dict):
            return {}
        return section_data
    
    def reset(self) -> None:
        """
        重置为默认配置
        """
        self.config = self.default_config.copy()
        self.logger.info("已重置为默认配置")
        
        # 通知观察者
        self.notify_observers()
    
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
        
    def get_all(self) -> Dict[str, Any]:
        """
        获取所有配置
        
        Returns:
        --------
        Dict[str, Any]
            所有配置
        """
        return self.config.copy() 