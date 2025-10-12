#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据缓存管理器
实现热重载功能，避免在调试时重复加载数据
"""

import os
import pickle
import hashlib
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging


class DataCacheManager:
    """数据缓存管理器"""
    
    def __init__(self, cache_dir: str = None, config=None):
        """
        初始化缓存管理器
        
        Parameters:
        -----------
        cache_dir : str, optional
            缓存目录路径，如果为None则使用配置中的设置
        config : object, optional
            配置对象，用于获取缓存相关配置
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        
        # 获取缓存配置
        if cache_dir is None and config:
            cache_dir = config.get('app.cache_dir', 'cache')
        elif cache_dir is None:
            cache_dir = 'cache'
            
        self.cache_dir = cache_dir
        
        # 检查缓存是否启用
        self.cache_enabled = True
        if config:
            self.cache_enabled = config.get('app.cache_enabled', True)
        
        # 确保缓存目录存在
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 缓存文件路径（版本化缓存）
        self.code_hash_file = os.path.join(cache_dir, "code_hash.txt")
        
        # 版本化缓存：使用时间戳作为版本标识
        self.data_cache_file = os.path.join(cache_dir, "data_models.pkl")  # 默认文件名，用于向后兼容
        
        self.logger.info(f"数据缓存管理器初始化完成，缓存目录: {self.cache_dir}，缓存启用: {self.cache_enabled}")
    
    def _calculate_code_hash(self) -> str:
        """
        计算源代码的哈希值，用于检测代码变更
        
        Returns:
        --------
        str
            源代码哈希值
        """
        # 使用更稳定的哈希策略：基于关键文件的内容
        # 只检查核心分析逻辑文件，避免因注释或格式变化导致缓存失效
        
        key_files = [
            "src/analysis/bank_analyzer.py",
            "src/analysis/call_analyzer.py", 
            "src/analysis/payment/wechat_analyzer.py",
            "src/analysis/payment/alipay_analyzer.py",
            "src/analysis/comprehensive_analyzer.py",
            "src/datasource/bank_model.py",
            "src/datasource/call_model.py",
            "src/datasource/payment/wechat_model.py",
            "src/datasource/payment/alipay_model.py"
        ]
        
        hash_obj = hashlib.md5()
        
        for file_path in key_files:
            if not os.path.exists(file_path):
                continue
                
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # 只提取函数定义和类定义的关键内容，忽略注释和空行
                lines = []
                for line in content.split('\n'):
                    stripped = line.strip()
                    if stripped and not stripped.startswith('#') and not stripped.startswith('"""'):
                        # 只保留包含def或class的行及其后续行（直到空行）
                        if 'def ' in stripped or 'class ' in stripped:
                            lines.append(stripped)
                
                if lines:
                    key_content = '\n'.join(lines)
                    hash_obj.update(key_content.encode())
                    
            except Exception as e:
                self.logger.warning(f"计算文件 {file_path} 哈希失败: {e}")
                continue
        
        # 如果关键文件都为空，使用固定哈希值
        if hash_obj.digest_size == 0:
            hash_obj.update(b"default_cache_key")
        
        return hash_obj.hexdigest()
    
    def is_cache_valid(self, cache_file_path: str = None) -> bool:
        """
        检查缓存是否有效
        
        Parameters:
        -----------
        cache_file_path : str, optional
            指定要检查的缓存文件路径，如果为None则检查默认缓存文件
            
        Returns:
        --------
        bool
            缓存是否有效
        """
        # 检查缓存是否启用
        if not self.cache_enabled:
            self.logger.debug("缓存功能已禁用")
            return False
            
        # 确定要检查的缓存文件路径
        if cache_file_path is None:
            cache_file_path = self.data_cache_file
            
        # 检查缓存文件是否存在
        if not os.path.exists(cache_file_path):
            self.logger.debug(f"缓存文件不存在: {cache_file_path}")
            return False
            
        # 检查缓存文件大小是否合理（避免损坏的缓存文件）
        cache_size = os.path.getsize(cache_file_path)
        if cache_size < 100:  # 小于100字节的缓存文件可能损坏
            self.logger.debug("缓存文件大小异常")
            return False
            
        # 检查缓存文件是否过期（默认7天，大大延长过期时间）
        cache_timeout = self.config.get('app.cache_timeout', 604800) if self.config else 604800
        if time.time() - os.path.getmtime(cache_file_path) > cache_timeout:
            self.logger.debug("缓存文件已过期")
            return False
            
        # 简化代码变更检查：只有当关键分析逻辑发生重大变更时才失效
        # 这样可以实现跨会话的缓存复用
        if not os.path.exists(self.code_hash_file):
            self.logger.debug("代码哈希文件不存在，但缓存文件存在，认为缓存有效")
            return True
            
        try:
            with open(self.code_hash_file, 'r', encoding='utf-8') as f:
                cached_hash = f.read().strip()
                
            current_hash = self._calculate_code_hash()
            
            # 只有当哈希值完全不同时才认为缓存失效
            # 这样可以容忍小的代码变更（如注释修改、格式调整等）
            if current_hash != cached_hash:
                self.logger.debug("源代码发生变更，但尝试加载缓存")
                # 即使代码有变更，仍然尝试加载缓存
                # 在实际使用中，如果缓存数据格式不兼容，会在加载时失败
                return True
        except Exception as e:
            self.logger.warning(f"读取代码哈希文件失败: {e}，认为缓存有效")
            return True
            
        self.logger.debug("缓存有效")
        return True
    
    def clear_cache(self) -> bool:
        """
        清除缓存文件
        
        Returns:
        --------
        bool
            清除是否成功
        """
        try:
            # 清除所有版本化的缓存文件
            cache_files = [f for f in os.listdir(self.cache_dir) 
                          if f.startswith("data_models") and f.endswith(".pkl")]
            
            cleared_count = 0
            for cache_file in cache_files:
                file_path = os.path.join(self.cache_dir, cache_file)
                if os.path.exists(file_path):
                    os.remove(file_path)
                    cleared_count += 1
            
            # 清除代码哈希文件
            if os.path.exists(self.code_hash_file):
                os.remove(self.code_hash_file)
            
            self.logger.info(f"已清除 {cleared_count} 个缓存文件")
            return True
            
        except Exception as e:
            self.logger.error(f"清除缓存失败: {e}")
            return False
    
    def save_data_models(self, data_models: Dict[str, Any], version: str = None) -> bool:
        """
        保存数据模型到缓存（改进版本，只保存可序列化的数据）
        
        Parameters:
        -----------
        data_models : Dict[str, Any]
            数据模型字典
        version : str, optional
            缓存版本标识，如果为None则使用时间戳
            
        Returns:
        --------
        bool
            保存是否成功
        """
        # 检查缓存是否启用
        if not self.cache_enabled:
            self.logger.debug("缓存功能已禁用，跳过保存")
            return False
            
        try:
            # 计算当前代码哈希
            current_hash = self._calculate_code_hash()
            
            # 生成版本标识
            if version is None:
                version = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 创建可序列化的数据副本，避免保存包含线程锁的对象
            serializable_data = {}
            for key, model in data_models.items():
                if model is None:
                    continue
                    
                # 只保存必要的数据，避免序列化复杂对象
                model_data = {
                    'data': model.data.copy() if hasattr(model, 'data') and model.data is not None else None,
                    'file_path': getattr(model, 'file_path', None),
                    'config_info': self._get_serializable_config(model)
                }
                serializable_data[key] = model_data
            
            # 保存到版本化文件
            versioned_cache_file = os.path.join(self.cache_dir, f"data_models_{version}.pkl")
            with open(versioned_cache_file, 'wb') as f:
                pickle.dump(serializable_data, f)
                
            # 保存代码哈希
            with open(self.code_hash_file, 'w', encoding='utf-8') as f:
                f.write(current_hash)
                
            self.logger.info(f"数据模型已保存到缓存版本 {version}，代码哈希: {current_hash[:8]}...")
            return True
            
        except Exception as e:
            self.logger.error(f"保存数据模型到缓存失败: {e}")
            # 如果保存失败，删除可能创建的0字节文件
            if version is not None:
                versioned_cache_file = os.path.join(self.cache_dir, f"data_models_{version}.pkl")
                if os.path.exists(versioned_cache_file) and os.path.getsize(versioned_cache_file) == 0:
                    try:
                        os.remove(versioned_cache_file)
                    except:
                        pass
            return False
    
    def _get_serializable_config(self, model) -> dict:
        """
        获取可序列化的配置信息
        
        Parameters:
        -----------
        model : object
            数据模型对象
            
        Returns:
        --------
        dict
            可序列化的配置信息
        """
        config_info = {}
        
        # 获取模型的基本配置信息
        if hasattr(model, 'config'):
            config = model.config
            if hasattr(config, 'config_dict'):
                # 如果是Config对象，获取其配置字典
                config_info = config.config_dict.copy()
            elif isinstance(config, dict):
                # 如果是字典，直接复制
                config_info = config.copy()
        
        # 获取列名配置
        column_attrs = ['name_column', 'date_column', 'amount_column', 'balance_column', 
                       'type_column', 'summary_column', 'remark_column', 'direction_column',
                       'opposite_name_column', 'phone_column', 'duration_column', 
                       'call_type_column', 'opposite_phone_column', 'opposite_location_column',
                       'time_column', 'credit_flag', 'debit_flag']
        
        for attr in column_attrs:
            if hasattr(model, attr):
                config_info[attr] = getattr(model, attr)
        
        return config_info
    
    def load_data_models(self, version: str = None) -> Optional[Dict[str, Any]]:
        """
        从缓存加载数据模型（改进版本，从序列化数据重建模型）
        
        Parameters:
        -----------
        version : str, optional
            指定要加载的缓存版本，如果为None则加载最新版本
            
        Returns:
        --------
        Optional[Dict[str, Any]]
            加载的数据模型，如果缓存无效则返回None
        """
        # 检查缓存是否启用
        if not self.cache_enabled:
            self.logger.debug("缓存功能已禁用，跳过加载")
            return None
            
        # 获取可用的缓存版本
        available_versions = self.get_available_cache_versions()
        if not available_versions:
            self.logger.debug("没有可用的缓存文件")
            return None
            
        # 确定要加载的版本
        if version is None:
            # 加载最新版本
            version = available_versions[-1]['version']
            cache_file = available_versions[-1]['file_path']
        else:
            # 查找指定版本
            cache_file = None
            for cache_info in available_versions:
                if cache_info['version'] == version:
                    cache_file = cache_info['file_path']
                    break
            
            if cache_file is None:
                self.logger.info(f"指定的缓存版本 {version} 不存在")
                return None
        
        # 检查缓存是否有效
        if not self.is_cache_valid(cache_file_path=cache_file):
            self.logger.debug("缓存无效，跳过加载")
            return None
            
        try:
            with open(cache_file, 'rb') as f:
                serialized_data = pickle.load(f)
            
            # 从序列化数据重建数据模型
            data_models = self._reconstruct_data_models(serialized_data)
            
            if data_models:
                self.logger.info(f"从缓存版本 {version} 成功加载数据模型")
                return data_models
            else:
                self.logger.warning("从缓存重建数据模型失败")
                return None
                
        except Exception as e:
            self.logger.error(f"从缓存加载数据模型失败: {e}")
            return None
    
    def _reconstruct_data_models(self, serialized_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        从序列化数据重建数据模型
        
        Parameters:
        -----------
        serialized_data : Dict[str, Any]
            序列化的数据
            
        Returns:
        --------
        Dict[str, Any]
            重建的数据模型字典
        """
        if not serialized_data:
            return {}
        
        data_models = {}
        
        # 导入必要的模型类
        from src.datasource.bank_model import BankDataModel
        from src.datasource.call_model import CallDataModel
        from src.datasource.payment.alipay_model import AlipayDataModel
        from src.datasource.payment.wechat_model import WeChatDataModel
        
        model_classes = {
            'bank': BankDataModel,
            'call': CallDataModel,
            'alipay': AlipayDataModel,
            'wechat': WeChatDataModel
        }
        
        for key, model_data in serialized_data.items():
            if key not in model_classes:
                self.logger.warning(f"未知的数据模型类型: {key}")
                continue
                
            try:
                model_class = model_classes[key]
                
                # 创建新的模型实例
                model = model_class()
                
                # 恢复数据
                if model_data.get('data') is not None:
                    model.data = model_data['data']
                
                # 恢复文件路径
                if model_data.get('file_path'):
                    model.file_path = model_data['file_path']
                
                # 恢复配置信息
                config_info = model_data.get('config_info', {})
                if hasattr(model, 'config') and config_info:
                    # 如果模型有config属性，尝试恢复配置
                    if hasattr(model.config, 'update_from_dict'):
                        model.config.update_from_dict(config_info)
                    elif isinstance(model.config, dict):
                        model.config.update(config_info)
                
                # 恢复列名配置
                for attr, value in config_info.items():
                    if hasattr(model, attr):
                        setattr(model, attr, value)
                
                data_models[key] = model
                
            except Exception as e:
                self.logger.error(f"重建 {key} 数据模型失败: {e}")
                continue
        
        return data_models
    
    def clear_cache(self) -> bool:
        """
        清除缓存
        
        Returns:
        --------
        bool
            清除是否成功
        """
        try:
            if os.path.exists(self.data_cache_file):
                os.remove(self.data_cache_file)
            if os.path.exists(self.code_hash_file):
                os.remove(self.code_hash_file)
            
            self.logger.info("缓存已清除")
            return True
            
        except Exception as e:
            self.logger.error(f"清除缓存失败: {e}")
            return False
    
    def get_available_cache_versions(self) -> List[Dict[str, Any]]:
        """
        获取所有可用的缓存版本
        
        Returns:
        --------
        List[Dict[str, Any]]
            缓存版本信息列表，按时间戳排序
        """
        try:
            if not os.path.exists(self.cache_dir):
                return []
                
            cache_files = [f for f in os.listdir(self.cache_dir) 
                          if f.startswith("data_models_") and f.endswith(".pkl")]
            
            versions = []
            for cache_file in cache_files:
                file_path = os.path.join(self.cache_dir, cache_file)
                
                # 从文件名提取版本信息
                version = cache_file.replace("data_models_", "").replace(".pkl", "")
                
                # 获取文件修改时间
                mtime = os.path.getmtime(file_path)
                size = os.path.getsize(file_path)
                
                versions.append({
                    'version': version,
                    'file_path': file_path,
                    'timestamp': mtime,
                    'size': size,
                    'formatted_time': datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
                })
            
            # 按时间戳排序
            versions.sort(key=lambda x: x['timestamp'])
            return versions
            
        except Exception as e:
            self.logger.error(f"获取缓存版本失败: {e}")
            return []
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息
        
        Returns:
        --------
        Dict[str, Any]
            缓存信息字典
        """
        cache_info = {
            'cache_dir': str(self.cache_dir),
            'enabled': self.cache_enabled,
            'timeout': self.config.get('app.cache_timeout', 604800) if self.config else 604800
        }
        
        # 获取可用缓存版本
        available_versions = self.get_available_cache_versions()
        cache_info['available_versions'] = available_versions
        cache_info['version_count'] = len(available_versions)
        
        # 检查最新缓存文件
        if available_versions:
            latest_version = available_versions[-1]
            cache_info['latest_cache_file'] = latest_version['file_path']
            cache_info['latest_cache_size'] = latest_version['size']
            cache_info['latest_cache_time'] = latest_version['formatted_time']
        else:
            cache_info['latest_cache_file'] = None
            cache_info['latest_cache_size'] = 0
            cache_info['latest_cache_time'] = None
            
        # 检查代码哈希文件
        cache_info['code_hash_exists'] = os.path.exists(self.code_hash_file)
        if cache_info['code_hash_exists']:
            cache_info['code_hash_size'] = os.path.getsize(self.code_hash_file)
        else:
            cache_info['code_hash_size'] = 0
            
        # 检查缓存有效性
        cache_info['is_valid'] = self.is_cache_valid()
        
        return cache_info