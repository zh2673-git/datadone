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
from typing import Dict, Any, Optional
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
        
        # 缓存文件路径
        self.data_cache_file = os.path.join(cache_dir, "data_models.pkl")
        self.code_hash_file = os.path.join(cache_dir, "code_hash.txt")
        
        self.logger.info(f"数据缓存管理器初始化完成，缓存目录: {self.cache_dir}，缓存启用: {self.cache_enabled}")
    
    def _calculate_code_hash(self) -> str:
        """
        计算源代码的哈希值，用于检测代码变更
        
        Returns:
        --------
        str
            源代码哈希值
        """
        src_dir = "src"
        if not os.path.exists(src_dir):
            return ""
        
        # 收集所有Python文件的修改时间
        python_files = []
        for root, dirs, files in os.walk(src_dir):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    python_files.append(file_path)
        
        # 按修改时间排序
        python_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # 取最近修改的10个文件计算哈希
        recent_files = python_files[:10]
        
        if not recent_files:
            return ""
        
        # 计算哈希值
        hash_obj = hashlib.md5()
        for file_path in recent_files:
            # 使用文件路径和修改时间
            mtime = os.path.getmtime(file_path)
            hash_obj.update(f"{file_path}:{mtime}".encode())
        
        return hash_obj.hexdigest()
    
    def is_cache_valid(self) -> bool:
        """
        检查缓存是否有效
        
        Returns:
        --------
        bool
            缓存是否有效
        """
        # 检查缓存是否启用
        if not self.cache_enabled:
            self.logger.debug("缓存功能已禁用")
            return False
            
        # 检查缓存文件是否存在
        if not os.path.exists(self.data_cache_file):
            self.logger.debug("缓存文件不存在")
            return False
            
        # 检查缓存文件是否过期（默认1小时）
        cache_timeout = self.config.get('app.cache_timeout', 3600) if self.config else 3600
        if time.time() - os.path.getmtime(self.data_cache_file) > cache_timeout:
            self.logger.debug("缓存文件已过期")
            return False
            
        # 检查源代码是否发生变更
        current_hash = self._calculate_code_hash()
        if not os.path.exists(self.code_hash_file):
            self.logger.debug("代码哈希文件不存在")
            return False
            
        with open(self.code_hash_file, 'r', encoding='utf-8') as f:
            cached_hash = f.read().strip()
            
        if current_hash != cached_hash:
            self.logger.debug("源代码发生变更，缓存失效")
            return False
            
        self.logger.debug("缓存有效")
        return True
    
    def save_data_models(self, data_models: Dict[str, Any]) -> bool:
        """
        保存数据模型到缓存
        
        Parameters:
        -----------
        data_models : Dict[str, Any]
            数据模型字典
            
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
            
            # 保存数据模型
            with open(self.data_cache_file, 'wb') as f:
                pickle.dump(data_models, f)
                
            # 保存代码哈希
            with open(self.code_hash_file, 'w', encoding='utf-8') as f:
                f.write(current_hash)
                
            self.logger.info(f"数据模型已保存到缓存，代码哈希: {current_hash[:8]}...")
            return True
            
        except Exception as e:
            self.logger.error(f"保存数据模型到缓存失败: {e}")
            return False
    
    def load_data_models(self) -> Optional[Dict[str, Any]]:
        """
        从缓存加载数据模型
        
        Returns:
        --------
        Optional[Dict[str, Any]]
            加载的数据模型，如果缓存无效则返回None
        """
        # 检查缓存是否启用
        if not self.cache_enabled:
            self.logger.debug("缓存功能已禁用，跳过加载")
            return None
            
        if not self.is_cache_valid():
            self.logger.debug("缓存无效，跳过加载")
            return None
            
        try:
            with open(self.data_cache_file, 'rb') as f:
                data_models = pickle.load(f)
                
            self.logger.info("从缓存成功加载数据模型")
            return data_models
            
        except Exception as e:
            self.logger.error(f"从缓存加载数据模型失败: {e}")
            return None
    
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
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息
        
        Returns:
        --------
        Dict[str, Any]
            缓存信息字典
        """
        info = {
            'cache_dir': self.cache_dir,
            'data_cache_exists': os.path.exists(self.data_cache_file),
            'code_hash_exists': os.path.exists(self.code_hash_file),
            'cache_valid': self.is_cache_valid()
        }
        
        if os.path.exists(self.data_cache_file):
            info['cache_size'] = os.path.getsize(self.data_cache_file)
            info['cache_mtime'] = time.ctime(os.path.getmtime(self.data_cache_file))
        
        return info