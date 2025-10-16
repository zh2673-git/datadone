#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
性能缓存管理器
提供高效的缓存机制，减少重复计算
"""

import hashlib
import pickle
import os
import time
from typing import Any, Optional, Dict, Callable
import logging
import pandas as pd


class PerformanceCache:
    """
    性能缓存管理器
    
    提供高效的缓存机制，包括：
    1. 内存缓存
    2. 磁盘缓存
    3. 缓存失效策略
    4. 性能统计
    """
    
    def __init__(self, cache_dir: str = "cache", max_memory_size: int = 100):
        """
        初始化缓存管理器
        
        Parameters:
        -----------
        cache_dir : str
            缓存目录
        max_memory_size : int
            最大内存缓存项数
        """
        self.cache_dir = cache_dir
        self.max_memory_size = max_memory_size
        self.memory_cache = {}
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }
        self.logger = logging.getLogger(__name__)
        
        # 确保缓存目录存在
        os.makedirs(cache_dir, exist_ok=True)
    
    def _generate_cache_key(self, func_name: str, *args, **kwargs) -> str:
        """
        生成缓存键
        
        Parameters:
        -----------
        func_name : str
            函数名
        *args
            位置参数
        **kwargs
            关键字参数
            
        Returns:
        --------
        str
            缓存键
        """
        # 创建参数的字符串表示
        key_data = f"{func_name}:{str(args)}:{str(sorted(kwargs.items()))}"
        
        # 生成MD5哈希
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_cache_valid(self, cache_time: float, ttl: int) -> bool:
        """
        检查缓存是否有效
        
        Parameters:
        -----------
        cache_time : float
            缓存时间
        ttl : int
            生存时间（秒）
            
        Returns:
        --------
        bool
            是否有效
        """
        return time.time() - cache_time < ttl
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存项
        
        Parameters:
        -----------
        key : str
            缓存键
            
        Returns:
        --------
        Optional[Any]
            缓存值，如果不存在则返回None
        """
        # 先检查内存缓存
        if key in self.memory_cache:
            cache_item = self.memory_cache[key]
            if self._is_cache_valid(cache_item['time'], cache_item['ttl']):
                self.cache_stats['hits'] += 1
                return cache_item['data']
            else:
                # 缓存过期，删除
                del self.memory_cache[key]
        
        # 检查磁盘缓存
        cache_file = os.path.join(self.cache_dir, f"{key}.pkl")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'rb') as f:
                    cache_item = pickle.load(f)
                
                if self._is_cache_valid(cache_item['time'], cache_item['ttl']):
                    # 加载到内存缓存
                    self._add_to_memory_cache(key, cache_item['data'], cache_item['ttl'])
                    self.cache_stats['hits'] += 1
                    return cache_item['data']
                else:
                    # 缓存过期，删除文件
                    os.remove(cache_file)
            except Exception as e:
                self.logger.warning(f"读取缓存文件失败: {str(e)}")
        
        self.cache_stats['misses'] += 1
        return None
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> None:
        """
        设置缓存项
        
        Parameters:
        -----------
        key : str
            缓存键
        value : Any
            缓存值
        ttl : int
            生存时间（秒）
        """
        # 添加到内存缓存
        self._add_to_memory_cache(key, value, ttl)
        
        # 保存到磁盘缓存
        cache_file = os.path.join(self.cache_dir, f"{key}.pkl")
        try:
            cache_item = {
                'data': value,
                'time': time.time(),
                'ttl': ttl
            }
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_item, f)
        except Exception as e:
            self.logger.warning(f"保存缓存文件失败: {str(e)}")
    
    def _add_to_memory_cache(self, key: str, value: Any, ttl: int) -> None:
        """
        添加到内存缓存
        
        Parameters:
        -----------
        key : str
            缓存键
        value : Any
            缓存值
        ttl : int
            生存时间（秒）
        """
        # 如果内存缓存已满，删除最旧的项
        if len(self.memory_cache) >= self.max_memory_size:
            oldest_key = min(self.memory_cache.keys(), 
                           key=lambda k: self.memory_cache[k]['time'])
            del self.memory_cache[oldest_key]
            self.cache_stats['evictions'] += 1
        
        self.memory_cache[key] = {
            'data': value,
            'time': time.time(),
            'ttl': ttl
        }
    
    def clear(self) -> None:
        """清空所有缓存"""
        self.memory_cache.clear()
        
        # 清空磁盘缓存
        try:
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.pkl'):
                    os.remove(os.path.join(self.cache_dir, filename))
        except Exception as e:
            self.logger.warning(f"清空磁盘缓存失败: {str(e)}")
    
    def get_stats(self) -> Dict[str, int]:
        """
        获取缓存统计信息
        
        Returns:
        --------
        Dict[str, int]
            统计信息
        """
        total_requests = self.cache_stats['hits'] + self.cache_stats['misses']
        hit_rate = self.cache_stats['hits'] / total_requests if total_requests > 0 else 0
        
        return {
            'hits': self.cache_stats['hits'],
            'misses': self.cache_stats['misses'],
            'evictions': self.cache_stats['evictions'],
            'hit_rate': hit_rate,
            'memory_items': len(self.memory_cache)
        }
    
    def cached(self, ttl: int = 3600):
        """
        缓存装饰器
        
        Parameters:
        -----------
        ttl : int
            生存时间（秒）
            
        Returns:
        --------
        callable
            装饰器函数
        """
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                # 生成缓存键
                cache_key = self._generate_cache_key(func.__name__, *args, **kwargs)
                
                # 尝试获取缓存
                cached_result = self.get(cache_key)
                if cached_result is not None:
                    return cached_result
                
                # 执行函数并缓存结果
                result = func(*args, **kwargs)
                self.set(cache_key, result, ttl)
                
                return result
            
            return wrapper
        return decorator


# 全局缓存实例
_global_cache = None


def get_cache() -> PerformanceCache:
    """
    获取全局缓存实例
    
    Returns:
    --------
    PerformanceCache
        缓存实例
    """
    global _global_cache
    if _global_cache is None:
        _global_cache = PerformanceCache()
    return _global_cache


def clear_global_cache() -> None:
    """清空全局缓存"""
    global _global_cache
    if _global_cache is not None:
        _global_cache.clear()
