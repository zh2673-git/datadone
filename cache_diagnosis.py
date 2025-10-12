#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
缓存系统诊断脚本
"""

import os
import sys
import time
import pickle

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.utils.cache_manager import DataCacheManager

def diagnose_cache():
    """诊断缓存系统问题"""
    print("=== 缓存系统诊断 ===")
    
    # 创建缓存管理器实例
    config = {
        'app': {
            'cache_enabled': True,
            'cache_timeout': 3600,
            'cache_dir': 'cache'
        }
    }
    
    cache_manager = DataCacheManager(config=config)
    
    # 检查缓存目录
    print("\n1. 检查缓存目录:")
    print(f"缓存目录: {cache_manager.cache_dir}")
    print(f"目录存在: {os.path.exists(cache_manager.cache_dir)}")
    
    if os.path.exists(cache_manager.cache_dir):
        cache_files = os.listdir(cache_manager.cache_dir)
        print(f"缓存文件数量: {len(cache_files)}")
        for file in cache_files:
            file_path = os.path.join(cache_manager.cache_dir, file)
            file_size = os.path.getsize(file_path)
            mtime = time.ctime(os.path.getmtime(file_path))
            print(f"  - {file}: {file_size} 字节, 修改时间: {mtime}")
    
    # 检查缓存文件路径
    print("\n2. 检查缓存文件路径:")
    print(f"数据缓存文件: {cache_manager.data_cache_file}")
    print(f"存在: {os.path.exists(cache_manager.data_cache_file)}")
    print(f"代码哈希文件: {cache_manager.code_hash_file}")
    print(f"存在: {os.path.exists(cache_manager.code_hash_file)}")
    
    # 检查缓存有效性
    print("\n3. 检查缓存有效性:")
    is_valid = cache_manager.is_cache_valid()
    print(f"缓存有效: {is_valid}")
    
    # 获取缓存信息
    print("\n4. 缓存详细信息:")
    cache_info = cache_manager.get_cache_info()
    for key, value in cache_info.items():
        print(f"  {key}: {value}")
    
    # 测试缓存保存和加载
    print("\n5. 测试缓存功能:")
    
    # 创建测试数据
    test_data = {
        'test_model': {
            'data': '测试数据',
            'file_path': 'test_file.xlsx',
            'config_info': {'test_config': True}
        }
    }
    
    # 测试保存
    print("测试保存缓存...")
    save_result = cache_manager.save_data_models(test_data)
    print(f"保存结果: {save_result}")
    
    # 检查保存后的文件
    if save_result:
        print("保存后检查文件:")
        if os.path.exists(cache_manager.data_cache_file):
            file_size = os.path.getsize(cache_manager.data_cache_file)
            print(f"数据缓存文件大小: {file_size} 字节")
            
            # 尝试加载
            try:
                with open(cache_manager.data_cache_file, 'rb') as f:
                    loaded_data = pickle.load(f)
                print(f"加载测试数据成功: {type(loaded_data)}")
            except Exception as e:
                print(f"加载测试数据失败: {e}")
        
        if os.path.exists(cache_manager.code_hash_file):
            with open(cache_manager.code_hash_file, 'r', encoding='utf-8') as f:
                hash_content = f.read()
            print(f"代码哈希文件内容: {hash_content[:50]}...")
    
    print("\n=== 诊断完成 ===")

if __name__ == "__main__":
    diagnose_cache()