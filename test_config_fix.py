#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试配置修复是否成功
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config import Config
from src.utils.cash_recognition import CashRecognitionEngine

def test_config_loading():
    """测试配置加载是否正确"""
    print("=== 测试配置修复结果 ===")
    
    # 初始化配置
    config = Config()
    
    # 初始化存取现识别引擎
    engine = CashRecognitionEngine(config)
    
    print("\n=== 配置加载结果 ===")
    print(f"存现关键词数量: {len(engine.deposit_keywords)}")
    print(f"取现关键词数量: {len(engine.withdraw_keywords)}")
    print(f"存现排除关键词数量: {len(engine.deposit_exclude_keywords)}")
    print(f"取现排除关键词数量: {len(engine.withdraw_exclude_keywords)}")
    print(f"高优先级存现关键词数量: {len(engine.high_priority_deposit_keywords)}")
    print(f"高优先级取现关键词数量: {len(engine.high_priority_withdraw_keywords)}")
    
    print("\n=== 关键词示例 ===")
    if engine.deposit_keywords:
        print(f"存现关键词前5个: {engine.deposit_keywords[:5]}")
    if engine.deposit_exclude_keywords:
        print(f"存现排除关键词前5个: {engine.deposit_exclude_keywords[:5]}")
    if engine.high_priority_deposit_keywords:
        print(f"高优先级存现关键词前5个: {engine.high_priority_deposit_keywords[:5]}")
    
    print("\n=== 检查关键配置 ===")
    success = True
    
    if not engine.deposit_keywords:
        print("❌ 存现关键词为空！")
        success = False
    else:
        print("✅ 存现关键词加载成功")
    
    if not engine.withdraw_keywords:
        print("❌ 取现关键词为空！")
        success = False
    else:
        print("✅ 取现关键词加载成功")
    
    if not engine.deposit_exclude_keywords:
        print("❌ 存现排除关键词为空！")
        success = False
    else:
        print("✅ 存现排除关键词加载成功")
    
    if not engine.withdraw_exclude_keywords:
        print("❌ 取现排除关键词为空！")
        success = False
    else:
        print("✅ 取现排除关键词加载成功")
    
    if not engine.high_priority_deposit_keywords:
        print("❌ 高优先级存现关键词为空！")
        success = False
    else:
        print("✅ 高优先级存现关键词加载成功")
    
    if not engine.high_priority_withdraw_keywords:
        print("❌ 高优先级取现关键词为空！")
        success = False
    else:
        print("✅ 高优先级取现关键词加载成功")
    
    print("\n=== 检查转账排除功能 ===")
    if "转账" in engine.deposit_exclude_keywords:
        print("✅ 存现排除关键词包含'转账'")
    else:
        print("❌ 存现排除关键词不包含'转账'")
        success = False
    
    if "转账" in engine.withdraw_exclude_keywords:
        print("✅ 取现排除关键词包含'转账'")
    else:
        print("❌ 取现排除关键词不包含'转账'")
        success = False
    
    print("\n=== 总结 ===")
    if success:
        print("🎉 配置修复成功！所有关键词都正确加载。")
        print("现在存取现识别应该能正确排除'转账存入'等转账交易。")
    else:
        print("❌ 配置修复失败！仍有问题需要解决。")
    
    return success

if __name__ == "__main__":
    test_config_loading()
