#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调试存取现识别逻辑
"""

import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config import Config
from src.utils.cash_recognition import CashRecognitionEngine

def test_specific_cases():
    """测试具体的问题案例"""
    print("=== 调试存取现识别逻辑 ===")
    
    # 创建测试数据
    test_data = pd.DataFrame({
        '本方姓名': ['张三', '张三', '张三', '张三', '张三'],
        '交易日期': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        '交易金额': [1000, 2000, 3000, 4000, 5000],
        '借贷标识': ['贷', '贷', '贷', '贷', '贷'],
        '对方姓名': ['', '', '', '', ''],
        '交易摘要': ['ATM存现', 'ATM转账存入', '转账存入', '柜台存现', 'ATM存款'],
        '交易备注': ['', '', '', '', ''],
        '交易类型': ['', '', '', '', '']
    })
    
    print("=== 测试数据 ===")
    print(test_data[['交易摘要', '借贷标识', '对方姓名', '交易金额']])
    
    # 初始化配置和引擎
    config = Config()
    engine = CashRecognitionEngine(config)
    
    # 准备列名配置
    columns_config = {
        'opposite_name_column': '对方姓名',
        'summary_column': '交易摘要',
        'remark_column': '交易备注',
        'type_column': '交易类型',
        'direction_column': '借贷标识',
        'amount_column': '交易金额',
        'income_flag': '贷',
        'expense_flag': '借'
    }
    
    print("\n=== 识别前状态 ===")
    print("高优先级存现关键词:", engine.high_priority_deposit_keywords[:5])
    print("存现排除关键词:", engine.deposit_exclude_keywords[:5])
    
    # 执行识别
    result = engine.recognize_cash_operations(test_data, columns_config)
    
    print("\n=== 识别结果 ===")
    print("结果列名:", result.columns.tolist())

    # 动态选择存在的列
    display_cols = ['交易摘要', '存取现标识']
    if '识别置信度' in result.columns:
        display_cols.extend(['识别置信度', '识别原因'])
    if '收入金额' in result.columns:
        display_cols.append('收入金额')
    if '支出金额' in result.columns:
        display_cols.append('支出金额')

    result_display = result[display_cols]
    print(result_display)
    
    print("\n=== 问题分析 ===")
    problem_cases = result[result['交易摘要'].str.contains('转账', na=False) & (result['存取现标识'] == '存现')]
    if not problem_cases.empty:
        print("❌ 发现问题：以下转账交易被错误识别为存现：")
        print(problem_cases[['交易摘要', '存取现标识', '识别原因']])
    else:
        print("✅ 没有发现转账被错误识别为存现的问题")
    
    # 详细分析每个案例
    print("\n=== 详细分析 ===")
    for idx, row in result.iterrows():
        summary = row['交易摘要']
        cash_flag = row['存取现标识']
        reason = row.get('识别原因', '无')
        
        print(f"案例 {idx+1}: '{summary}' → {cash_flag} ({reason})")
        
        # 检查是否应该被排除
        should_be_excluded = any(exclude_word in summary for exclude_word in engine.deposit_exclude_keywords)
        should_be_included = any(include_word in summary for include_word in engine.high_priority_deposit_keywords)
        
        if '转账' in summary and cash_flag == '存现':
            print(f"  ❌ 问题：包含'转账'但被识别为存现")
            print(f"  - 应该被排除: {should_be_excluded}")
            print(f"  - 匹配高优先级: {should_be_included}")
        elif '转账' in summary and cash_flag == '转账':
            print(f"  ✅ 正确：包含'转账'且保持为转账")
        elif '转账' not in summary and cash_flag == '存现':
            print(f"  ✅ 正确：不包含'转账'且识别为存现")

def test_keyword_matching():
    """测试关键词匹配逻辑"""
    print("\n=== 关键词匹配测试 ===")
    
    config = Config()
    engine = CashRecognitionEngine(config)
    
    test_cases = [
        "ATM存现",
        "ATM转账存入", 
        "转账存入",
        "ATM存款",
        "柜台存现"
    ]
    
    print("高优先级存现关键词:", engine.high_priority_deposit_keywords)
    print("存现排除关键词:", engine.deposit_exclude_keywords)
    
    for case in test_cases:
        print(f"\n测试案例: '{case}'")
        
        # 检查高优先级匹配
        high_priority_match = any(keyword in case for keyword in engine.high_priority_deposit_keywords)
        print(f"  高优先级匹配: {high_priority_match}")
        
        # 检查排除匹配
        exclude_match = any(keyword in case for keyword in engine.deposit_exclude_keywords)
        print(f"  排除匹配: {exclude_match}")
        
        # 最终结果
        final_result = high_priority_match and not exclude_match
        print(f"  最终结果: {'应该识别为存现' if final_result else '不应该识别为存现'}")

if __name__ == "__main__":
    test_specific_cases()
    test_keyword_matching()
