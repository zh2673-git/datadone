#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调试Word报告数据获取逻辑
"""

import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config import Config
from src.datasource.bank_model import BankDataModel

def test_word_report_data_source():
    """测试Word报告数据来源"""
    print("=== 调试Word报告数据获取逻辑 ===")
    
    # 创建测试数据，模拟真实的银行数据
    test_data = pd.DataFrame({
        '本方姓名': ['张三'] * 10,
        '交易日期': pd.date_range('2024-01-01', periods=10),
        '交易金额': [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000],
        '借贷标识': ['贷'] * 10,
        '对方姓名': [''] * 10,  # 所有都是空，符合存取现条件
        '交易摘要': [
            'ATM存现',           # 应该识别为存现
            'ATM转账存入',       # 应该保持为转账
            '转账存入',          # 应该保持为转账
            '柜台存现',          # 应该识别为存现
            'ATM存款',           # 应该识别为存现
            '网银转账存入',      # 应该保持为转账
            'CRS无卡存现',       # 应该识别为存现
            '转账',              # 应该保持为转账
            '现金存款',          # 应该识别为存现
            '自助存现'           # 应该识别为存现
        ],
        '交易备注': [''] * 10,
        '交易类型': [''] * 10,
        '账户余额': [10000] * 10,
        '银行类型': ['建设银行'] * 10,
        '本方账号': ['6217002'] * 10
    })
    
    print("=== 原始测试数据 ===")
    print(test_data[['交易摘要', '借贷标识', '对方姓名', '交易金额']])
    
    # 初始化银行数据模型
    config = Config()
    bank_model = BankDataModel(data=test_data, config=config)
    
    print("\n=== 银行模型预处理后的数据 ===")
    processed_data = bank_model.data
    print("数据列名:", processed_data.columns.tolist())
    
    # 显示存取现识别结果
    result_display = processed_data[['交易摘要', '存取现标识']]
    if '识别置信度' in processed_data.columns:
        result_display = processed_data[['交易摘要', '存取现标识', '识别置信度', '识别原因']]
    
    print("\n=== 存取现识别结果 ===")
    print(result_display)
    
    # 分析问题
    print("\n=== 问题分析 ===")
    problem_cases = processed_data[
        processed_data['交易摘要'].str.contains('转账', na=False) & 
        (processed_data['存取现标识'] == '存现')
    ]
    
    if not problem_cases.empty:
        print("❌ 发现问题：以下转账交易被错误识别为存现：")
        print(problem_cases[['交易摘要', '存取现标识', '识别原因']])
    else:
        print("✅ 没有发现转账被错误识别为存现的问题")
    
    # 模拟Word报告的数据获取
    print("\n=== 模拟Word报告数据获取 ===")
    person_name = '张三'
    
    # 方法1：直接从bank_model.data获取（Word报告使用的方法）
    person_cash_data_method1 = bank_model.data[
        (bank_model.data[bank_model.name_column] == person_name) & 
        (bank_model.data['存取现标识'] == '存现')
    ].copy()
    
    print(f"方法1 - 直接从bank_model.data获取存现数据（Word报告方法）:")
    print(f"存现交易数量: {len(person_cash_data_method1)}")
    if not person_cash_data_method1.empty:
        print("存现交易摘要:")
        for summary in person_cash_data_method1['交易摘要']:
            print(f"  - {summary}")
    
    # 方法2：使用bank_model的get_deposit_data方法
    person_cash_data_method2 = bank_model.get_deposit_data(person_name)
    
    print(f"\n方法2 - 使用get_deposit_data方法:")
    print(f"存现交易数量: {len(person_cash_data_method2)}")
    if not person_cash_data_method2.empty:
        print("存现交易摘要:")
        for summary in person_cash_data_method2['交易摘要']:
            print(f"  - {summary}")
    
    # 检查两种方法是否一致
    if len(person_cash_data_method1) == len(person_cash_data_method2):
        print("\n✅ 两种方法获取的数据数量一致")
    else:
        print(f"\n❌ 两种方法获取的数据数量不一致: {len(person_cash_data_method1)} vs {len(person_cash_data_method2)}")
    
    # 检查是否有转账被包含在存现数据中
    print("\n=== 检查存现数据中是否包含转账 ===")
    transfer_in_deposit = person_cash_data_method1[
        person_cash_data_method1['交易摘要'].str.contains('转账', na=False)
    ]
    
    if not transfer_in_deposit.empty:
        print("❌ 存现数据中包含转账交易:")
        for _, row in transfer_in_deposit.iterrows():
            print(f"  - {row['交易摘要']} (识别原因: {row.get('识别原因', '无')})")
    else:
        print("✅ 存现数据中没有包含转账交易")
    
    # 获取前5名存现交易（模拟Word报告）
    print("\n=== 模拟Word报告前5名存现交易 ===")
    if not person_cash_data_method1.empty:
        top_5_deposits = person_cash_data_method1.nlargest(5, bank_model.amount_column, keep='first')
        print("前5名存现交易:")
        for _, row in top_5_deposits.iterrows():
            print(f"  - {row['交易摘要']}: {row[bank_model.amount_column]}元")
    else:
        print("没有存现交易")

if __name__ == "__main__":
    test_word_report_data_source()
