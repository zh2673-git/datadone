#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
使用真实数据调试存取现识别问题
"""

import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config import Config
from src.datasource.bank_model import BankDataModel

def debug_real_bank_data():
    """调试真实银行数据"""
    print("=== 使用真实数据调试存取现识别 ===")
    
    # 查找银行数据文件
    data_dir = "data/samples"
    bank_files = []
    
    if os.path.exists(data_dir):
        for file in os.listdir(data_dir):
            if file.endswith('.xlsx') and ('账单数据预览' in file or '银行' in file):
                bank_files.append(os.path.join(data_dir, file))
    
    if not bank_files:
        print("❌ 未找到银行数据文件")
        return
    
    print(f"找到银行数据文件: {bank_files}")
    
    # 使用第一个银行数据文件
    bank_file = bank_files[0]
    print(f"使用文件: {bank_file}")
    
    try:
        # 初始化银行数据模型
        config = Config()
        bank_model = BankDataModel(data_path=bank_file, config=config)
        
        print(f"\n=== 数据基本信息 ===")
        print(f"总记录数: {len(bank_model.data)}")
        print(f"数据列名: {bank_model.data.columns.tolist()}")
        
        # 检查存取现识别结果
        if '存取现标识' in bank_model.data.columns:
            cash_stats = bank_model.data['存取现标识'].value_counts()
            print(f"\n=== 存取现识别统计 ===")
            print(cash_stats)
            
            # 查找包含"转账"的交易摘要
            print(f"\n=== 包含'转账'的交易摘要分析 ===")
            transfer_data = bank_model.data[
                bank_model.data['交易摘要'].str.contains('转账', na=False)
            ]
            
            if not transfer_data.empty:
                print(f"包含'转账'的交易数量: {len(transfer_data)}")
                
                # 按存取现标识分组
                transfer_by_flag = transfer_data.groupby('存取现标识').size()
                print("按存取现标识分组:")
                print(transfer_by_flag)
                
                # 查找被错误识别为存现的转账
                wrong_deposits = transfer_data[transfer_data['存取现标识'] == '存现']
                if not wrong_deposits.empty:
                    print(f"\n❌ 发现 {len(wrong_deposits)} 条转账被错误识别为存现:")
                    for _, row in wrong_deposits.iterrows():
                        print(f"  - {row['交易摘要']} (识别原因: {row.get('识别原因', '无')})")
                        print(f"    对方姓名: '{row.get('对方姓名', '')}', 借贷标识: {row.get('借贷标识', '')}")
                else:
                    print("✅ 没有转账被错误识别为存现")
                
                # 显示一些转账交易的详细信息
                print(f"\n=== 转账交易样本 ===")
                sample_transfers = transfer_data.head(10)
                for _, row in sample_transfers.iterrows():
                    print(f"交易摘要: {row['交易摘要']}")
                    print(f"  存取现标识: {row['存取现标识']}")
                    print(f"  对方姓名: '{row.get('对方姓名', '')}'")
                    print(f"  借贷标识: {row.get('借贷标识', '')}")
                    print(f"  识别原因: {row.get('识别原因', '无')}")
                    print()
            else:
                print("没有找到包含'转账'的交易")
            
            # 检查存现数据
            print(f"\n=== 存现数据分析 ===")
            deposit_data = bank_model.data[bank_model.data['存取现标识'] == '存现']
            
            if not deposit_data.empty:
                print(f"存现交易数量: {len(deposit_data)}")
                
                # 检查存现数据中是否有转账
                deposit_with_transfer = deposit_data[
                    deposit_data['交易摘要'].str.contains('转账', na=False)
                ]
                
                if not deposit_with_transfer.empty:
                    print(f"❌ 存现数据中包含 {len(deposit_with_transfer)} 条转账:")
                    for _, row in deposit_with_transfer.iterrows():
                        print(f"  - {row['交易摘要']} (金额: {row['交易金额']})")
                else:
                    print("✅ 存现数据中没有包含转账")
                
                # 显示存现交易的前10名
                print(f"\n=== 存现交易前10名 ===")
                top_deposits = deposit_data.nlargest(10, '交易金额')
                for _, row in top_deposits.iterrows():
                    print(f"  - {row['交易摘要']}: {row['交易金额']}元")
            else:
                print("没有找到存现交易")
        else:
            print("❌ 数据中没有'存取现标识'列")
            
    except Exception as e:
        print(f"❌ 处理数据时出错: {e}")
        import traceback
        traceback.print_exc()

def check_config_keywords():
    """检查配置中的关键词"""
    print("\n=== 检查配置关键词 ===")
    
    config = Config()
    
    deposit_exclude = config.get('data_sources.bank.deposit_exclude_keywords', [])
    print(f"存现排除关键词数量: {len(deposit_exclude)}")
    print("存现排除关键词（前20个）:")
    for i, keyword in enumerate(deposit_exclude[:20]):
        print(f"  {i+1}. {keyword}")
    
    # 检查是否包含常见的转账变体
    transfer_variants = ['转账', '转帐', '转账存入', '转帐存入', '转存', '转入']
    print(f"\n检查转账关键词变体:")
    for variant in transfer_variants:
        if variant in deposit_exclude:
            print(f"  ✅ {variant}")
        else:
            print(f"  ❌ {variant} (缺失)")

if __name__ == "__main__":
    debug_real_bank_data()
    check_config_keywords()
