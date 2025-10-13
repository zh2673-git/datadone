#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
大额资金追踪功能测试脚本
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.fund_tracking import FundTrackingEngine
from src.datasource.bank_model import BankDataModel
from src.datasource.call_model import CallDataModel
from src.utils.config import Config

def create_test_data():
    """创建测试数据"""
    
    # 创建银行测试数据
    bank_data = pd.DataFrame({
        '交易日期': ['2024-01-15', '2024-01-20', '2024-02-10', '2024-02-25', '2024-03-05'],
        '本方姓名': ['张三', '张三', '李四', '王五', '张三'],
        '对方姓名': ['李四', '王五', '赵六', '张三', '李四'],
        '交易金额': [150000, -80000, 200000, -120000, 300000],
        '交易摘要': ['转账收入', '转账支出', '转账收入', '转账支出', '转账收入'],
        '银行名称': ['工商银行', '建设银行', '农业银行', '中国银行', '工商银行']
    })
    
    # 创建微信测试数据
    wechat_data = pd.DataFrame({
        '交易日期': ['2024-01-18', '2024-02-12', '2024-03-08'],
        '本方姓名': ['张三', '李四', '王五'],
        '对方姓名': ['赵六', '张三', '李四'],
        '交易金额': [50000, -60000, 70000],
        '交易备注': ['微信转账', '微信转账', '微信转账']
    })
    
    return bank_data, wechat_data

def test_fund_tracking():
    """测试大额资金追踪功能"""
    
    print("=== 大额资金追踪功能测试 ===")
    
    # 创建测试数据
    bank_data, wechat_data = create_test_data()
    
    print("测试数据创建完成:")
    print("银行数据:")
    print(bank_data)
    print("\n微信数据:")
    print(wechat_data)
    
    # 创建数据模型
    config = Config()
    
    # 创建银行数据模型（简化版）
    class SimpleBankDataModel:
        def __init__(self, data):
            self.data = data
            self.name_column = '本方姓名'
            self.amount_column = '交易金额'
            self.date_column = '交易日期'
            self.opposite_name_column = '对方姓名'
    
    bank_model = SimpleBankDataModel(bank_data)
    
    # 创建微信模型（简化版）
    class WechatDataModel:
        def __init__(self, data):
            self.data = data
            self.name_column = '本方姓名'
            self.amount_column = '交易金额'
            self.date_column = '交易日期'
            self.opposite_name_column = '对方姓名'
    
    wechat_model = WechatDataModel(wechat_data)
    
    # 创建数据模型字典
    data_models = {
        'bank': bank_model,
        'wechat': wechat_model
    }
    
    # 创建大额资金追踪引擎
    fund_tracker = FundTrackingEngine(config)
    
    # 执行大额资金追踪
    print("\n=== 开始大额资金追踪 ===")
    tracking_results = fund_tracker.track_large_funds(data_models)
    
    if not tracking_results.empty:
        print("大额资金追踪结果:")
        print(tracking_results.to_string())
        
        # 输出详细的追踪说明
        print("\n=== 详细追踪说明 ===")
        for idx, row in tracking_results.iterrows():
            if '追踪说明' in row:
                print(f"{row['核心人员']} - {row['交易日期']}:")
                print(f"  {row['追踪说明']}")
                print()
    else:
        print("未发现大额交易记录")
    
    return tracking_results

if __name__ == "__main__":
    test_fund_tracking()