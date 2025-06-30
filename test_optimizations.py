"""
测试优化后的功能
验证存取现识别、高级分析等功能的正确性和性能
"""

import pandas as pd
import numpy as np
import sys
import os
import time
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config import Config
from src.utils.cash_recognition import CashRecognitionEngine
from src.utils.advanced_analysis import AdvancedAnalysisEngine
from src.datasource.bank_model import BankDataModel


def create_test_data():
    """创建测试数据"""
    np.random.seed(42)
    
    # 创建基础测试数据
    data = {
        '本方姓名': ['张三'] * 50 + ['李四'] * 30 + ['王五'] * 20,
        '交易日期': pd.date_range('2024-01-01', periods=100, freq='D'),
        '交易时间': [f"{np.random.randint(8, 20):02d}:{np.random.randint(0, 60):02d}:00" for _ in range(100)],
        '交易金额': np.random.choice([100, 200, 500, 1000, 2000, 5000, 10000, 15000, 50000], 100),
        '借贷标识': np.random.choice(['借', '贷'], 100),
        '对方姓名': [''] * 30 + [f'商户{i}' for i in range(70)],  # 30个空值用于存取现测试
        '交易摘要': [],
        '交易备注': [],
        '账户余额': np.random.randint(10000, 100000, 100),
        '数据来源': ['测试数据.xlsx'] * 100
    }
    
    # 生成存取现相关的摘要和备注
    cash_keywords = ['ATM存现', 'ATM取现', '柜台存现', '柜台取现', '现金存款', '现金取款', 
                    '无卡存现', '无卡取现', 'CRS存现', 'CRS取现']
    transfer_keywords = ['转账', '网银转账', '手机银行转账', '代发工资', '理财购买']
    
    for i in range(100):
        if data['对方姓名'][i] == '':  # 存取现交易
            keyword = np.random.choice(cash_keywords)
            data['交易摘要'].append(keyword)
            data['交易备注'].append(f'{keyword}业务')
        else:  # 转账交易
            keyword = np.random.choice(transfer_keywords)
            data['交易摘要'].append(keyword)
            data['交易备注'].append(f'{keyword}业务')
    
    return pd.DataFrame(data)


def test_cash_recognition():
    """测试存取现识别功能"""
    print("=" * 50)
    print("测试存取现识别功能")
    print("=" * 50)
    
    # 创建配置和识别引擎
    config = Config()
    recognition_engine = CashRecognitionEngine(config)
    
    # 创建测试数据
    test_data = create_test_data()
    
    # 配置列名映射
    columns_config = {
        'opposite_name_column': '对方姓名',
        'summary_column': '交易摘要',
        'remark_column': '交易备注',
        'direction_column': '借贷标识',
        'amount_column': '交易金额',
        'income_flag': '贷',
        'expense_flag': '借'
    }
    
    # 执行识别
    start_time = time.time()
    result_data = recognition_engine.recognize_cash_operations(test_data, columns_config)
    end_time = time.time()
    
    # 统计结果
    stats = recognition_engine.get_recognition_stats(result_data)
    
    print(f"识别耗时: {end_time - start_time:.4f}秒")
    print(f"识别统计: {stats}")
    
    # 验证识别结果
    cash_transactions = result_data[result_data['存取现标识'].isin(['存现', '取现'])]
    transfer_transactions = result_data[result_data['存取现标识'] == '转账']
    
    print(f"\n识别结果验证:")
    print(f"存取现交易数: {len(cash_transactions)}")
    print(f"转账交易数: {len(transfer_transactions)}")
    
    if '识别置信度' in result_data.columns:
        avg_confidence = cash_transactions['识别置信度'].mean()
        print(f"平均识别置信度: {avg_confidence:.2f}")
    
    # 显示部分识别结果
    print(f"\n存取现交易示例:")
    if not cash_transactions.empty:
        display_columns = ['本方姓名', '交易摘要', '存取现标识', '交易金额']
        if '识别置信度' in cash_transactions.columns:
            display_columns.append('识别置信度')
        print(cash_transactions[display_columns].head())
    
    return result_data


def test_advanced_analysis():
    """测试高级分析功能"""
    print("\n" + "=" * 50)
    print("测试高级分析功能")
    print("=" * 50)
    
    # 创建配置和分析引擎
    config = Config()
    analysis_engine = AdvancedAnalysisEngine(config)
    
    # 创建测试数据
    test_data = create_test_data()
    
    # 时间模式分析
    print("\n1. 时间模式分析:")
    start_time = time.time()
    time_patterns = analysis_engine.analyze_time_patterns(test_data, '交易日期', '交易时间')
    end_time = time.time()
    
    print(f"分析耗时: {end_time - start_time:.4f}秒")
    if time_patterns:
        print("工作日分布:", time_patterns.get('weekday_distribution', {}))
        print("工作时间分析:", time_patterns.get('working_hours_analysis', {}))
    
    # 金额模式分析
    print("\n2. 金额模式分析:")
    start_time = time.time()
    amount_patterns = analysis_engine.analyze_amount_patterns(test_data, '交易金额')
    end_time = time.time()
    
    print(f"分析耗时: {end_time - start_time:.4f}秒")
    if amount_patterns:
        print("金额区间分布:", amount_patterns.get('amount_ranges', {}))
        print("整数金额分析:", amount_patterns.get('round_number_analysis', {}))
    
    # 异常检测
    print("\n3. 异常检测:")
    start_time = time.time()
    anomalies = analysis_engine.detect_anomalies(test_data, '本方姓名', '交易金额', '交易日期', '交易时间')
    end_time = time.time()
    
    print(f"检测耗时: {end_time - start_time:.4f}秒")
    if anomalies:
        print(f"检测到异常数: {anomalies.get('anomaly_count', 0)}")
        print(f"异常类型: {anomalies.get('anomaly_types', [])}")
    
    # 交易模式分析
    print("\n4. 交易模式分析:")
    start_time = time.time()
    transaction_patterns = analysis_engine.analyze_transaction_patterns(test_data, '本方姓名', '交易金额', '交易日期')
    end_time = time.time()
    
    print(f"分析耗时: {end_time - start_time:.4f}秒")
    if transaction_patterns:
        print("整体模式:", transaction_patterns.get('overall_patterns', {}))
        
        # 显示个人模式示例
        person_patterns = transaction_patterns.get('person_patterns', {})
        if person_patterns:
            print("\n个人模式示例:")
            for person, pattern in list(person_patterns.items())[:2]:
                print(f"{person}: 交易次数={pattern['交易次数']}, 整数金额比例={pattern['整数金额比例']:.2f}")


def test_bank_model_integration():
    """测试银行模型集成"""
    print("\n" + "=" * 50)
    print("测试银行模型集成")
    print("=" * 50)
    
    # 创建测试数据
    test_data = create_test_data()
    
    # 创建银行模型
    start_time = time.time()
    bank_model = BankDataModel(data=test_data)
    end_time = time.time()
    
    print(f"模型初始化耗时: {end_time - start_time:.4f}秒")
    
    # 验证存取现识别结果
    cash_data = bank_model.get_cash_data()
    transfer_data = bank_model.get_transfer_data()
    
    print(f"存取现交易数: {len(cash_data)}")
    print(f"转账交易数: {len(transfer_data)}")
    
    # 验证数据完整性
    total_transactions = len(cash_data) + len(transfer_data)
    original_transactions = len(test_data)
    
    print(f"数据完整性验证: {total_transactions}/{original_transactions} = {total_transactions/original_transactions:.2%}")
    
    if '识别置信度' in bank_model.data.columns:
        cash_with_confidence = bank_model.data[bank_model.data['存取现标识'].isin(['存现', '取现'])]
        if not cash_with_confidence.empty:
            avg_confidence = cash_with_confidence['识别置信度'].mean()
            print(f"平均识别置信度: {avg_confidence:.2f}")


def test_performance():
    """性能测试"""
    print("\n" + "=" * 50)
    print("性能测试")
    print("=" * 50)
    
    # 测试不同数据量的性能
    data_sizes = [100, 500, 1000, 5000]
    
    for size in data_sizes:
        print(f"\n测试数据量: {size}")
        
        # 生成测试数据
        large_test_data = create_test_data()
        # 扩展数据
        multiplier = size // 100
        if multiplier > 1:
            large_test_data = pd.concat([large_test_data] * multiplier, ignore_index=True)
            large_test_data = large_test_data.head(size)
        
        # 测试银行模型性能
        start_time = time.time()
        bank_model = BankDataModel(data=large_test_data)
        end_time = time.time()
        
        processing_time = end_time - start_time
        throughput = size / processing_time if processing_time > 0 else float('inf')
        
        print(f"处理时间: {processing_time:.4f}秒")
        print(f"处理速度: {throughput:.0f} 条/秒")
        
        # 内存使用情况
        memory_usage = large_test_data.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        print(f"内存使用: {memory_usage:.2f} MB")


def main():
    """主测试函数"""
    print("开始测试优化后的功能")
    print("测试时间:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    try:
        # 测试存取现识别
        test_cash_recognition()
        
        # 测试高级分析
        test_advanced_analysis()
        
        # 测试银行模型集成
        test_bank_model_integration()
        
        # 性能测试
        test_performance()
        
        print("\n" + "=" * 50)
        print("所有测试完成!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n测试过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
