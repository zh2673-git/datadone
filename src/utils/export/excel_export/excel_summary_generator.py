"""
Excel汇总生成器模块

提供Excel汇总表生成的相关功能，包括银行、微信、支付宝等平台的汇总数据生成。
"""

import pandas as pd
from typing import Dict, Any, List


class ExcelSummaryGenerator:
    """Excel汇总生成器类"""
    
    def __init__(self):
        """初始化Excel汇总生成器"""
        pass
    
    def get_bank_summary_data(self, bank_model) -> pd.DataFrame:
        """
        获取银行数据的汇总信息
        
        Parameters:
        -----------
        bank_model : BankDataModel
            银行数据模型
            
        Returns:
        --------
        pd.DataFrame
            银行汇总数据
        """
        if not bank_model or bank_model.data.empty:
            return pd.DataFrame()

        full_data = bank_model.data.copy()

        group_keys = ['本方姓名']
        if '数据来源' in full_data.columns:
            group_keys.insert(0, '数据来源')

        # 存取现汇总
        cash_summary = full_data.groupby(group_keys).agg(
            存现金额=('收入金额', lambda x: x[full_data.loc[x.index, '存取现标识'] == '存现'].sum()),
            取现金额=('支出金额', lambda x: x[full_data.loc[x.index, '存取现标识'] == '取现'].sum())
        ).reset_index()
        cash_summary = cash_summary[(cash_summary['存现金额'] > 0) | (cash_summary['取现金额'] > 0)]

        # 转账汇总
        transfer_summary = full_data[full_data['存取现标识'] == '转账'].groupby(group_keys).agg(
            转入金额=('收入金额', 'sum'),
            转出金额=('支出金额', 'sum')
        ).reset_index()
        transfer_summary = transfer_summary[(transfer_summary['转入金额'] > 0) | (transfer_summary['转出金额'] > 0)]

        # 合并银行数据
        combined_bank_data = []

        if not cash_summary.empty:
            cash_summary['分析类型'] = '存取现'
            cash_summary['平台'] = '银行'
            cash_summary['转入金额'] = 0
            cash_summary['转出金额'] = 0
            combined_bank_data.append(cash_summary)

        if not transfer_summary.empty:
            transfer_summary['分析类型'] = '转账'
            transfer_summary['平台'] = '银行'
            transfer_summary['存现金额'] = 0
            transfer_summary['取现金额'] = 0
            combined_bank_data.append(transfer_summary)

        if combined_bank_data:
            result = pd.concat(combined_bank_data, ignore_index=True)
            # 统一列顺序
            base_cols = ['分析类型', '平台']
            if '数据来源' in result.columns:
                base_cols.append('数据来源')
            base_cols.extend(['本方姓名', '存现金额', '取现金额', '转入金额', '转出金额'])
            final_cols = [col for col in base_cols if col in result.columns]
            return result[final_cols]

        return pd.DataFrame()
    
    def get_payment_summary_data(self, payment_model, platform_name: str) -> pd.DataFrame:
        """
        获取微信/支付宝数据的汇总信息
        
        Parameters:
        -----------
        payment_model : PaymentDataModel
            支付数据模型
        platform_name : str
            平台名称（微信/支付宝）
            
        Returns:
        --------
        pd.DataFrame
            支付汇总数据
        """
        if not payment_model or payment_model.data.empty:
            return pd.DataFrame()

        full_data = payment_model.data.copy()

        group_keys = ['本方姓名']
        if '数据来源' in full_data.columns:
            group_keys.insert(0, '数据来源')

        # 微信/支付宝转账汇总
        summary = full_data.groupby(group_keys).agg(
            转入金额=('收入金额', 'sum'),
            转出金额=('支出金额', 'sum')
        ).reset_index()
        summary = summary[(summary['转入金额'] > 0) | (summary['转出金额'] > 0)]

        if not summary.empty:
            summary['分析类型'] = '转账'
            summary['平台'] = platform_name
            summary['存现金额'] = 0
            summary['取现金额'] = 0

            # 统一列顺序
            base_cols = ['分析类型', '平台']
            if '数据来源' in summary.columns:
                base_cols.append('数据来源')
            base_cols.extend(['本方姓名', '存现金额', '取现金额', '转入金额', '转出金额'])
            final_cols = [col for col in base_cols if col in summary.columns]
            return summary[final_cols]

        return pd.DataFrame()
    
    def generate_comprehensive_summary(self, data_models: Dict) -> pd.DataFrame:
        """
        生成综合分析汇总表，包含银行、微信、支付宝的汇总数据
        
        Parameters:
        -----------
        data_models : Dict
            包含各种数据模型的字典
            
        Returns:
        --------
        pd.DataFrame
            综合分析汇总数据
        """
        all_summary_data = []

        # 处理银行数据
        if data_models and 'bank' in data_models and data_models['bank']:
            bank_summary = self.get_bank_summary_data(data_models['bank'])
            if not bank_summary.empty:
                all_summary_data.append(bank_summary)

        # 处理微信数据
        if data_models and 'wechat' in data_models and data_models['wechat']:
            wechat_summary = self.get_payment_summary_data(data_models['wechat'], '微信')
            if not wechat_summary.empty:
                all_summary_data.append(wechat_summary)

        # 处理支付宝数据
        if data_models and 'alipay' in data_models and data_models['alipay']:
            alipay_summary = self.get_payment_summary_data(data_models['alipay'], '支付宝')
            if not alipay_summary.empty:
                all_summary_data.append(alipay_summary)

        # 合并所有汇总数据
        if all_summary_data:
            combined_summary = pd.concat(all_summary_data, ignore_index=True)
            return combined_summary

        return pd.DataFrame()
    
    def format_platform_details(self, group: pd.DataFrame) -> str:
        """
        格式化平台金额分布详情
        
        Parameters:
        -----------
        group : pd.DataFrame
            平台数据分组
            
        Returns:
        --------
        str
            格式化后的平台详情字符串
        """
        details = []
        for _, row in group.iterrows():
            platform = row['平台']
            
            # 安全地获取收入总额和支出总额
            income = 0
            expense = 0
            if '收入总额' in row.index:
                income = row['收入总额']
            if '支出总额' in row.index:
                expense = row['支出总额']
                
            if income > 0 or expense > 0:
                detail = f"{platform}(收入{income:.0f}元,支出{expense:.0f}元)"
                details.append(detail)
        return '; '.join(details) if details else '无'