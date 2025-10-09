"""
Excel数据提取器模块

提供Excel数据提取的相关功能，包括从分析结果中提取银行、支付平台原始数据等。
"""

import pandas as pd
from typing import Dict, Any, List, Optional


class ExcelDataExtractor:
    """Excel数据提取器类"""
    
    def __init__(self):
        """初始化Excel数据提取器"""
        pass
    
    def get_bank_raw_data(self, analysis_results: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        获取银行平台原始数据
        
        Parameters:
        -----------
        analysis_results : Dict[str, Any]
            分析结果字典
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            银行原始数据字典，键为数据类型，值为DataFrame
        """
        bank_raw_data = {}
        
        # 从分析结果中提取银行相关数据
        if 'bank' in analysis_results:
            bank_data = analysis_results['bank']
            
            # 转账数据
            if 'transfer' in bank_data:
                bank_raw_data['转账'] = bank_data['transfer']
            
            # 存现数据
            if 'deposit' in bank_data:
                bank_raw_data['存现'] = bank_data['deposit']
            
            # 取现数据
            if 'withdrawal' in bank_data:
                bank_raw_data['取现'] = bank_data['withdrawal']
            
            # 工资数据
            if 'salary' in bank_data:
                bank_raw_data['工资'] = bank_data['salary']
            
            # 奖金数据
            if 'bonus' in bank_data:
                bank_raw_data['奖金'] = bank_data['bonus']
            
            # 报销数据
            if 'reimbursement' in bank_data:
                bank_raw_data['报销'] = bank_data['reimbursement']
            
            # 贷款数据
            if 'loan' in bank_data:
                bank_raw_data['贷款'] = bank_data['loan']
            
            # 还款数据
            if 'repayment' in bank_data:
                bank_raw_data['还款'] = bank_data['repayment']
        
        return bank_raw_data
    
    def get_payment_raw_data(self, analysis_results: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        获取微信/支付宝平台原始数据
        
        Parameters:
        -----------
        analysis_results : Dict[str, Any]
            分析结果字典
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            支付平台原始数据字典，键为数据类型，值为DataFrame
        """
        payment_raw_data = {}
        
        # 从分析结果中提取微信相关数据
        if 'wechat' in analysis_results:
            wechat_data = analysis_results['wechat']
            
            # 微信转账数据
            if 'transfer' in wechat_data:
                payment_raw_data['微信转账'] = wechat_data['transfer']
            
            # 微信红包数据
            if 'red_packet' in wechat_data:
                payment_raw_data['微信红包'] = wechat_data['red_packet']
            
            # 微信收款数据
            if 'receipt' in wechat_data:
                payment_raw_data['微信收款'] = wechat_data['receipt']
            
            # 微信付款数据
            if 'payment' in wechat_data:
                payment_raw_data['微信付款'] = wechat_data['payment']
        
        # 从分析结果中提取支付宝相关数据
        if 'alipay' in analysis_results:
            alipay_data = analysis_results['alipay']
            
            # 支付宝转账数据
            if 'transfer' in alipay_data:
                payment_raw_data['支付宝转账'] = alipay_data['transfer']
            
            # 支付宝收款数据
            if 'receipt' in alipay_data:
                payment_raw_data['支付宝收款'] = alipay_data['receipt']
            
            # 支付宝付款数据
            if 'payment' in alipay_data:
                payment_raw_data['支付宝付款'] = alipay_data['payment']
        
        return payment_raw_data
    
    def get_special_data_from_results(self, analysis_results: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        从分析结果中提取特殊数据
        
        Parameters:
        -----------
        analysis_results : Dict[str, Any]
            分析结果字典
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            特殊数据字典，键为数据类型，值为DataFrame
        """
        special_data = {}
        
        # 提取话单数据
        if 'call' in analysis_results:
            call_data = analysis_results['call']
            
            # 话单原始数据
            if 'raw_data' in call_data:
                special_data['话单原始数据'] = call_data['raw_data']
            
            # 话单频率分析结果
            if 'frequency_analysis' in call_data:
                special_data['话单频率分析'] = call_data['frequency_analysis']
        
        # 提取账单数据
        if 'bill' in analysis_results:
            bill_data = analysis_results['bill']
            
            # 账单原始数据
            if 'raw_data' in bill_data:
                special_data['账单原始数据'] = bill_data['raw_data']
            
            # 账单频率分析结果
            if 'frequency_analysis' in bill_data:
                special_data['账单频率分析'] = bill_data['frequency_analysis']
        
        # 提取综合分析结果
        if 'comprehensive' in analysis_results:
            comprehensive_data = analysis_results['comprehensive']
            
            # 综合分析结果
            if 'analysis_result' in comprehensive_data:
                special_data['综合分析结果'] = comprehensive_data['analysis_result']
        
        return special_data
    
    def get_key_transaction_data(self, analysis_results: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        获取重点交易数据
        
        Parameters:
        -----------
        analysis_results : Dict[str, Any]
            分析结果字典
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            重点交易数据字典，键为数据类型，值为DataFrame
        """
        key_transaction_data = {}
        
        # 从分析结果中提取重点交易数据
        if 'key_transactions' in analysis_results:
            key_transactions = analysis_results['key_transactions']
            
            # 重点收入数据
            if 'key_income' in key_transactions:
                key_transaction_data['重点收入'] = key_transactions['key_income']
            
            # 重点支出数据
            if 'key_expense' in key_transactions:
                key_transaction_data['重点支出'] = key_transactions['key_expense']
            
            # 重点交易汇总
            if 'summary' in key_transactions:
                key_transaction_data['重点交易汇总'] = key_transactions['summary']
        
        return key_transaction_data
    
    def extract_company_info(self, call_df: pd.DataFrame) -> pd.DataFrame:
        """
        从通话数据中提取单位信息
        
        Parameters:
        -----------
        call_df : pd.DataFrame
            通话数据DataFrame
            
        Returns:
        --------
        pd.DataFrame
            包含单位信息的DataFrame
        """
        if call_df.empty:
            return pd.DataFrame()
        
        # 检查是否存在单位相关字段
        company_fields = []
        if '对方单位名称' in call_df.columns:
            company_fields.append('对方单位名称')
        if '对方职务' in call_df.columns:
            company_fields.append('对方职务')
        if '对方号码' in call_df.columns:
            company_fields.append('对方号码')
        
        if not company_fields:
            return pd.DataFrame()
        
        # 按对方姓名分组，提取单位信息
        agg_dict = {}
        for field in company_fields:
            agg_dict[field] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        
        # 添加通话次数统计（使用count统计记录数）
        # 使用更灵活的列名处理，避免硬编码
        call_count_column = '通话次数'
        # 只在通话次数列存在时才添加统计
        if call_count_column in call_df.columns:
            agg_dict[call_count_column] = 'count'
        
        # 检查通话时长字段
        if '通话时长' in call_df.columns:
            agg_dict['通话时长'] = 'sum'
        elif '通话总时长(分钟)' in call_df.columns:
            agg_dict['通话总时长(分钟)'] = 'sum'
        
        # 检查对方姓名列是否存在
        if '对方姓名' not in call_df.columns:
            # 如果对方姓名列不存在，尝试使用其他标识列
            if '对方号码' in call_df.columns:
                group_by_column = '对方号码'
            elif '对方' in call_df.columns:
                group_by_column = '对方'
            else:
                # 如果没有合适的标识列，返回空DataFrame
                return pd.DataFrame()
        else:
            group_by_column = '对方姓名'
        
        company_info = call_df.groupby(group_by_column).agg(agg_dict).reset_index()
        
        return company_info
    
    def add_company_info_to_dataframe(self, df: pd.DataFrame, company_info: pd.DataFrame) -> pd.DataFrame:
        """
        为DataFrame添加单位信息列
        
        Parameters:
        -----------
        df : pd.DataFrame
            目标DataFrame
        company_info : pd.DataFrame
            单位信息DataFrame
            
        Returns:
        --------
        pd.DataFrame
            添加了单位信息的DataFrame
        """
        if df.empty or company_info.empty:
            return df
        
        # 检查DataFrame中是否有对方姓名字段
        if '对方姓名' not in df.columns:
            return df
        
        # 合并单位信息
        result_df = pd.merge(df, company_info, on='对方姓名', how='left')
        
        # 填充空值
        for col in company_info.columns:
            if col != '对方姓名' and col in result_df.columns:
                if col in ['对方单位名称', '对方职务', '对方号码']:
                    result_df[col] = result_df[col].fillna('')
                else:
                    result_df[col] = result_df[col].fillna(0)
        
        return result_df
    
    def standardize_frequency_table(self, df: pd.DataFrame, table_type: str = 'bill') -> pd.DataFrame:
        """
        统一频率表字段结构
        
        Parameters:
        -----------
        df : pd.DataFrame
            频率表DataFrame
        table_type : str
            表类型：'bill'（账单）或'call'（话单）
            
        Returns:
        --------
        pd.DataFrame
            标准化后的频率表
        """
        if df.empty:
            return df
        
        result_df = df.copy()
        
        if table_type == 'bill':
            # 账单类频率表标准化
            # 确保包含收入总额、支出总额、交易次数字段
            if '收入总额' not in result_df.columns:
                result_df['收入总额'] = 0
            if '支出总额' not in result_df.columns:
                result_df['支出总额'] = 0
            if '交易次数' not in result_df.columns:
                result_df['交易次数'] = 0
            
            # 计算收入占比和支出占比
            total_income = result_df['收入总额'].sum()
            total_expense = result_df['支出总额'].sum()
            
            if total_income > 0:
                result_df['收入占比'] = (result_df['收入总额'] / total_income * 100).round(2)
            else:
                result_df['收入占比'] = 0
            
            if total_expense > 0:
                result_df['支出占比'] = (result_df['支出总额'] / total_expense * 100).round(2)
            else:
                result_df['支出占比'] = 0
            
            # 添加净收入字段
            result_df['净收入'] = result_df['收入总额'] - result_df['支出总额']
            
        elif table_type == 'call':
            # 话单类频率表标准化
            # 确保包含通话次数字段
            if '通话次数' not in result_df.columns:
                result_df['通话次数'] = 0
            
            # 检查通话时长字段
            if '通话时长' not in result_df.columns and '通话总时长(分钟)' not in result_df.columns:
                result_df['通话总时长(分钟)'] = 0
            
            # 计算通话次数占比（只在通话次数列存在时计算）
            if '通话次数' in result_df.columns:
                total_calls = result_df['通话次数'].sum()
                if total_calls > 0:
                    result_df['通话次数占比'] = (result_df['通话次数'] / total_calls * 100).round(2)
                else:
                    result_df['通话次数占比'] = 0
            else:
                result_df['通话次数占比'] = 0
            
            # 计算平均通话时长（只在通话次数列存在时计算）
            if '通话次数' in result_df.columns:
                total_calls = result_df['通话次数'].sum()
                if '通话时长' in result_df.columns and total_calls > 0:
                    result_df['平均通话时长'] = (result_df['通话时长'] / result_df['通话次数']).round(2)
                elif '通话总时长(分钟)' in result_df.columns and total_calls > 0:
                    result_df['平均通话时长(分钟)'] = (result_df['通话总时长(分钟)'] / result_df['通话次数']).round(2)
                else:
                    result_df['平均通话时长'] = 0
        
        return result_df
    
    def standardize_call_frequency_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        统一话单类频率表字段结构
        
        Parameters:
        -----------
        df : pd.DataFrame
            话单频率表DataFrame
            
        Returns:
        --------
        pd.DataFrame
            标准化后的话单频率表
        """
        return self.standardize_frequency_table(df, 'call')
    
    def get_platform_based_analysis_data(self, analysis_results: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        获取各平台基准交叉分析数据
        
        Parameters:
        -----------
        analysis_results : Dict[str, Any]
            分析结果字典
            
        Returns:
        --------
        Dict[str, pd.DataFrame]
            平台基准分析数据字典
        """
        platform_analysis_data = {}
        
        # 从分析结果中提取平台基准分析数据
        if 'platform_analysis' in analysis_results:
            platform_analysis = analysis_results['platform_analysis']
            
            # 银行基准分析
            if 'bank_based' in platform_analysis:
                platform_analysis_data['银行基准分析'] = platform_analysis['bank_based']
            
            # 微信基准分析
            if 'wechat_based' in platform_analysis:
                platform_analysis_data['微信基准分析'] = platform_analysis['wechat_based']
            
            # 支付宝基准分析
            if 'alipay_based' in platform_analysis:
                platform_analysis_data['支付宝基准分析'] = platform_analysis['alipay_based']
        
        return platform_analysis_data