#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import List, Dict, Tuple, Optional
import os

from src.base import BaseDataModel
from src.utils.config import Config
from src.utils.cash_recognition import CashRecognitionEngine

class BankDataModel(BaseDataModel):
    """
    银行数据模型，用于加载和处理银行交易数据
    """
    def __init__(self, data_path=None, data=None, config=None):
        """
        初始化银行数据模型
        
        Parameters:
        -----------
        data_path : str, optional
            数据文件路径，如果提供则从文件加载数据
        data : pd.DataFrame, optional
            直接提供的数据，如果提供则使用此数据
        config : Config, optional
            配置对象，如果不提供则使用默认配置
        """
        self.config = config or Config()
        
        # 定义列名配置
        self.name_column = self.config.get('data_sources.bank.name_column', '本方姓名')
        self.date_column = self.config.get('data_sources.bank.date_column', '交易日期')
        self.amount_column = self.config.get('data_sources.bank.amount_column', '交易金额')
        self.balance_column = self.config.get('data_sources.bank.balance_column', '账户余额')
        self.type_column = self.config.get('data_sources.bank.type_column', '交易类型')
        self.summary_column = self.config.get('data_sources.bank.summary_column', '交易摘要')
        self.remark_column = self.config.get('data_sources.bank.remark_column', '交易备注')
        self.direction_column = self.config.get('data_sources.bank.direction_column', '借贷标识')
        self.opposite_name_column = self.config.get('data_sources.bank.opposite_name_column', '对方姓名')
        self.special_date_column = self.config.get('data_sources.bank.special_date_column', '特殊日期名称')
        self.income_flag = self.config.get('data_sources.bank.income_flag', '贷')    # 收入
        self.expense_flag = self.config.get('data_sources.bank.expense_flag', '借')   # 支出
        self.bank_name_column = self.config.get('data_sources.bank.bank_name_column', '银行类型')
        self.account_column = self.config.get('data_sources.bank.account_column', '本方账号')
        
        # 定义必需的列
        self.required_columns = [
            self.name_column,
            self.date_column,
            self.amount_column,
            self.direction_column
        ]
        
        # 存取现关键词（修正配置路径）
        self.deposit_keywords = self.config.get('data_sources.bank.deposit_keywords', ['存', '现金存', '柜台存', '存款', '现金存入', '存现'])
        self.withdraw_keywords = self.config.get('data_sources.bank.withdraw_keywords', ['取', '现金取', '柜台取', 'ATM取', '取款', '现金支取', '取现'])
        self.deposit_exclude_keywords = self.config.get('data_sources.bank.deposit_exclude_keywords', ['转存', '存息', '利息存入'])
        self.withdraw_exclude_keywords = self.config.get('data_sources.bank.withdraw_exclude_keywords', ['转取', '利息取出', '息取'])

        # 增强识别配置
        self.enable_enhanced_algorithm = self.config.get('analysis.cash.recognition.enable_enhanced_algorithm', True)
        self.confidence_threshold = self.config.get('analysis.cash.recognition.confidence_threshold', 0.5)
        self.high_priority_confidence = self.config.get('analysis.cash.recognition.high_priority_confidence', 0.95)
        self.medium_priority_confidence = self.config.get('analysis.cash.recognition.medium_priority_confidence', 0.8)
        self.low_priority_confidence = self.config.get('analysis.cash.recognition.low_priority_confidence', 0.6)
        self.large_amount_threshold = self.config.get('analysis.cash.recognition.large_amount_threshold', 100000)
        self.small_amount_threshold = self.config.get('analysis.cash.recognition.small_amount_threshold', 10)
        self.enable_fuzzy_matching = self.config.get('analysis.cash.recognition.enable_fuzzy_matching', True)
        self.enable_amount_analysis = self.config.get('analysis.cash.recognition.enable_amount_analysis', True)
        self.enable_time_analysis = self.config.get('analysis.cash.recognition.enable_time_analysis', False)
        self.common_cash_amounts = self.config.get('analysis.cash.recognition.common_cash_amounts', [100, 200, 300, 500, 1000, 2000, 3000, 5000, 10000, 20000, 50000])
        self.round_amount_modulos = self.config.get('analysis.cash.recognition.round_amount_modulos', [50, 100])
        self.high_priority_deposit_keywords = self.config.get('analysis.cash.recognition.high_priority_deposit_keywords', ['ATM存现', 'CRS无卡存现', '柜台存现'])
        self.high_priority_withdraw_keywords = self.config.get('analysis.cash.recognition.high_priority_withdraw_keywords', ['ATM取现', 'CRS无卡取现', '柜台取现'])

        # 初始化存取现识别引擎
        self.cash_recognition_engine = CashRecognitionEngine(self.config)

        # 调用父类初始化
        super().__init__(data_path, data)
    
    def _safe_to_numeric(self, series):
        """安全地将Series转换为数值类型，无法转换的填充为0"""
        return pd.to_numeric(series, errors='coerce').fillna(0)

    def preprocess(self):
        """
        数据预处理
        """
        # 1. 统一借贷列
        # 检查是否存在 '借' 和 '贷' 列
        debit_col = next((col for col in self.data.columns if col in ['借', '借方发生额']), None)
        credit_col = next((col for col in self.data.columns if col in ['贷', '贷方发生额']), None)

        if debit_col and credit_col and self.amount_column not in self.data.columns:
            self.logger.info(f"检测到 '{debit_col}' 和 '{credit_col}' 列，将它们合并为 '{self.amount_column}' 和 '{self.direction_column}'")
            
            # 安全地转换为数值类型
            debit_values = self._safe_to_numeric(self.data[debit_col])
            credit_values = self._safe_to_numeric(self.data[credit_col])
            
            # 创建 '交易金额' 和 '借贷标识'
            self.data[self.amount_column] = np.where(debit_values != 0, debit_values, credit_values)
            self.data[self.direction_column] = np.where(debit_values != 0, self.expense_flag, self.income_flag)
            
            # 如果金额为0，则标识可能不明确，可以设置一个默认或留空
            self.data.loc[self.data[self.amount_column] == 0, self.direction_column] = '未知'
            
        # 2. 确保核心列存在且类型正确
        # 确保日期列为日期类型
        if self.date_column in self.data.columns:
            self.data[self.date_column] = pd.to_datetime(self.data[self.date_column], errors='coerce')

        # 确保金额列为数值类型
        if self.amount_column in self.data.columns:
            self.data[self.amount_column] = self._safe_to_numeric(self.data[self.amount_column])

        # 确保账户余额列为数值类型
        if self.balance_column in self.data.columns:
            self.data[self.balance_column] = self._safe_to_numeric(self.data[self.balance_column])

        # 确保可选字段存在（如果不存在则创建默认值）
        if self.remark_column not in self.data.columns:
            self.data[self.remark_column] = ''
            self.logger.info(f"创建缺失的字段 '{self.remark_column}'")

        if self.summary_column not in self.data.columns:
            self.data[self.summary_column] = ''
            self.logger.info(f"创建缺失的字段 '{self.summary_column}'")

        if self.opposite_name_column not in self.data.columns:
            self.data[self.opposite_name_column] = '未知'
            self.logger.info(f"创建缺失的字段 '{self.opposite_name_column}'")
        
        # 3. 添加收入和支出列
        self.data['收入金额'] = 0.0
        self.data['支出金额'] = 0.0
        
        # 4. 添加存取现标识
        self.add_cash_operation_flag()
        
        # 5. 在标记完存取现后，处理剩余的转账交易的收入和支出
        self.process_transfer_income_expense()
        
        # 6. 添加银行名称列
        self.add_bank_name_column()
        
        # 7. 添加数据来源列
        if self.file_path and '数据来源' not in self.data.columns:
            source_name = os.path.splitext(os.path.basename(self.file_path))[0]
            self.data['数据来源'] = source_name
            self.logger.info(f"已添加 '数据来源' 列，值为 '{source_name}'")
        elif '数据来源' not in self.data.columns:
            self.data['数据来源'] = '银行数据' # 默认值
            self.logger.info("未找到文件路径，添加 '数据来源' 列，值为 '银行数据'")

        self.logger.info("银行数据预处理完成")
    
    def add_cash_operation_flag(self):
        """
        添加存取现标识，并处理存取现交易的收入和支出金额。
        使用统一的识别引擎，支持基础和增强两种识别模式。
        """
        # 准备列名配置
        columns_config = {
            'opposite_name_column': self.opposite_name_column,
            'summary_column': self.summary_column,
            'remark_column': self.remark_column,
            'type_column': getattr(self, 'type_column', None),
            'direction_column': self.direction_column,
            'amount_column': self.amount_column,
            'income_flag': self.income_flag,
            'expense_flag': self.expense_flag
        }

        # 使用识别引擎进行识别
        self.data = self.cash_recognition_engine.recognize_cash_operations(self.data, columns_config)

        # 记录识别统计信息
        stats = self.cash_recognition_engine.get_recognition_stats(self.data)
        self.logger.info(f"存取现识别完成: {stats}")

        self.logger.info("存取现标识添加完成")

    def process_transfer_income_expense(self):
        """
        为被标识为'转账'的交易记录分配收入和支出金额。
        """
        transfer_mask = self.data['存取现标识'] == '转账'
        
        if self.direction_column in self.data.columns:
            # 根据借贷标识判断
            income_mask = transfer_mask & (self.data[self.direction_column] == self.income_flag)
            expense_mask = transfer_mask & (self.data[self.direction_column] == self.expense_flag)

            self.data.loc[income_mask, '收入金额'] = self.data.loc[income_mask, self.amount_column].abs()
            self.data.loc[expense_mask, '支出金额'] = self.data.loc[expense_mask, self.amount_column].abs()
        else:
            # 根据金额正负判断
            income_mask = transfer_mask & (self.data[self.amount_column] > 0)
            expense_mask = transfer_mask & (self.data[self.amount_column] < 0)

            self.data.loc[income_mask, '收入金额'] = self.data.loc[income_mask, self.amount_column]
            self.data.loc[expense_mask, '支出金额'] = self.data.loc[expense_mask, self.amount_column].abs()
            
        self.logger.info("转账交易的收入和支出列处理完成")
    
    def add_bank_name_column(self):
        """
        添加银行名称列，确保银行名称信息被正确提取
        
        从示例数据看，银行类型列已经包含了银行名称信息，如"建设银行"
        """
        # 如果已经有银行类型列，确保它不为空
        if self.bank_name_column in self.data.columns:
            # 检查是否有空值
            empty_banks = self.data[self.bank_name_column].isna().sum()
            if empty_banks > 0:
                self.logger.warning(f"发现 {empty_banks} 条记录的银行类型为空")
        else:
            # 如果没有银行类型列，尝试从交易机构名称或对方银行名称中提取
            self.logger.warning(f"数据中没有 {self.bank_name_column} 列，尝试从其他字段提取银行信息")
            
            # 尝试从交易机构名称提取
            if '交易机构名称' in self.data.columns:
                # 提取银行名称
                self.data[self.bank_name_column] = self.data['交易机构名称'].apply(self._extract_bank_from_institution)
            # 尝试从对方银行名称提取
            elif '对方银行名称' in self.data.columns:
                self.data[self.bank_name_column] = self.data['对方银行名称']
            # 尝试从账号中提取
            elif self.account_column in self.data.columns:
                self.data[self.bank_name_column] = self.data[self.account_column].apply(self._extract_bank_from_account)
            else:
                self.logger.warning(f"无法从数据中提取银行信息")
                # 添加一个默认的银行类型列
                self.data[self.bank_name_column] = "未知银行"
    
    def _extract_bank_from_institution(self, institution_name):
        """
        从交易机构名称中提取银行名称
        
        Parameters:
        -----------
        institution_name : str
            交易机构名称
            
        Returns:
        --------
        str
            银行名称
        """
        if not institution_name or not isinstance(institution_name, str):
            return "未知银行"
        
        # 常见银行名称映射
        bank_keywords = {
            '建行': '建设银行',
            '工行': '工商银行',
            '农行': '农业银行',
            '中行': '中国银行',
            '交行': '交通银行',
            '招行': '招商银行',
            '邮储': '邮政储蓄银行'
        }
        
        # 直接包含银行名称的情况
        for bank in ['建设银行', '工商银行', '农业银行', '中国银行', '交通银行', '招商银行', '浦发银行', '民生银行', '兴业银行', '邮政储蓄银行']:
            if bank in institution_name:
                return bank
        
        # 使用简称匹配
        for keyword, bank in bank_keywords.items():
            if keyword in institution_name:
                return bank
        
        # 提取冒号后的分行信息
        if ':' in institution_name:
            parts = institution_name.split(':')
            if len(parts) > 1 and '银行' in parts[1]:
                return parts[1].split('银行')[0] + '银行'
        
        return "未知银行"
    
    def _extract_bank_from_account(self, account):
        """
        从账号中提取银行名称
        
        Parameters:
        -----------
        account : str
            账号
            
        Returns:
        --------
        str
            银行名称
        """
        if not account or not isinstance(account, str):
            return "未知银行"
        
        # 常见银行卡号前缀映射
        bank_prefixes = {
            '622848': '农业银行',
            '622700': '建设银行',
            '621700': '建设银行',
            '621661': '建设银行',
            '621226': '工商银行',
            '622202': '工商银行',
            '622262': '交通银行',
            '622666': '中国银行',
            '622622': '中国银行',
            '622588': '招商银行',
            '621286': '招商银行',
            '622155': '浦发银行',
            '622169': '浦发银行',
            '622516': '浦发银行',
            '622916': '民生银行',
            '622918': '民生银行',
            '622909': '兴业银行',
            '622908': '兴业银行',
            '621095': '邮政储蓄银行',
            '620062': '邮政储蓄银行',
            '623218': '邮政储蓄银行',
            # 从示例数据中看到的卡号
            '6217002': '建设银行',
            '6227002': '建设银行',
            '4367422': '建设银行'
        }
        
        # 尝试从账号中提取银行名称
        for prefix, bank in bank_prefixes.items():
            if str(account).startswith(prefix):
                return bank
        
        return "未知银行"
    
    def get_persons(self) -> List[str]:
        """
        获取所有人名
        
        Returns:
        --------
        List[str]
            所有人名列表
        """
        if self.name_column not in self.data.columns:
            return []
        
        return self.data[self.name_column].dropna().unique().tolist()
    
    def get_opposite_persons(self) -> List[str]:
        """
        获取所有对方人名
        
        Returns:
        --------
        List[str]
            所有对方人名列表
        """
        if self.opposite_name_column not in self.data.columns:
            return []
        
        return self.data[self.opposite_name_column].dropna().unique().tolist()
    
    def get_data_by_person(self, person_name: str) -> pd.DataFrame:
        """
        按人名筛选数据
        
        Parameters:
        -----------
        person_name : str
            人名
            
        Returns:
        --------
        pd.DataFrame
            筛选后的数据
        """
        return self.filter_by_value(self.name_column, person_name)
    
    def get_transfer_data(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取转账数据（对方姓名不为空的交易）
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只返回该人的转账数据
            
        Returns:
        --------
        pd.DataFrame
            转账数据
        """
        if self.opposite_name_column not in self.data.columns:
            return pd.DataFrame()
        
        # 转账条件：对方姓名不为空
        transfer_mask = self.data[self.opposite_name_column].notna() & (self.data[self.opposite_name_column] != '')
        
        # 如果提供了人名，再按人名筛选
        if person_name:
            person_mask = self.data[self.name_column] == person_name
            transfer_mask = transfer_mask & person_mask
        
        return self.data[transfer_mask]
    
    def get_cash_data(self, person_name: Optional[str] = None, cash_type: Optional[str] = None) -> pd.DataFrame:
        """
        获取存取现数据
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只返回该人的存取现数据
        cash_type : str, optional
            存取现类型，可选值为'存现'或'取现'，如果不提供则返回所有存取现数据
            
        Returns:
        --------
        pd.DataFrame
            存取现数据
        """
        # 存取现条件：存取现标识不为空
        cash_mask = self.data['存取现标识'] != ''
        
        # 如果提供了存取现类型，再按类型筛选
        if cash_type:
            cash_mask = self.data['存取现标识'] == cash_type
        
        # 如果提供了人名，再按人名筛选
        if person_name:
            person_mask = self.data[self.name_column] == person_name
            cash_mask = cash_mask & person_mask
        
        return self.data[cash_mask]
    
    def get_deposit_data(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取存现数据
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只返回该人的存现数据
            
        Returns:
        --------
        pd.DataFrame
            存现数据
        """
        return self.get_cash_data(person_name, '存现')
    
    def get_withdraw_data(self, person_name: Optional[str] = None) -> pd.DataFrame:
        """
        获取取现数据
        
        Parameters:
        -----------
        person_name : str, optional
            人名，如果提供则只返回该人的取现数据
            
        Returns:
        --------
        pd.DataFrame
            取现数据
        """
        return self.get_cash_data(person_name, '取现') 

    def load_data(self, data_path: Optional[str] = None):
        """
        加载数据
        
        Parameters:
        -----------
        data_path : str, optional
            数据文件路径，如果提供则从文件加载数据
        """
        if data_path:
            self.file_path = data_path
        
        try:
            # 尝试使用多种方式加载，不在这里强制转换类型
            try:
                self.data = pd.read_excel(self.file_path)
            except Exception:
                try:
                    self.data = pd.read_excel(self.file_path, engine='openpyxl')
                except Exception:
                    self.data = pd.read_csv(self.file_path, encoding='gbk', sep='\t')

            # 添加数据来源列
            if '数据来源' not in self.data.columns and self.file_path:
                self.data['数据来源'] = os.path.basename(self.file_path)

            self.logger.info("数据加载完成")
        except Exception as e:
            self.logger.error(f"数据加载失败，错误信息：{e}")
            # 创建一个空的DataFrame，防止后续代码出错
            self.data = pd.DataFrame()
            # 可以在这里重新抛出异常，或者让调用者处理空数据
            raise
        
        if not self.data.empty:
            self.validate()
            self.preprocess() 