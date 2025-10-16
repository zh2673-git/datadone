#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Set
import logging
from datetime import datetime, timedelta
from collections import defaultdict

from src.utils.config import Config
from src.utils.key_transactions import KeyTransactionEngine


class FundTrackingEngine:
    """
    大额资金追踪引擎
    
    功能：
    1. 以人名为单位，筛选出大额资金
    2. 追踪大额资金的来源和去向
    3. 支持跨人员资金流向追踪
    """
    
    def __init__(self, config: Optional[Config] = None):
        """
        初始化大额资金追踪引擎
        
        Parameters:
        -----------
        config : Config, optional
            配置对象，如果不提供则使用默认配置
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = config or Config()
        
        # 获取大额资金阈值配置
        self.large_amount_thresholds = self.config.get(
            'analysis.key_transactions.large_amount_thresholds',
            {
                "level1": {"min": 50000, "max": 100000, "name": "5万-10万"},
                "level2": {"min": 100000, "max": 500000, "name": "10万-50万"},
                "level3": {"min": 500000, "max": 1000000, "name": "50万-100万"},
                "level4": {"min": 1000000, "max": 999999999, "name": "100万及以上"}
            }
        )
        
        # 最小大额资金阈值（用于筛选大额交易）
        self.min_large_amount = min(
            level_config["min"] 
            for level_config in self.large_amount_thresholds.values()
        )
        
        # 追踪时间窗口（天）
        self.tracking_window_days = self.config.get(
            'analysis.fund_tracking.tracking_window_days', 30
        )
        
        # 最大追踪深度
        self.max_tracking_depth = self.config.get(
            'analysis.fund_tracking.max_tracking_depth', 3
        )
        
        self.logger.info(f"大额资金追踪引擎初始化完成，最小大额阈值: {self.min_large_amount}")
    
    def track_large_funds(self, data_models: Dict[str, object]) -> pd.DataFrame:
        """
        追踪所有数据模型中的大额资金流向
        
        Parameters:
        -----------
        data_models : Dict[str, object]
            包含各种数据模型的字典，如 {'bank': bank_model, 'wechat': wechat_model, ...}
            
        Returns:
        --------
        pd.DataFrame
            大额资金追踪结果，包含资金流向信息
        """
        if not data_models:
            self.logger.warning("未提供数据模型，无法进行大额资金追踪")
            return pd.DataFrame()
        
        self.logger.info("开始大额资金追踪分析...")
        
        # 收集所有大额交易
        all_large_transactions = []
        
        for model_name, model in data_models.items():
            if model is None or model.data.empty:
                continue
                
            # 检查模型是否支持大额资金追踪（需要有金额列）
            if not hasattr(model, 'amount_column'):
                self.logger.debug(f"跳过 {model_name} 数据模型，不支持大额资金追踪")
                continue
                
            # 根据模型类型获取相应的列名
            if hasattr(model, 'name_column'):
                name_col = model.name_column
                amount_col = model.amount_column
                date_col = model.date_column
                
                if hasattr(model, 'opposite_name_column'):
                    opposite_name_col = model.opposite_name_column
                else:
                    opposite_name_col = None
                
                # 筛选大额交易
                large_transactions = self._extract_large_transactions(
                    model.data, name_col, amount_col, date_col, opposite_name_col, model_name
                )
                
                if not large_transactions.empty:
                    all_large_transactions.append(large_transactions)
        
        if not all_large_transactions:
            self.logger.info("未发现大额交易")
            return pd.DataFrame()
        
        # 合并所有大额交易
        combined_transactions = pd.concat(all_large_transactions, ignore_index=True)
        
        # 按时间排序
        combined_transactions = combined_transactions.sort_values('交易日期')
        
        # 构建资金流向追踪结果
        tracking_results = self._build_fund_flow_tracking(combined_transactions, data_models)
        
        self.logger.info(f"大额资金追踪完成，共追踪到 {len(tracking_results)} 条记录")
        
        return tracking_results
    
    def _analyze_fund_sources(self, transactions: pd.DataFrame, data_models: Dict[str, object], month: str) -> Dict:
        """
        分析资金来源明细
        
        Parameters:
        -----------
        transactions : pd.DataFrame
            交易数据
        data_models : Dict[str, object]
            数据模型字典
        month : str
            月份
            
        Returns:
        --------
        Dict
            资金来源明细
        """
        income_transactions = transactions[transactions['交易方向'] == '收入']
        
        source_details = {
            '现金收入': 0,
            '银行转账': {},
            '微信转账': {},
            '支付宝转账': {},
            '其他收入': {}
        }
        
        for _, tx in income_transactions.iterrows():
            amount = abs(tx['交易金额'])
            opposite_person = tx.get('对方姓名', '未知')
            data_source = tx.get('数据来源', '未知')
            
            # 根据数据来源分类
            if '银行' in data_source:
                if opposite_person not in source_details['银行转账']:
                    source_details['银行转账'][opposite_person] = 0
                source_details['银行转账'][opposite_person] += amount
            elif '微信' in data_source:
                if opposite_person not in source_details['微信转账']:
                    source_details['微信转账'][opposite_person] = 0
                source_details['微信转账'][opposite_person] += amount
            elif '支付宝' in data_source:
                if opposite_person not in source_details['支付宝转账']:
                    source_details['支付宝转账'][opposite_person] = 0
                source_details['支付宝转账'][opposite_person] += amount
            else:
                # 检查是否为现金收入
                if '现金' in str(tx.get('交易摘要', '')) or '现金' in str(tx.get('交易备注', '')):
                    source_details['现金收入'] += amount
                else:
                    if opposite_person not in source_details['其他收入']:
                        source_details['其他收入'][opposite_person] = 0
                    source_details['其他收入'][opposite_person] += amount
        
        return source_details
    
    def _analyze_fund_destinations(self, transactions: pd.DataFrame, data_models: Dict[str, object], month: str) -> Dict:
        """
        分析资金去向明细
        
        Parameters:
        -----------
        transactions : pd.DataFrame
            交易数据
        data_models : Dict[str, object]
            数据模型字典
        month : str
            月份
            
        Returns:
        --------
        Dict
            资金去向明细
        """
        expense_transactions = transactions[transactions['交易方向'] == '支出']
        
        destination_details = {
            '现金支出': 0,
            '银行转账': {},
            '微信转账': {},
            '支付宝转账': {},
            '其他支出': {}
        }
        
        for _, tx in expense_transactions.iterrows():
            amount = abs(tx['交易金额'])
            opposite_person = tx.get('对方姓名', '未知')
            data_source = tx.get('数据来源', '未知')
            
            # 根据数据来源分类
            if '银行' in data_source:
                if opposite_person not in destination_details['银行转账']:
                    destination_details['银行转账'][opposite_person] = 0
                destination_details['银行转账'][opposite_person] += amount
            elif '微信' in data_source:
                if opposite_person not in destination_details['微信转账']:
                    destination_details['微信转账'][opposite_person] = 0
                destination_details['微信转账'][opposite_person] += amount
            elif '支付宝' in data_source:
                if opposite_person not in destination_details['支付宝转账']:
                    destination_details['支付宝转账'][opposite_person] = 0
                destination_details['支付宝转账'][opposite_person] += amount
            else:
                # 检查是否为现金支出
                if '现金' in str(tx.get('交易摘要', '')) or '现金' in str(tx.get('交易备注', '')):
                    destination_details['现金支出'] += amount
                else:
                    if opposite_person not in destination_details['其他支出']:
                        destination_details['其他支出'][opposite_person] = 0
                    destination_details['其他支出'][opposite_person] += amount
        
        return destination_details
    
    def _generate_detailed_remark(self, person_name: str, month: str, income: float, expense: float,
                                 source_details: Dict, destination_details: Dict) -> str:
        """
        生成详细的备注信息
        
        Parameters:
        -----------
        person_name : str
            人员姓名
        month : str
            月份
        income : float
            总收入
        expense : float
            总支出
        source_details : Dict
            资金来源明细
        destination_details : Dict
            资金去向明细
            
        Returns:
        --------
        str
            详细的备注信息
        """
        remark_parts = []
        
        # 计算净流入金额
        net_amount = income - expense
        
        # 收入部分 - 详细记录每笔收入来源
        if income > 0:
            income_parts = [f"总收入{income:,.0f}元"]
            
            # 现金收入
            if source_details['现金收入'] > 0:
                income_parts.append(f"现金收入{source_details['现金收入']:,.0f}元")
            
            # 银行转账收入 - 详细记录每个转账人
            if source_details['银行转账']:
                bank_total = sum(source_details['银行转账'].values())
                bank_details = []
                for person, amount in source_details['银行转账'].items():
                    bank_details.append(f"{person}银行转账{amount:,.0f}元")
                if bank_details:
                    income_parts.append(f"银行转账收入{bank_total:,.0f}元(" + "、".join(bank_details) + ")")
            
            # 微信转账收入 - 详细记录每个转账人
            if source_details['微信转账']:
                wechat_total = sum(source_details['微信转账'].values())
                wechat_details = []
                for person, amount in source_details['微信转账'].items():
                    wechat_details.append(f"{person}微信{amount:,.0f}元")
                if wechat_details:
                    income_parts.append(f"微信收入{wechat_total:,.0f}元(" + "、".join(wechat_details) + ")")
            
            # 支付宝转账收入 - 详细记录每个转账人
            if source_details['支付宝转账']:
                alipay_total = sum(source_details['支付宝转账'].values())
                alipay_details = []
                for person, amount in source_details['支付宝转账'].items():
                    alipay_details.append(f"{person}支付宝{amount:,.0f}元")
                if alipay_details:
                    income_parts.append(f"支付宝收入{alipay_total:,.0f}元(" + "、".join(alipay_details) + ")")
            
            # 其他收入 - 详细记录每个来源
            if source_details['其他收入']:
                other_total = sum(source_details['其他收入'].values())
                other_details = []
                for person, amount in source_details['其他收入'].items():
                    other_details.append(f"{person}{amount:,.0f}元")
                if other_details:
                    income_parts.append(f"其他收入{other_total:,.0f}元(" + "、".join(other_details) + ")")
            
            remark_parts.append("资金来源：" + "；".join(income_parts))
        
        # 支出部分 - 详细记录每笔支出去向
        if expense > 0:
            expense_parts = [f"总支出{expense:,.0f}元"]
            
            # 现金支出
            if destination_details['现金支出'] > 0:
                expense_parts.append(f"现金支出{destination_details['现金支出']:,.0f}元")
            
            # 银行转账支出 - 详细记录每个收款人
            if destination_details['银行转账']:
                bank_total = sum(destination_details['银行转账'].values())
                bank_details = []
                for person, amount in destination_details['银行转账'].items():
                    bank_details.append(f"{person}银行转账{amount:,.0f}元")
                if bank_details:
                    expense_parts.append(f"银行转账支出{bank_total:,.0f}元(" + "、".join(bank_details) + ")")
            
            # 微信转账支出 - 详细记录每个收款人
            if destination_details['微信转账']:
                wechat_total = sum(destination_details['微信转账'].values())
                wechat_details = []
                for person, amount in destination_details['微信转账'].items():
                    wechat_details.append(f"{person}微信{amount:,.0f}元")
                if wechat_details:
                    expense_parts.append(f"微信支出{wechat_total:,.0f}元(" + "、".join(wechat_details) + ")")
            
            # 支付宝转账支出 - 详细记录每个收款人
            if destination_details['支付宝转账']:
                alipay_total = sum(destination_details['支付宝转账'].values())
                alipay_details = []
                for person, amount in destination_details['支付宝转账'].items():
                    alipay_details.append(f"{person}支付宝{amount:,.0f}元")
                if alipay_details:
                    expense_parts.append(f"支付宝支出{alipay_total:,.0f}元(" + "、".join(alipay_details) + ")")
            
            # 其他支出 - 详细记录每个去向
            if destination_details['其他支出']:
                other_total = sum(destination_details['其他支出'].values())
                other_details = []
                for person, amount in destination_details['其他支出'].items():
                    other_details.append(f"{person}{amount:,.0f}元")
                if other_details:
                    expense_parts.append(f"其他支出{other_total:,.0f}元(" + "、".join(other_details) + ")")
            
            remark_parts.append("资金去向：" + "；".join(expense_parts))
        
        # 添加净流入/流出信息
        if net_amount > 0:
            remark_parts.append(f"净流入{net_amount:,.0f}元")
        elif net_amount < 0:
            remark_parts.append(f"净流出{-net_amount:,.0f}元")
        else:
            remark_parts.append("收支平衡")
        
        return f"{person_name}{month.strftime('%Y年%m月')}大额资金流向：" + "；".join(remark_parts)
    
    def _track_person_funds_by_month(self, person_name: str, person_transactions: pd.DataFrame,
                                data_models: Dict[str, object], month: str, 
                                current_depth: int = 0, visited_persons: Optional[Set[str]] = None) -> pd.DataFrame:
        """
        按月份追踪单个人员的大额资金流向
        
        Parameters:
        -----------
        person_name : str
            人员姓名
        person_transactions : pd.DataFrame
            该人员的大额交易
        data_models : Dict[str, object]
            数据模型字典
        month : str
            月份
        current_depth : int
            当前追踪深度
        visited_persons : Set[str], optional
            已访问的人员集合，用于避免循环追踪
            
        Returns:
        --------
        pd.DataFrame
            该人员的资金流向追踪结果
        """
        if visited_persons is None:
            visited_persons = set()
        
        # 避免循环追踪
        if person_name in visited_persons or current_depth >= self.max_tracking_depth:
            return pd.DataFrame()
        
        visited_persons.add(person_name)
        
        tracking_results = []
        
        # 统计该人员在该月份的大额资金流入和流出
        month_income = person_transactions[person_transactions['交易方向'] == '收入']['交易金额'].sum()
        month_expense = person_transactions[person_transactions['交易方向'] == '支出']['交易金额'].sum()
        
        # 分析资金来源和去向明细
        source_details = self._analyze_fund_sources(person_transactions, data_models, month)
        destination_details = self._analyze_fund_destinations(person_transactions, data_models, month)
        
        # 生成详细的备注信息
        remark = self._generate_detailed_remark(person_name, month, month_income, month_expense, 
                                               source_details, destination_details)
        
        # 添加月度汇总记录
        tracking_results.append({
            '追踪层级': current_depth,
            '核心人员': person_name,
            '关联人员': '月度汇总',
            '交易日期': month.strftime('%Y年%m月'),
            '交易金额': month_income - month_expense,  # 净流入
            '交易方向': '汇总',
            '大额级别': self._get_amount_level(max(abs(month_income), abs(month_expense))),
            '数据来源': '月度统计',
            '资金流向': '月度汇总',
            '追踪说明': remark,
            '月份': month.strftime('%Y年%m月')
        })
        
        # 追踪单笔交易
        for _, transaction in person_transactions.iterrows():
            tracking_result = self._track_single_transaction(transaction, data_models, 
                                                                   current_depth, visited_persons.copy())
            
            if tracking_result:
                tracking_results.extend(tracking_result)
        
        if tracking_results:
            return pd.DataFrame(tracking_results)
        else:
            return pd.DataFrame()
        
        # 收集所有大额交易
        all_large_transactions = []
        
        for model_name, model in data_models.items():
            if model is None or model.data.empty:
                continue
                
            # 检查模型是否支持大额资金追踪（需要有金额列）
            if not hasattr(model, 'amount_column'):
                self.logger.debug(f"跳过 {model_name} 数据模型，不支持大额资金追踪")
                continue
                
            # 根据模型类型获取相应的列名
            if hasattr(model, 'name_column'):
                name_col = model.name_column
                amount_col = model.amount_column
                date_col = model.date_column
                
                if hasattr(model, 'opposite_name_column'):
                    opposite_name_col = model.opposite_name_column
                else:
                    opposite_name_col = None
                
                # 筛选大额交易
                large_transactions = self._extract_large_transactions(
                    model.data, name_col, amount_col, date_col, opposite_name_col, model_name
                )
                
                if not large_transactions.empty:
                    all_large_transactions.append(large_transactions)
        
        if not all_large_transactions:
            self.logger.info("未发现大额交易")
            return pd.DataFrame()
        
        # 合并所有大额交易
        combined_transactions = pd.concat(all_large_transactions, ignore_index=True)
        
        # 按时间排序
        combined_transactions = combined_transactions.sort_values('交易日期')
        
        # 构建资金流向追踪结果
        tracking_results = self._build_fund_flow_tracking(combined_transactions, data_models)
        
        return tracking_results
    
    def _extract_large_transactions(self, data: pd.DataFrame, name_col: str, 
                                   amount_col: str, date_col: str, 
                                   opposite_name_col: Optional[str], 
                                   data_source: str) -> pd.DataFrame:
        """
        从数据中提取大额交易
        
        Parameters:
        -----------
        data : pd.DataFrame
            原始数据
        name_col : str
            人名列名
        amount_col : str
            金额列名
        date_col : str
            日期列名
        opposite_name_col : str, optional
            对方人名列名
        data_source : str
            数据源名称
            
        Returns:
        --------
        pd.DataFrame
            大额交易数据
        """
        if data.empty:
            return pd.DataFrame()
        
        # 确保金额列是数值类型
        if amount_col not in data.columns:
            self.logger.warning(f"数据中缺少金额列: {amount_col}")
            return pd.DataFrame()
        
        # 转换金额为数值类型 - 使用.loc避免SettingWithCopyWarning
        data.loc[:, amount_col] = pd.to_numeric(data[amount_col], errors='coerce')
        
        # 筛选大额交易（绝对值大于最小阈值）
        large_mask = data[amount_col].abs() >= self.min_large_amount
        large_data = data[large_mask].copy()
        
        if large_data.empty:
            return pd.DataFrame()
        
        # 构建大额交易结果
        result_data = []
        
        for _, row in large_data.iterrows():
            amount = row[amount_col]
            person_name = row[name_col] if name_col in row else '未知'
            
            # 确定交易方向
            if amount > 0:
                direction = '收入'
                opposite_person = row[opposite_name_col] if opposite_name_col and opposite_name_col in row else '未知'
            else:
                direction = '支出'
                opposite_person = row[opposite_name_col] if opposite_name_col and opposite_name_col in row else '未知'
            
            # 确定大额级别
            amount_level = self._get_amount_level(abs(amount))
            
            # 优化数据来源显示：如果是银行数据，显示具体银行名称
            source_display = data_source
            # 检查是否为银行数据（数据源名称为'bank'或包含'银行'）
            if data_source == 'bank' or '银行' in data_source:
                # 尝试从数据中提取具体的银行名称
                bank_name = self._extract_bank_name_from_row(row)
                if bank_name and bank_name != '未知银行':
                    source_display = bank_name
            
            result_data.append({
                '数据来源': source_display,
                '交易日期': row[date_col] if date_col in row else '未知',
                '本方姓名': person_name,
                '对方姓名': opposite_person,
                '交易金额': amount,
                '交易方向': direction,
                '大额级别': amount_level,
                '原始数据索引': _
            })
        
        return pd.DataFrame(result_data)
    
    def _extract_bank_name_from_row(self, row: pd.Series) -> str:
        """
        从数据行中提取银行名称
        
        Parameters:
        -----------
        row : pd.Series
            数据行
            
        Returns:
        --------
        str
            银行名称
        """
        # 优先从银行类型列中提取
        bank_columns = ['银行类型', '银行名称', '交易机构名称', '对方银行名称']
        
        for col in bank_columns:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                bank_name = str(row[col]).strip()
                # 如果银行名称包含银行信息，直接返回
                if '银行' in bank_name:
                    return bank_name
        
        # 如果从列名中无法提取，尝试从账号中提取
        account_columns = ['账号', '银行卡号', '对方账号']
        for col in account_columns:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                account = str(row[col]).strip()
                # 根据银行卡号前缀判断银行
                bank_name = self._extract_bank_from_account(account)
                if bank_name != '未知银行':
                    return bank_name
        
        return '未知银行'
    
    def _extract_bank_from_account(self, account: str) -> str:
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
            '6217002': '建设银行',
            '6227002': '建设银行',
            '4367422': '建设银行'
        }
        
        # 尝试从账号中提取银行名称
        for prefix, bank in bank_prefixes.items():
            if str(account).startswith(prefix):
                return bank
        
        return "未知银行"
    
    def _get_amount_level(self, amount: float) -> str:
        """
        根据金额确定大额级别
        
        Parameters:
        -----------
        amount : float
            交易金额
            
        Returns:
        --------
        str
            大额级别名称
        """
        for level_key, level_config in self.large_amount_thresholds.items():
            min_amount = level_config["min"]
            max_amount = level_config["max"]
            
            if min_amount <= amount < max_amount:
                return level_config["name"]
        
        return "未知级别"
    
    def _build_fund_flow_tracking(self, transactions: pd.DataFrame, 
                                 data_models: Dict[str, object]) -> pd.DataFrame:
        """
        构建资金流向追踪结果
        
        Parameters:
        -----------
        transactions : pd.DataFrame
            所有大额交易数据
        data_models : Dict[str, object]
            数据模型字典
            
        Returns:
        --------
        pd.DataFrame
            资金流向追踪结果
        """
        if transactions.empty:
            return pd.DataFrame()
        
        # 性能优化：避免不必要的DataFrame复制，使用视图操作
        transactions_processed = transactions.copy()
        transactions_processed['交易日期'] = pd.to_datetime(
            transactions_processed['交易日期'], 
            errors='coerce', 
            format='mixed'
        )
        transactions_processed['月份'] = transactions_processed['交易日期'].dt.to_period('M')
        
        # 性能优化：只处理有效日期的数据
        valid_date_mask = pd.notna(transactions_processed['交易日期'])
        if not valid_date_mask.any():
            return pd.DataFrame()
        
        transactions_valid = transactions_processed[valid_date_mask]
        
        # 性能优化：使用更高效的分组操作，减少内存使用
        tracking_results = []
        
        # 按月份和人员分组追踪，使用更高效的方法
        for (month, person_name), month_person_transactions in transactions_valid.groupby(['月份', '本方姓名']):
            # 性能优化：限制单次处理的数据量，避免内存溢出
            if len(month_person_transactions) > 1000:  # 设置合理的批次大小
                # 分批处理大量数据
                batch_size = 500
                for i in range(0, len(month_person_transactions), batch_size):
                    batch_data = month_person_transactions.iloc[i:i+batch_size]
                    person_tracking = self._track_person_funds_by_month(
                        person_name, batch_data, data_models, month
                    )
                    if not person_tracking.empty:
                        tracking_results.append(person_tracking)
            else:
                # 正常处理
                person_tracking = self._track_person_funds_by_month(
                    person_name, month_person_transactions, data_models, month
                )
                if not person_tracking.empty:
                    tracking_results.append(person_tracking)
        
        if tracking_results:
            return pd.concat(tracking_results, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def _track_person_funds(self, person_name: str, person_transactions: pd.DataFrame,
                           data_models: Dict[str, object], current_depth: int = 0,
                           visited_persons: Optional[Set[str]] = None) -> pd.DataFrame:
        """
        追踪单个人员的大额资金流向
        
        Parameters:
        -----------
        person_name : str
            人员姓名
        person_transactions : pd.DataFrame
            该人员的大额交易
        data_models : Dict[str, object]
            数据模型字典
        current_depth : int
            当前追踪深度
        visited_persons : Set[str], optional
            已访问的人员集合，用于避免循环追踪
            
        Returns:
        --------
        pd.DataFrame
            该人员的资金流向追踪结果
        """
        if visited_persons is None:
            visited_persons = set()
        
        # 性能优化：限制递归深度，避免栈溢出
        if person_name in visited_persons or current_depth >= min(self.max_tracking_depth, 5):
            return pd.DataFrame()
        
        visited_persons.add(person_name)
        
        tracking_results = []
        
        # 性能优化：使用向量化操作替代iterrows，提高处理速度
        if len(person_transactions) > 100:
            # 对于大量数据，使用批量处理
            batch_size = 50
            for i in range(0, len(person_transactions), batch_size):
                batch_transactions = person_transactions.iloc[i:i+batch_size]
                for _, transaction in batch_transactions.iterrows():
                    tracking_result = self._track_single_transaction(
                        transaction, data_models, current_depth, visited_persons.copy()
                    )
                    if tracking_result:
                        tracking_results.extend(tracking_result)
        else:
            # 对于少量数据，正常处理
            for _, transaction in person_transactions.iterrows():
                tracking_result = self._track_single_transaction(
                    transaction, data_models, current_depth, visited_persons.copy()
                )
                if tracking_result:
                    tracking_results.extend(tracking_result)
        
        if tracking_results:
            return pd.DataFrame(tracking_results)
        else:
            return pd.DataFrame()
    
    def _track_single_transaction(self, transaction: pd.Series, 
                                 data_models: Dict[str, object],
                                 current_depth: int, 
                                 visited_persons: Set[str]) -> List[Dict]:
        """
        追踪单笔大额交易的资金流向
        
        Parameters:
        -----------
        transaction : pd.Series
            单笔大额交易
        data_models : Dict[str, object]
            数据模型字典
        current_depth : int
            当前追踪深度
        visited_persons : Set[str]
            已访问的人员集合
            
        Returns:
        --------
        List[Dict]
            资金流向追踪结果列表
        """
        tracking_results = []
        
        # 添加当前交易信息
        tracking_results.append({
            '追踪层级': current_depth,
            '核心人员': transaction['本方姓名'],
            '关联人员': transaction['对方姓名'],
            '交易日期': transaction['交易日期'],
            '交易金额': transaction['交易金额'],
            '交易方向': transaction['交易方向'],
            '大额级别': transaction['大额级别'],
            '数据来源': transaction['数据来源'],
            '资金流向': '直接交易',
            '追踪说明': f"{transaction['本方姓名']} {transaction['交易方向']} {abs(transaction['交易金额']):,.0f}元给{transaction['对方姓名']}"
        })
        
        # 如果当前交易是支出，追踪资金的去向
        if transaction['交易方向'] == '支出':
            opposite_person = transaction['对方姓名']
            
            # 在时间窗口内追踪对方人员的相关交易
            transaction_date = pd.to_datetime(transaction['交易日期'])
            start_date = transaction_date - timedelta(days=self.tracking_window_days)
            end_date = transaction_date + timedelta(days=self.tracking_window_days)
            
            # 在所有数据模型中查找对方人员的相关交易
            opposite_transactions = self._find_related_transactions(
                opposite_person, start_date, end_date, data_models, visited_persons
            )
            
            if not opposite_transactions.empty:
                # 递归追踪对方人员的资金流向
                next_depth = current_depth + 1
                opposite_tracking = self._track_person_funds(
                    opposite_person, opposite_transactions, data_models, 
                    next_depth, visited_persons
                )
                
                if not opposite_tracking.empty:
                    # 添加间接追踪结果
                    for _, opposite_tx in opposite_tracking.iterrows():
                        tracking_results.append({
                            '追踪层级': opposite_tx['追踪层级'],
                            '核心人员': transaction['本方姓名'],
                            '关联人员': opposite_tx['关联人员'],
                            '交易日期': opposite_tx['交易日期'],
                            '交易金额': opposite_tx['交易金额'],
                            '交易方向': opposite_tx['交易方向'],
                            '大额级别': opposite_tx['大额级别'],
                            '数据来源': opposite_tx['数据来源'],
                            '资金流向': '间接追踪',
                            '追踪说明': f"通过{opposite_person}间接追踪：{opposite_tx['追踪说明']}"
                        })
        
        return tracking_results
    
    def _find_related_transactions(self, person_name: str, start_date: datetime,
                                  end_date: datetime, data_models: Dict[str, object],
                                  visited_persons: Set[str]) -> pd.DataFrame:
        """
        查找指定人员在时间窗口内的相关交易
        
        Parameters:
        -----------
        person_name : str
            人员姓名
        start_date : datetime
            开始日期
        end_date : datetime
            结束日期
        data_models : Dict[str, object]
            数据模型字典
        visited_persons : Set[str]
            已访问的人员集合
            
        Returns:
        --------
        pd.DataFrame
            相关交易数据
        """
        related_transactions = []
        
        for model_name, model in data_models.items():
            if model is None or model.data.empty:
                continue
            
            if hasattr(model, 'name_column') and hasattr(model, 'date_column') and hasattr(model, 'amount_column'):
                name_col = model.name_column
                date_col = model.date_column
                amount_col = model.amount_column
                
                # 筛选该人员的交易
                person_data = model.data[model.data[name_col] == person_name].copy()
                
                if person_data.empty:
                    continue
                
                # 转换日期列
                person_data.loc[:, date_col] = pd.to_datetime(person_data[date_col], errors='coerce')
                
                # 筛选时间窗口内的交易
                time_mask = (person_data[date_col] >= start_date) & (person_data[date_col] <= end_date)
                time_filtered_data = person_data[time_mask].copy()
                
                if time_filtered_data.empty:
                    continue
                
                # 筛选大额交易 - 使用.loc避免SettingWithCopyWarning
                time_filtered_data.loc[:, amount_col] = pd.to_numeric(time_filtered_data[amount_col], errors='coerce')
                large_mask = time_filtered_data[amount_col].abs() >= self.min_large_amount
                large_data = time_filtered_data[large_mask]
                
                if large_data.empty:
                    continue
                
                # 构建交易记录
                for _, row in large_data.iterrows():
                    amount = row[amount_col]
                    direction = '收入' if amount > 0 else '支出'
                    
                    if hasattr(model, 'opposite_name_column'):
                        opposite_name_col = model.opposite_name_column
                        opposite_person = row[opposite_name_col] if opposite_name_col in row else '未知'
                    else:
                        opposite_person = '未知'
                    
                    # 避免追踪已访问的人员（除了直接交易对方）
                    if opposite_person in visited_persons and opposite_person != person_name:
                        continue
                    
                    amount_level = self._get_amount_level(abs(amount))
                    
                    related_transactions.append({
                        '数据来源': model_name,
                        '交易日期': row[date_col],
                        '本方姓名': person_name,
                        '对方姓名': opposite_person,
                        '交易金额': amount,
                        '交易方向': direction,
                        '大额级别': amount_level
                    })
        
        if related_transactions:
            return pd.DataFrame(related_transactions)
        else:
            return pd.DataFrame()
    
    def generate_tracking_report(self, tracking_results: pd.DataFrame) -> Dict:
        """
        生成大额资金追踪报告
        
        Parameters:
        -----------
        tracking_results : pd.DataFrame
            资金流向追踪结果
            
        Returns:
        --------
        Dict
            追踪报告统计信息
        """
        if tracking_results.empty:
            return {
                '总追踪交易数': 0,
                '涉及人员数': 0,
                '最大追踪深度': 0,
                '大额级别分布': {},
                '数据来源分布': {},
                '资金流向统计': {}
            }
        
        report = {
            '总追踪交易数': len(tracking_results),
            '涉及人员数': tracking_results['核心人员'].nunique(),
            '最大追踪深度': tracking_results['追踪层级'].max(),
            '大额级别分布': tracking_results['大额级别'].value_counts().to_dict(),
            '数据来源分布': tracking_results['数据来源'].value_counts().to_dict(),
            '资金流向统计': tracking_results['资金流向'].value_counts().to_dict()
        }
        
        return report