#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
重点收支识别引擎
用于识别涉及房产、车辆、工资、奖金、租金等重点收支的交易
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional
from src.utils.config import Config


class KeyTransactionEngine:
    """重点收支识别引擎"""
    
    def __init__(self, config: Optional[Config] = None):
        """
        初始化重点收支识别引擎
        
        Parameters:
        -----------
        config : Config, optional
            配置对象，如果为None则创建新的配置对象
        """
        self.config = config or Config()
        self.logger = logging.getLogger(__name__)
        
        # 加载配置
        self.key_config = self.config.get('analysis.key_transactions', {})
        self.enabled = self.key_config.get('enabled', True)
        
        if not self.enabled:
            self.logger.info("重点收支识别功能已禁用")
            return

        # 调试：检查配置是否正确加载
        self.logger.debug(f"key_config内容: {self.key_config}")
        self.logger.debug(f"配置文件路径: {self.config.config_file}")
        self.logger.debug(f"完整配置: {self.config.get('analysis', {})}")
            
        # 工作收入关键词
        self.work_income_keywords = self.key_config.get('work_income', {}).get('keywords', [])

        # 资产收入关键词
        asset_income_config = self.key_config.get('asset_income', {})
        self.property_keywords = asset_income_config.get('property', {}).get('keywords', [])
        self.rental_keywords = asset_income_config.get('rental', {}).get('keywords', [])
        self.vehicle_keywords = asset_income_config.get('vehicle', {}).get('keywords', [])
        self.securities_keywords = asset_income_config.get('securities', {}).get('keywords', [])

        # 大额交易阈值
        self.large_amount_thresholds = self.key_config.get('large_amount_thresholds', {})
        
        total_asset_keywords = len(self.property_keywords + self.rental_keywords + self.vehicle_keywords + self.securities_keywords)
        self.logger.info(f"重点收支识别引擎初始化完成，工作收入关键词: {len(self.work_income_keywords)}个，"
                        f"资产收入关键词: {total_asset_keywords}个")
        self.logger.debug(f"工作收入关键词: {self.work_income_keywords}")
        self.logger.debug(f"房产关键词: {self.property_keywords}")
        self.logger.debug(f"租金关键词: {self.rental_keywords}")
        self.logger.debug(f"车辆关键词: {self.vehicle_keywords}")
        self.logger.debug(f"证券关键词: {self.securities_keywords}")
        self.logger.debug(f"大额交易阈值: {self.large_amount_thresholds}")
    
    def identify_key_transactions(self, data: pd.DataFrame, summary_column: str, 
                                remark_column: str, type_column: str, 
                                amount_column: str, opposite_name_column: str) -> pd.DataFrame:
        """
        识别重点收支交易
        
        Parameters:
        -----------
        data : pd.DataFrame
            银行流水数据
        summary_column : str
            交易摘要列名
        remark_column : str
            交易备注列名
        type_column : str
            交易类型列名
        amount_column : str
            交易金额列名
        opposite_name_column : str
            对方姓名列名
            
        Returns:
        --------
        pd.DataFrame
            包含重点收支标识的数据
        """
        if not self.enabled or data.empty:
            return data.copy()
        
        result_data = data.copy()
        
        # 初始化重点收支标识列
        result_data['是否工作收入'] = False
        result_data['是否房产收入'] = False
        result_data['是否租金收入'] = False
        result_data['是否车辆收入'] = False
        result_data['是否证券收入'] = False
        result_data['是否大额收入'] = False
        result_data['是否大额支出'] = False
        result_data['大额级别'] = ''
        result_data['重点收支类型'] = ''
        result_data['重点收支子类'] = ''
        result_data['识别原因'] = ''
        result_data['是否重点收支'] = False
        
        # 创建用于匹配的文本列（合并摘要、备注、类型、对方姓名）
        result_data['匹配文本'] = (
            result_data[summary_column].fillna('').astype(str) + ' ' +
            result_data[remark_column].fillna('').astype(str) + ' ' +
            result_data[type_column].fillna('').astype(str) + ' ' +
            result_data[opposite_name_column].fillna('').astype(str)
        )
        
        # 识别工作收入
        self._identify_work_income(result_data, amount_column, opposite_name_column)
        
        # 识别资产收入
        self._identify_asset_income(result_data, amount_column)
        
        # 识别大额交易
        self._identify_large_amount_transactions(result_data, amount_column)

        # 生成主要的重点收支类型和子类（用于显示和统计）
        self._generate_main_category(result_data)

        # 清理临时列
        result_data.drop('匹配文本', axis=1, inplace=True)

        self.logger.info(f"重点收支识别完成，共识别出 {result_data['是否重点收支'].sum()} 笔重点收支交易")

        return result_data
    
    def _identify_work_income(self, data: pd.DataFrame, amount_column: str, opposite_name_column: str):
        """识别工作收入"""
        # 只识别收入（正金额）
        income_mask = data[amount_column] > 0

        # 关键词匹配
        if not self.work_income_keywords:
            return

        work_income_mask = data['匹配文本'].str.contains('|'.join(self.work_income_keywords), case=False, na=False)

        # 应用工作收入标识
        final_mask = income_mask & work_income_mask
        data.loc[final_mask, '是否工作收入'] = True
        data.loc[final_mask, '是否重点收支'] = True

        self.logger.debug(f"识别出工作收入交易 {final_mask.sum()} 笔")
    
    def _identify_asset_income(self, data: pd.DataFrame, amount_column: str):
        """识别资产收入"""
        # 只识别收入（正金额）
        income_mask = data[amount_column] > 0

        # 房产收入
        if self.property_keywords:
            property_mask = data['匹配文本'].str.contains('|'.join(self.property_keywords), case=False, na=False)
            final_property_mask = income_mask & property_mask
            data.loc[final_property_mask, '是否房产收入'] = True
            data.loc[final_property_mask, '是否重点收支'] = True

        # 租金收入
        if self.rental_keywords:
            rental_mask = data['匹配文本'].str.contains('|'.join(self.rental_keywords), case=False, na=False)
            final_rental_mask = income_mask & rental_mask
            data.loc[final_rental_mask, '是否租金收入'] = True
            data.loc[final_rental_mask, '是否重点收支'] = True

        # 车辆收入
        if self.vehicle_keywords:
            vehicle_mask = data['匹配文本'].str.contains('|'.join(self.vehicle_keywords), case=False, na=False)
            final_vehicle_mask = income_mask & vehicle_mask
            data.loc[final_vehicle_mask, '是否车辆收入'] = True
            data.loc[final_vehicle_mask, '是否重点收支'] = True

        # 证券收入
        if self.securities_keywords:
            securities_mask = data['匹配文本'].str.contains('|'.join(self.securities_keywords), case=False, na=False)
            final_securities_mask = income_mask & securities_mask
            data.loc[final_securities_mask, '是否证券收入'] = True
            data.loc[final_securities_mask, '是否重点收支'] = True

        # 统计各类资产收入
        property_count = data['是否房产收入'].sum()
        rental_count = data['是否租金收入'].sum()
        vehicle_count = data['是否车辆收入'].sum()
        securities_count = data['是否证券收入'].sum()

        total_asset_income = property_count + rental_count + vehicle_count + securities_count
        self.logger.debug(f"识别出资产收入交易 {total_asset_income} 笔（房产{property_count}，租金{rental_count}，车辆{vehicle_count}，证券{securities_count}）")
    
    def _identify_large_amount_transactions(self, data: pd.DataFrame, amount_column: str):
        """识别大额交易"""
        abs_amounts = data[amount_column].abs()

        # 区分收入和支出
        income_mask = data[amount_column] > 0
        expense_mask = data[amount_column] < 0

        for level_key, level_config in self.large_amount_thresholds.items():
            min_amount = level_config.get('min', 0)
            max_amount = level_config.get('max', float('inf'))
            level_name = level_config.get('name', level_key)

            # 大额交易掩码（收入和支出都算）
            large_amount_mask = (abs_amounts >= min_amount) & (abs_amounts < max_amount)

            if large_amount_mask.any():
                # 大额收入
                large_income_mask = large_amount_mask & income_mask
                data.loc[large_income_mask, '是否大额收入'] = True
                data.loc[large_income_mask, '大额级别'] = level_name
                data.loc[large_income_mask, '是否重点收支'] = True

                # 大额支出
                large_expense_mask = large_amount_mask & expense_mask
                data.loc[large_expense_mask, '是否大额支出'] = True
                data.loc[large_expense_mask, '大额级别'] = level_name
                data.loc[large_expense_mask, '是否重点收支'] = True

        large_income_total = data['是否大额收入'].sum()
        large_expense_total = data['是否大额支出'].sum()
        self.logger.debug(f"识别出大额交易 {large_income_total + large_expense_total} 笔（收入{large_income_total}，支出{large_expense_total}）")

    def _generate_main_category(self, data: pd.DataFrame):
        """根据多标签生成主要的重点收支类型和子类"""
        # 按优先级确定主要类型：工作收入 > 房产收入 > 租金收入 > 车辆收入 > 证券收入 > 大额交易

        # 工作收入（最高优先级）
        work_mask = data['是否工作收入']
        data.loc[work_mask, '重点收支类型'] = '工作收入'
        data.loc[work_mask, '重点收支子类'] = '工资奖金'
        data.loc[work_mask, '识别原因'] = '工作收入关键词匹配'

        # 房产收入
        property_mask = data['是否房产收入'] & (data['重点收支类型'] == '')
        data.loc[property_mask, '重点收支类型'] = '资产收入'
        data.loc[property_mask, '重点收支子类'] = '房产'
        data.loc[property_mask, '识别原因'] = '房产关键词匹配'

        # 租金收入
        rental_mask = data['是否租金收入'] & (data['重点收支类型'] == '')
        data.loc[rental_mask, '重点收支类型'] = '资产收入'
        data.loc[rental_mask, '重点收支子类'] = '租金'
        data.loc[rental_mask, '识别原因'] = '租金关键词匹配'

        # 车辆收入
        vehicle_mask = data['是否车辆收入'] & (data['重点收支类型'] == '')
        data.loc[vehicle_mask, '重点收支类型'] = '资产收入'
        data.loc[vehicle_mask, '重点收支子类'] = '车辆'
        data.loc[vehicle_mask, '识别原因'] = '车辆关键词匹配'

        # 证券收入
        securities_mask = data['是否证券收入'] & (data['重点收支类型'] == '')
        data.loc[securities_mask, '重点收支类型'] = '资产收入'
        data.loc[securities_mask, '重点收支子类'] = '证券'
        data.loc[securities_mask, '识别原因'] = '证券关键词匹配'

        # 大额收入
        large_income_mask = data['是否大额收入'] & (data['重点收支类型'] == '')
        data.loc[large_income_mask, '重点收支类型'] = '大额收入'
        data.loc[large_income_mask, '重点收支子类'] = data.loc[large_income_mask, '大额级别']
        data.loc[large_income_mask, '识别原因'] = '金额达到大额标准'

        # 大额支出
        large_expense_mask = data['是否大额支出'] & (data['重点收支类型'] == '')
        data.loc[large_expense_mask, '重点收支类型'] = '大额支出'
        data.loc[large_expense_mask, '重点收支子类'] = data.loc[large_expense_mask, '大额级别']
        data.loc[large_expense_mask, '识别原因'] = '金额达到大额标准'
    
    def generate_statistics(self, data: pd.DataFrame, name_column: str, 
                          amount_column: str, date_column: str, 
                          opposite_name_column: str) -> pd.DataFrame:
        """
        生成重点收支统计信息
        
        Parameters:
        -----------
        data : pd.DataFrame
            包含重点收支标识的数据
        name_column : str
            姓名列名
        amount_column : str
            金额列名
        date_column : str
            日期列名
        opposite_name_column : str
            对方姓名列名
            
        Returns:
        --------
        pd.DataFrame
            重点收支统计结果
        """
        if data.empty or not data['是否重点收支'].any():
            return pd.DataFrame()
        
        # 只处理重点收支数据
        key_data = data[data['是否重点收支']].copy()
        
        # 按人员分组统计
        stats_list = []
        
        for person_name in key_data[name_column].unique():
            person_data = key_data[key_data[name_column] == person_name]
            
            # 基本统计
            stats = {
                '姓名': person_name,
                '重点收支总笔数': len(person_data),
                '重点收支总金额': person_data[amount_column].sum(),
                '重点收入总额': person_data[person_data[amount_column] > 0][amount_column].sum(),
                '重点支出总额': abs(person_data[person_data[amount_column] < 0][amount_column].sum()),
                '时间范围': f"{person_data[date_column].min()} 至 {person_data[date_column].max()}"
            }
            
            # 工作收入统计（使用多标签）
            work_income_data = person_data[person_data['是否工作收入']]
            if not work_income_data.empty:
                stats['工作收入总额'] = work_income_data[amount_column].sum()
                stats['工作收入次数'] = len(work_income_data)
                # 统计可能的工作单位
                work_units = work_income_data[opposite_name_column].dropna().unique()
                stats['可能工作单位数'] = len(work_units)
                stats['可能工作单位'] = '、'.join(work_units[:3])  # 最多显示前3个
            else:
                stats['工作收入总额'] = 0
                stats['工作收入次数'] = 0
                stats['可能工作单位数'] = 0
                stats['可能工作单位'] = ''
            
            # 资产收入统计（使用多标签）
            # 按子类统计
            asset_types = [
                ('房产', '是否房产收入'),
                ('租金', '是否租金收入'),
                ('车辆', '是否车辆收入'),
                ('证券', '是否证券收入')
            ]

            total_asset_amount = 0
            total_asset_count = 0

            for sub_type, column_name in asset_types:
                sub_data = person_data[person_data[column_name]]
                stats[f'{sub_type}收入次数'] = len(sub_data)
                stats[f'{sub_type}收入金额'] = sub_data[amount_column].sum() if not sub_data.empty else 0
                total_asset_amount += stats[f'{sub_type}收入金额']
                total_asset_count += stats[f'{sub_type}收入次数']

            stats['资产收入总额'] = total_asset_amount
            stats['资产收入次数'] = total_asset_count
            
            # 大额交易统计（使用多标签）
            # 大额收入
            large_income_data = person_data[person_data['是否大额收入']]
            if not large_income_data.empty:
                stats['大额收入总额'] = large_income_data[amount_column].sum()
                stats['大额收入总次数'] = len(large_income_data)

                # 按金额级别统计
                for level_key, level_config in self.large_amount_thresholds.items():
                    level_name = level_config.get('name', level_key)
                    level_data = large_income_data[large_income_data['大额级别'] == level_name]
                    stats[f'大额收入_{level_name}_次数'] = len(level_data)
                    stats[f'大额收入_{level_name}_金额'] = level_data[amount_column].sum() if not level_data.empty else 0
            else:
                stats['大额收入总额'] = 0
                stats['大额收入总次数'] = 0
                for level_key, level_config in self.large_amount_thresholds.items():
                    level_name = level_config.get('name', level_key)
                    stats[f'大额收入_{level_name}_次数'] = 0
                    stats[f'大额收入_{level_name}_金额'] = 0

            # 大额支出
            large_expense_data = person_data[person_data['是否大额支出']]
            if not large_expense_data.empty:
                stats['大额支出总额'] = abs(large_expense_data[amount_column].sum())
                stats['大额支出总次数'] = len(large_expense_data)

                # 按金额级别统计
                for level_key, level_config in self.large_amount_thresholds.items():
                    level_name = level_config.get('name', level_key)
                    level_data = large_expense_data[large_expense_data['大额级别'] == level_name]
                    stats[f'大额支出_{level_name}_次数'] = len(level_data)
                    stats[f'大额支出_{level_name}_金额'] = abs(level_data[amount_column].sum()) if not level_data.empty else 0
            else:
                stats['大额支出总额'] = 0
                stats['大额支出总次数'] = 0
                for level_key, level_config in self.large_amount_thresholds.items():
                    level_name = level_config.get('name', level_key)
                    stats[f'大额支出_{level_name}_次数'] = 0
                    stats[f'大额支出_{level_name}_金额'] = 0
            
            stats_list.append(stats)
        
        return pd.DataFrame(stats_list)
