"""
存取现识别工具类
提供统一的存取现识别算法，支持基础和增强两种模式
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
import logging


class CashRecognitionEngine:
    """存取现识别引擎"""
    
    def __init__(self, config):
        """
        初始化识别引擎
        
        Parameters:
        -----------
        config : Config
            配置对象
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 加载配置参数
        self._load_config()
    
    def _load_config(self):
        """加载配置参数"""
        # 基础关键词配置（修正配置路径）
        self.deposit_keywords = self.config.get('data_sources.bank.deposit_keywords', [])
        self.withdraw_keywords = self.config.get('data_sources.bank.withdraw_keywords', [])
        self.deposit_exclude_keywords = self.config.get('data_sources.bank.deposit_exclude_keywords', [])
        self.withdraw_exclude_keywords = self.config.get('data_sources.bank.withdraw_exclude_keywords', [])
        
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
        self.common_cash_amounts = self.config.get('analysis.cash.recognition.common_cash_amounts', [])
        self.round_amount_modulos = self.config.get('analysis.cash.recognition.round_amount_modulos', [50, 100])
        self.high_priority_deposit_keywords = self.config.get('analysis.cash.recognition.high_priority_deposit_keywords', [])
        self.high_priority_withdraw_keywords = self.config.get('analysis.cash.recognition.high_priority_withdraw_keywords', [])

        # 验证配置加载情况
        self._validate_config()

    def _validate_config(self):
        """验证配置加载情况"""
        self.logger.info("=== 存取现识别配置验证 ===")
        self.logger.info(f"存现关键词数量: {len(self.deposit_keywords)}")
        self.logger.info(f"取现关键词数量: {len(self.withdraw_keywords)}")
        self.logger.info(f"存现排除关键词数量: {len(self.deposit_exclude_keywords)}")
        self.logger.info(f"取现排除关键词数量: {len(self.withdraw_exclude_keywords)}")
        self.logger.info(f"高优先级存现关键词数量: {len(self.high_priority_deposit_keywords)}")
        self.logger.info(f"高优先级取现关键词数量: {len(self.high_priority_withdraw_keywords)}")

        # 检查关键配置是否为空
        if not self.deposit_keywords:
            self.logger.warning("⚠️  存现关键词为空，中优先级识别将失效")
        if not self.withdraw_keywords:
            self.logger.warning("⚠️  取现关键词为空，中优先级识别将失效")
        if not self.deposit_exclude_keywords:
            self.logger.warning("⚠️  存现排除关键词为空，转账过滤将失效")
        if not self.withdraw_exclude_keywords:
            self.logger.warning("⚠️  取现排除关键词为空，转账过滤将失效")

        # 显示部分关键词示例
        if self.deposit_keywords:
            self.logger.info(f"存现关键词示例: {self.deposit_keywords[:5]}")
        if self.deposit_exclude_keywords:
            self.logger.info(f"存现排除关键词示例: {self.deposit_exclude_keywords[:5]}")
        self.logger.info("=== 配置验证完成 ===")

    def recognize_cash_operations(self, data: pd.DataFrame, columns_config: Dict[str, str]) -> pd.DataFrame:
        """
        识别存取现操作
        
        Parameters:
        -----------
        data : pd.DataFrame
            待识别的数据
        columns_config : Dict[str, str]
            列名配置字典，包含必要的列名映射
            
        Returns:
        --------
        pd.DataFrame
            添加了存取现标识的数据
        """
        # 复制数据避免修改原始数据
        result_data = data.copy()
        
        # 初始化标识列
        result_data['存取现标识'] = '转账'
        
        # 如果启用增强算法，添加额外字段
        if self.enable_enhanced_algorithm:
            result_data['识别置信度'] = 0.0
            result_data['识别原因'] = ''
        
        # 获取必要的列
        opposite_name_column = columns_config.get('opposite_name_column')
        summary_column = columns_config.get('summary_column')
        remark_column = columns_config.get('remark_column')
        type_column = columns_config.get('type_column')
        direction_column = columns_config.get('direction_column')
        amount_column = columns_config.get('amount_column')
        income_flag = columns_config.get('income_flag')
        expense_flag = columns_config.get('expense_flag')
        
        # 检查必要列是否存在
        required_columns = [summary_column, remark_column, direction_column, amount_column]
        missing_columns = [col for col in required_columns if col not in result_data.columns]
        if missing_columns:
            self.logger.warning(f"缺少必要列: {missing_columns}")
            return result_data
        
        # 构建对方姓名为空的掩码
        if opposite_name_column and opposite_name_column in result_data.columns:
            empty_opposite_mask = result_data[opposite_name_column].isna() | \
                                  (result_data[opposite_name_column].astype(str).str.strip() == '') | \
                                  (result_data[opposite_name_column].astype(str).str.strip() == '\\N')
        else:
            # 如果没有对方姓名字段，则所有记录都可能是存取现
            empty_opposite_mask = pd.Series([True] * len(result_data), index=result_data.index)
        
        # 获取相关列，并填充空值
        summary_col = result_data[summary_column].astype(str).fillna('')
        remark_col = result_data[remark_column].astype(str).fillna('')
        type_col = result_data[type_column].astype(str).fillna('') if type_column and type_column in result_data.columns else pd.Series([''] * len(result_data))

        # 执行识别
        if self.enable_enhanced_algorithm:
            self._enhanced_recognition(result_data, empty_opposite_mask, summary_col, remark_col, type_col,
                                     direction_column, amount_column, income_flag, expense_flag)
        else:
            self._basic_recognition(result_data, empty_opposite_mask, summary_col, remark_col, type_col,
                                  direction_column, amount_column, income_flag, expense_flag)
        
        return result_data
    
    def _basic_recognition(self, data: pd.DataFrame, empty_opposite_mask: pd.Series,
                          summary_col: pd.Series, remark_col: pd.Series, type_col: pd.Series,
                          direction_column: str, amount_column: str,
                          income_flag: str, expense_flag: str):
        """基础识别算法"""
        # 构建正则表达式模式
        deposit_pattern = '|'.join(self.deposit_keywords) if self.deposit_keywords else ''
        deposit_exclude_pattern = '|'.join(self.deposit_exclude_keywords) if self.deposit_exclude_keywords else ''
        withdraw_pattern = '|'.join(self.withdraw_keywords) if self.withdraw_keywords else ''
        withdraw_exclude_pattern = '|'.join(self.withdraw_exclude_keywords) if self.withdraw_exclude_keywords else ''
        
        if not deposit_pattern or not withdraw_pattern:
            self.logger.warning("存取现关键词配置为空，跳过识别")
            return
        
        # 存现识别
        deposit_mask = empty_opposite_mask & (
            (summary_col.str.contains(deposit_pattern, case=False, na=False)) |
            (remark_col.str.contains(deposit_pattern, case=False, na=False)) |
            (type_col.str.contains(deposit_pattern, case=False, na=False))
        ) & (data[direction_column] == income_flag)

        if deposit_exclude_pattern:
            deposit_mask = deposit_mask & ~(
                (summary_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                (remark_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                (type_col.str.contains(deposit_exclude_pattern, case=False, na=False))
            )

        data.loc[deposit_mask, '存取现标识'] = '存现'
        data.loc[deposit_mask, '收入金额'] = data.loc[deposit_mask, amount_column].abs()

        # 取现识别
        remaining_mask = ~deposit_mask & empty_opposite_mask
        withdraw_mask = remaining_mask & (
            (summary_col.str.contains(withdraw_pattern, case=False, na=False)) |
            (remark_col.str.contains(withdraw_pattern, case=False, na=False)) |
            (type_col.str.contains(withdraw_pattern, case=False, na=False))
        ) & (data[direction_column] == expense_flag)

        if withdraw_exclude_pattern:
            withdraw_mask = withdraw_mask & ~(
                (summary_col.str.contains(withdraw_exclude_pattern, case=False, na=False)) |
                (remark_col.str.contains(withdraw_exclude_pattern, case=False, na=False)) |
                (type_col.str.contains(withdraw_exclude_pattern, case=False, na=False))
            )

        data.loc[withdraw_mask, '存取现标识'] = '取现'
        data.loc[withdraw_mask, '支出金额'] = data.loc[withdraw_mask, amount_column].abs()
    
    def _enhanced_recognition(self, data: pd.DataFrame, empty_opposite_mask: pd.Series,
                            summary_col: pd.Series, remark_col: pd.Series, type_col: pd.Series,
                            direction_column: str, amount_column: str,
                            income_flag: str, expense_flag: str):
        """增强识别算法"""
        # 1. 高优先级精确匹配
        self._high_priority_recognition(data, empty_opposite_mask, summary_col, remark_col, type_col,
                                      direction_column, amount_column, income_flag, expense_flag)

        # 2. 中优先级模糊匹配
        self._medium_priority_recognition(data, empty_opposite_mask, summary_col, remark_col, type_col,
                                        direction_column, amount_column, income_flag, expense_flag)

        # 3. 低优先级上下文分析
        if self.enable_fuzzy_matching:
            self._low_priority_recognition(data, empty_opposite_mask, summary_col, remark_col, type_col,
                                         direction_column, amount_column, income_flag, expense_flag)
        
        # 4. 智能金额分析
        if self.enable_amount_analysis:
            self._amount_based_analysis(data, amount_column)
    
    def _high_priority_recognition(self, data: pd.DataFrame, empty_opposite_mask: pd.Series,
                                 summary_col: pd.Series, remark_col: pd.Series, type_col: pd.Series,
                                 direction_column: str, amount_column: str,
                                 income_flag: str, expense_flag: str):
        """高优先级精确匹配识别（也需要排除转账）"""
        # 构建排除模式
        deposit_exclude_pattern = '|'.join(self.deposit_exclude_keywords) if self.deposit_exclude_keywords else ''
        withdraw_exclude_pattern = '|'.join(self.withdraw_exclude_keywords) if self.withdraw_exclude_keywords else ''

        # 存现高优先级匹配
        for keyword in self.high_priority_deposit_keywords:
            # 第1步：基础匹配（只处理未识别的转账）
            base_mask = empty_opposite_mask & (
                (summary_col.str.contains(keyword, case=False, na=False)) |
                (remark_col.str.contains(keyword, case=False, na=False)) |
                (type_col.str.contains(keyword, case=False, na=False))
            ) & (data[direction_column] == income_flag) & (data['存取现标识'] == '转账')

            # 第2步：排除转账
            if deposit_exclude_pattern:
                mask = base_mask & ~(
                    (summary_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                    (remark_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                    (type_col.str.contains(deposit_exclude_pattern, case=False, na=False))
                )
            else:
                mask = base_mask

            if mask.any():
                data.loc[mask, '存取现标识'] = '存现'
                data.loc[mask, '收入金额'] = data.loc[mask, amount_column].abs()
                data.loc[mask, '识别置信度'] = self.high_priority_confidence
                data.loc[mask, '识别原因'] = f'高优先级关键词匹配: {keyword}'

        # 取现高优先级匹配
        for keyword in self.high_priority_withdraw_keywords:
            # 第1步：基础匹配
            base_mask = empty_opposite_mask & (
                (summary_col.str.contains(keyword, case=False, na=False)) |
                (remark_col.str.contains(keyword, case=False, na=False)) |
                (type_col.str.contains(keyword, case=False, na=False))
            ) & (data[direction_column] == expense_flag) & (data['存取现标识'] == '转账')

            # 第2步：排除转账
            if withdraw_exclude_pattern:
                mask = base_mask & ~(
                    (summary_col.str.contains(withdraw_exclude_pattern, case=False, na=False)) |
                    (remark_col.str.contains(withdraw_exclude_pattern, case=False, na=False)) |
                    (type_col.str.contains(withdraw_exclude_pattern, case=False, na=False))
                )
            else:
                mask = base_mask

            if mask.any():
                data.loc[mask, '存取现标识'] = '取现'
                data.loc[mask, '支出金额'] = data.loc[mask, amount_column].abs()
                data.loc[mask, '识别置信度'] = self.high_priority_confidence
                data.loc[mask, '识别原因'] = f'高优先级关键词匹配: {keyword}'
    
    def _medium_priority_recognition(self, data: pd.DataFrame, empty_opposite_mask: pd.Series,
                                   summary_col: pd.Series, remark_col: pd.Series, type_col: pd.Series,
                                   direction_column: str, amount_column: str,
                                   income_flag: str, expense_flag: str):
        """中优先级识别：先过滤转账，再筛选存取现"""
        # 构建正则表达式模式
        deposit_pattern = '|'.join(self.deposit_keywords) if self.deposit_keywords else ''
        deposit_exclude_pattern = '|'.join(self.deposit_exclude_keywords) if self.deposit_exclude_keywords else ''
        withdraw_pattern = '|'.join(self.withdraw_keywords) if self.withdraw_keywords else ''
        withdraw_exclude_pattern = '|'.join(self.withdraw_exclude_keywords) if self.withdraw_exclude_keywords else ''

        if not deposit_pattern or not withdraw_pattern:
            return

        # 存现识别：先过滤转账，再筛选存现关键词
        # 第1步：基础条件筛选（对方姓名为空 + 借贷标识为贷）
        deposit_base_mask = empty_opposite_mask & (data[direction_column] == income_flag) & (data['存取现标识'] == '转账')

        # 第2步：过滤转账相关交易
        if deposit_exclude_pattern:
            deposit_candidate_mask = deposit_base_mask & ~(
                (summary_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                (remark_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                (type_col.str.contains(deposit_exclude_pattern, case=False, na=False))
            )
        else:
            deposit_candidate_mask = deposit_base_mask

        # 第3步：从候选中筛选包含存现关键词的交易
        deposit_mask = deposit_candidate_mask & (
            (summary_col.str.contains(deposit_pattern, case=False, na=False)) |
            (remark_col.str.contains(deposit_pattern, case=False, na=False)) |
            (type_col.str.contains(deposit_pattern, case=False, na=False))
        )

        if deposit_mask.any():
            data.loc[deposit_mask, '存取现标识'] = '存现'
            data.loc[deposit_mask, '收入金额'] = data.loc[deposit_mask, amount_column].abs()
            data.loc[deposit_mask, '识别置信度'] = self.medium_priority_confidence
            data.loc[deposit_mask, '识别原因'] = '中优先级关键词匹配'

        # 取现识别：先过滤转账，再筛选取现关键词
        # 第1步：基础条件筛选（对方姓名为空 + 借贷标识为借）
        withdraw_base_mask = empty_opposite_mask & (data[direction_column] == expense_flag) & (data['存取现标识'] == '转账')

        # 第2步：过滤转账相关交易
        if withdraw_exclude_pattern:
            withdraw_candidate_mask = withdraw_base_mask & ~(
                (summary_col.str.contains(withdraw_exclude_pattern, case=False, na=False)) |
                (remark_col.str.contains(withdraw_exclude_pattern, case=False, na=False)) |
                (type_col.str.contains(withdraw_exclude_pattern, case=False, na=False))
            )
        else:
            withdraw_candidate_mask = withdraw_base_mask

        # 第3步：从候选中筛选包含取现关键词的交易
        withdraw_mask = withdraw_candidate_mask & (
            (summary_col.str.contains(withdraw_pattern, case=False, na=False)) |
            (remark_col.str.contains(withdraw_pattern, case=False, na=False)) |
            (type_col.str.contains(withdraw_pattern, case=False, na=False))
        )

        if withdraw_mask.any():
            data.loc[withdraw_mask, '存取现标识'] = '取现'
            data.loc[withdraw_mask, '支出金额'] = data.loc[withdraw_mask, amount_column].abs()
            data.loc[withdraw_mask, '识别置信度'] = self.medium_priority_confidence
            data.loc[withdraw_mask, '识别原因'] = '中优先级关键词匹配'

        # ATM智能识别：先过滤转账，再识别ATM存取现
        self._atm_smart_recognition(data, empty_opposite_mask, summary_col, remark_col, type_col,
                                  direction_column, amount_column, income_flag, expense_flag,
                                  deposit_exclude_pattern, withdraw_exclude_pattern)

    def _atm_smart_recognition(self, data: pd.DataFrame, empty_opposite_mask: pd.Series,
                             summary_col: pd.Series, remark_col: pd.Series, type_col: pd.Series,
                             direction_column: str, amount_column: str,
                             income_flag: str, expense_flag: str,
                             deposit_exclude_pattern: str, withdraw_exclude_pattern: str):
        """ATM智能识别：先过滤转账，再识别ATM存取现"""

        # ATM存现识别：先过滤转账，再筛选ATM
        # 第1步：基础条件筛选（对方姓名为空 + 借贷标识为贷）
        atm_deposit_base_mask = empty_opposite_mask & (data[direction_column] == income_flag) & (data['存取现标识'] == '转账')

        # 第2步：过滤转账相关交易
        if deposit_exclude_pattern:
            atm_deposit_candidate_mask = atm_deposit_base_mask & ~(
                (summary_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                (remark_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                (type_col.str.contains(deposit_exclude_pattern, case=False, na=False))
            )
        else:
            atm_deposit_candidate_mask = atm_deposit_base_mask

        # 第3步：从候选中筛选包含ATM的交易
        atm_deposit_mask = atm_deposit_candidate_mask & (
            (summary_col.str.contains('ATM', case=False, na=False)) |
            (remark_col.str.contains('ATM', case=False, na=False)) |
            (type_col.str.contains('ATM', case=False, na=False))
        )

        if atm_deposit_mask.any():
            data.loc[atm_deposit_mask, '存取现标识'] = '存现'
            data.loc[atm_deposit_mask, '收入金额'] = data.loc[atm_deposit_mask, amount_column].abs()
            data.loc[atm_deposit_mask, '识别置信度'] = self.medium_priority_confidence
            data.loc[atm_deposit_mask, '识别原因'] = 'ATM智能识别-存现'

        # ATM取现识别：先过滤转账，再筛选ATM
        # 第1步：基础条件筛选（对方姓名为空 + 借贷标识为借）
        atm_withdraw_base_mask = empty_opposite_mask & (data[direction_column] == expense_flag) & (data['存取现标识'] == '转账')

        # 第2步：过滤转账相关交易
        if withdraw_exclude_pattern:
            atm_withdraw_candidate_mask = atm_withdraw_base_mask & ~(
                (summary_col.str.contains(withdraw_exclude_pattern, case=False, na=False)) |
                (remark_col.str.contains(withdraw_exclude_pattern, case=False, na=False)) |
                (type_col.str.contains(withdraw_exclude_pattern, case=False, na=False))
            )
        else:
            atm_withdraw_candidate_mask = atm_withdraw_base_mask

        # 第3步：从候选中筛选包含ATM的交易
        atm_withdraw_mask = atm_withdraw_candidate_mask & (
            (summary_col.str.contains('ATM', case=False, na=False)) |
            (remark_col.str.contains('ATM', case=False, na=False)) |
            (type_col.str.contains('ATM', case=False, na=False))
        )

        if atm_withdraw_mask.any():
            data.loc[atm_withdraw_mask, '存取现标识'] = '取现'
            data.loc[atm_withdraw_mask, '支出金额'] = data.loc[atm_withdraw_mask, amount_column].abs()
            data.loc[atm_withdraw_mask, '识别置信度'] = self.medium_priority_confidence
            data.loc[atm_withdraw_mask, '识别原因'] = 'ATM智能识别-取现'

    def _low_priority_recognition(self, data: pd.DataFrame, empty_opposite_mask: pd.Series,
                                summary_col: pd.Series, remark_col: pd.Series, type_col: pd.Series,
                                direction_column: str, amount_column: str,
                                income_flag: str, expense_flag: str):
        """低优先级上下文分析识别"""
        # 构建排除模式
        deposit_exclude_pattern = '|'.join(self.deposit_exclude_keywords) if self.deposit_exclude_keywords else ''
        withdraw_exclude_pattern = '|'.join(self.withdraw_exclude_keywords) if self.withdraw_exclude_keywords else ''

        # 基于金额特征的识别（整数金额更可能是存取现）
        round_amount_conditions = []
        for modulo in self.round_amount_modulos:
            round_amount_conditions.append(data[amount_column].abs() % modulo == 0)
        round_amount_mask = pd.concat(round_amount_conditions, axis=1).any(axis=1) if round_amount_conditions else pd.Series([False] * len(data))

        # 基于金额范围的识别（常见存取现金额范围）
        amount_mask = data[amount_column].abs().isin(self.common_cash_amounts) if self.common_cash_amounts else pd.Series([False] * len(data))

        # 模糊匹配：包含"现"字但不在排除列表中
        base_fuzzy_mask = empty_opposite_mask & (
            (summary_col.str.contains('现', case=False, na=False)) |
            (remark_col.str.contains('现', case=False, na=False)) |
            (type_col.str.contains('现', case=False, na=False))
        ) & (data['存取现标识'] == '转账')

        # 排除转账相关交易（这是关键修复）
        if deposit_exclude_pattern:
            fuzzy_cash_mask = base_fuzzy_mask & ~(
                (summary_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                (remark_col.str.contains(deposit_exclude_pattern, case=False, na=False)) |
                (type_col.str.contains(deposit_exclude_pattern, case=False, na=False))
            )
        else:
            fuzzy_cash_mask = base_fuzzy_mask

        # 存现模糊匹配
        fuzzy_deposit_mask = fuzzy_cash_mask & (data[direction_column] == income_flag) & (round_amount_mask | amount_mask)
        if fuzzy_deposit_mask.any():
            data.loc[fuzzy_deposit_mask, '存取现标识'] = '存现'
            data.loc[fuzzy_deposit_mask, '收入金额'] = data.loc[fuzzy_deposit_mask, amount_column].abs()
            data.loc[fuzzy_deposit_mask, '识别置信度'] = self.low_priority_confidence
            data.loc[fuzzy_deposit_mask, '识别原因'] = '低优先级上下文分析'

        # 取现模糊匹配
        fuzzy_withdraw_mask = fuzzy_cash_mask & (data[direction_column] == expense_flag) & (round_amount_mask | amount_mask)
        if fuzzy_withdraw_mask.any():
            data.loc[fuzzy_withdraw_mask, '存取现标识'] = '取现'
            data.loc[fuzzy_withdraw_mask, '支出金额'] = data.loc[fuzzy_withdraw_mask, amount_column].abs()
            data.loc[fuzzy_withdraw_mask, '识别置信度'] = self.low_priority_confidence
            data.loc[fuzzy_withdraw_mask, '识别原因'] = '低优先级上下文分析'

    def _amount_based_analysis(self, data: pd.DataFrame, amount_column: str):
        """基于金额的智能分析"""
        # 异常大额存取现（可能是误识别）
        large_amount_mask = (data['存取现标识'].isin(['存现', '取现'])) & (data[amount_column].abs() > self.large_amount_threshold)

        if large_amount_mask.any():
            # 降低大额交易的置信度
            data.loc[large_amount_mask, '识别置信度'] *= 0.8
            data.loc[large_amount_mask, '识别原因'] += ' (大额交易置信度调整)'

        # 小额存取现（可能是找零或测试）
        small_amount_mask = (data['存取现标识'].isin(['存现', '取现'])) & (data[amount_column].abs() < self.small_amount_threshold)

        if small_amount_mask.any():
            # 降低小额交易的置信度
            data.loc[small_amount_mask, '识别置信度'] *= 0.7
            data.loc[small_amount_mask, '识别原因'] += ' (小额交易置信度调整)'

    def get_recognition_stats(self, data: pd.DataFrame) -> Dict[str, any]:
        """
        获取识别统计信息

        Parameters:
        -----------
        data : pd.DataFrame
            已识别的数据

        Returns:
        --------
        Dict[str, any]
            识别统计信息
        """
        stats = {
            '总记录数': len(data),
            '存现记录数': len(data[data['存取现标识'] == '存现']),
            '取现记录数': len(data[data['存取现标识'] == '取现']),
            '转账记录数': len(data[data['存取现标识'] == '转账']),
        }

        if self.enable_enhanced_algorithm and '识别置信度' in data.columns:
            cash_data = data[data['存取现标识'].isin(['存现', '取现'])]
            if not cash_data.empty:
                stats['平均置信度'] = cash_data['识别置信度'].mean()
                stats['高置信度记录数'] = len(cash_data[cash_data['识别置信度'] >= 0.8])
                stats['中置信度记录数'] = len(cash_data[(cash_data['识别置信度'] >= 0.6) & (cash_data['识别置信度'] < 0.8)])
                stats['低置信度记录数'] = len(cash_data[cash_data['识别置信度'] < 0.6])

        return stats
