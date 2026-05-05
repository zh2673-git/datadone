#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""分析引擎 - 编排所有子分析器的执行（11步升级版）"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.models.analysis_result import AnalysisResult
from src.analysis.cash_recognition import CashRecognizer
from src.analysis.frequency import FrequencyAnalyzer
from src.analysis.special_analysis import SpecialAnalyzer
from src.analysis.cross_analysis import CrossAnalyzer
from src.analysis.key_transaction import KeyTransactionAnalyzer
from src.analysis.fund_tracking import FundTracker
from src.analysis.advanced import AdvancedAnalyzer
from src.analysis.baseline import BaselineAnalyzer
from src.analysis.pattern_recognizer import PatternRecognizer
from src.analysis.timeline_analyzer import TimelineAnalyzer
from src.analysis.risk_assessor import RiskAssessor

logger = logging.getLogger(__name__)


class AnalysisEngine:
    """分析引擎 - 编排所有子分析器，统一入口"""

    def __init__(self, keywords=None, thresholds=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.keywords = keywords
        self.thresholds = thresholds

        # 初始化子分析器（原有7个）
        self.cash_recognizer = CashRecognizer(keywords, thresholds)
        self.frequency_analyzer = FrequencyAnalyzer()
        self.special_analyzer = SpecialAnalyzer(keywords, thresholds)
        self.cross_analyzer = CrossAnalyzer()
        self.key_transaction_analyzer = KeyTransactionAnalyzer(keywords, thresholds)
        self.fund_tracker = FundTracker(keywords, thresholds)
        self.advanced_analyzer = AdvancedAnalyzer(thresholds)

        # 新增4个分析器
        self.baseline_analyzer = BaselineAnalyzer(thresholds)
        self.pattern_recognizer = PatternRecognizer(thresholds)
        self.timeline_analyzer = TimelineAnalyzer(thresholds)
        self.risk_assessor = RiskAssessor(thresholds)

    def analyze(self, state: dict, analysis_type: str = 'all') -> AnalysisResult:
        """
        执行分析（11步升级版）

        Args:
            state: 全局状态，包含 raw_data 等已加载数据
            analysis_type: 分析类型
                'all' | 'frequency' | 'cash' | 'special' | 'cross' |
                'key_transaction' | 'fund_tracking' | 'advanced' | 'baseline' |
                'pattern' | 'timeline' | 'risk'
        Returns:
            AnalysisResult: 结构化分析结果
        """
        raw_data = state.get('raw_data', {})
        data_paths = state.get('data_paths', {})

        # 获取各平台数据
        bank_data = raw_data.get('bank', {})
        wechat_data = raw_data.get('wechat', {})
        alipay_data = raw_data.get('alipay', {})
        call_data = raw_data.get('call', {})

        # 合并各平台原始 DataFrame
        bank_all = self._merge_person_data(bank_data) if isinstance(bank_data, dict) else bank_data
        wechat_all = self._merge_person_data(wechat_data) if isinstance(wechat_data, dict) else wechat_data
        alipay_all = self._merge_person_data(alipay_data) if isinstance(alipay_data, dict) else alipay_data
        call_all = self._merge_person_data(call_data) if isinstance(call_data, dict) else call_data

        # 构建分析用的统一数据结构
        all_bill_data = {
            '银行': bank_data,
            '微信': wechat_data,
            '支付宝': alipay_data,
        }

        # 提取所有本方姓名
        persons = self._extract_persons(bank_data, wechat_data, alipay_data)

        # 初始化结果
        cash_result = {}
        freq_result = {}
        call_freq_result = []
        cross_result = {}
        key_result = {}
        special_dates = {}
        special_amounts = {}
        fund_result = []
        cash_call_result = []
        advanced_result = {}
        bank_deposit = {}
        bank_withdraw = {}

        # 新增结果变量
        baseline_result = {}
        anomalies_result = {}
        patterns_result = {}
        timeline_result = {}
        risk_result = {}

        # ===== 原有7步 =====

        # 1. 存取现识别
        if analysis_type in ('all', 'cash'):
            self.logger.info("执行存取现识别...")
            cash_result = self.cash_recognizer.analyze(bank_data if isinstance(bank_data, dict) else {})
            bank_all = self._merge_person_data(bank_data) if isinstance(bank_data, dict) else bank_data

        # 为所有平台计算收入/支出金额
        self._calc_all_income_expense(bank_all, wechat_all, alipay_all)

        # 2. 频率分析
        if analysis_type in ('all', 'frequency'):
            self.logger.info("执行频率分析...")
            freq_result = self.frequency_analyzer.analyze(all_bill_data)
            if call_data:
                call_df = call_all if isinstance(call_all, pd.DataFrame) else self._merge_person_data(call_data)
                if call_df is not None and isinstance(call_df, pd.DataFrame):
                    call_freq_result = self.frequency_analyzer.analyze_calls(call_df)

        # 3. 特殊分析
        if analysis_type in ('all', 'special'):
            self.logger.info("执行特殊分析...")
            special_output = self.special_analyzer.analyze(all_bill_data)
            special_dates = special_output.dates
            special_amounts = special_output.amounts

        # 4. 综合交叉分析
        if analysis_type in ('all', 'cross'):
            self.logger.info("执行综合交叉分析...")
            cross_result = self.cross_analyzer.analyze(freq_result, call_freq_result)

        # 5. 重点收支识别
        if analysis_type in ('all', 'key_transaction'):
            self.logger.info("执行重点收支识别...")
            key_result = self.key_transaction_analyzer.analyze(all_bill_data)

        # 6. 大额资金追踪
        if analysis_type in ('all', 'fund_tracking'):
            self.logger.info("执行大额资金追踪...")
            call_df_for_tracking = call_all if isinstance(call_all, pd.DataFrame) else None
            fund_output = self.fund_tracker.analyze(all_bill_data, cash_result, call_df_for_tracking)
            fund_result = fund_output.tracking
            cash_call_result = fund_output.cash_call_match

        # 7. 高级分析
        if analysis_type in ('all', 'advanced'):
            self.logger.info("执行高级分析...")
            advanced_result = self.advanced_analyzer.analyze(all_bill_data)

        # ===== 新增4步 =====

        # 8. 行为基线（依赖存取现标识等前置结果）
        if analysis_type in ('all', 'baseline'):
            self.logger.info("执行行为基线分析...")
            baseline_result = self.baseline_analyzer.analyze(all_bill_data, call_all)

        # 9. 异常检测（基于基线）
        if analysis_type in ('all', 'advanced') and baseline_result:
            self.logger.info("执行基于基线的异常检测...")
            anomalies_result = self.advanced_analyzer.detect_anomalies_with_baseline(
                all_bill_data, baseline_result
            )

        # 10. 行为模式识别
        if analysis_type in ('all', 'pattern'):
            self.logger.info("执行行为模式识别...")
            patterns_result = self.pattern_recognizer.analyze(
                all_bill_data, call_all, baseline_result, anomalies_result
            )

        # 10. 时序链分析
        if analysis_type in ('all', 'timeline'):
            self.logger.info("执行时序链分析...")
            timeline_result = self.timeline_analyzer.analyze(all_bill_data, call_all)

        # 11. 风险研判
        if analysis_type in ('all', 'risk'):
            self.logger.info("执行风险研判...")
            risk_result = self.risk_assessor.assess(
                persons, baseline_result, anomalies_result,
                patterns_result, timeline_result, cross_result,
                call_freq_result
            )

        # 存取现统计
        if bank_all is not None and isinstance(bank_all, pd.DataFrame) and not bank_all.empty:
            for person in persons:
                person_data = bank_all[bank_all['本方姓名'] == person]
                bank_deposit[person] = person_data.loc[person_data['存取现标识'] == '存现', '收入金额'].sum()
                bank_withdraw[person] = person_data.loc[person_data['存取现标识'] == '取现', '支出金额'].sum()

        # 更新原始数据引用
        state['raw_data'] = {
            'bank': bank_all,
            'wechat': wechat_all,
            'alipay': alipay_all,
            'call': call_all,
        }

        return AnalysisResult(
            persons=persons,
            data_sources=data_paths,
            cash_recognition=cash_result,
            frequency=freq_result,
            call_frequency=call_freq_result,
            cross_analysis=cross_result,
            key_transactions=key_result,
            special_dates=special_dates,
            special_amounts=special_amounts,
            fund_tracking=fund_result,
            cash_call_match=cash_call_result,
            advanced=advanced_result,
            bank_cash_deposit_total=bank_deposit,
            bank_cash_withdrawal_total=bank_withdraw,
            # 新增
            baseline=baseline_result,
            anomalies=anomalies_result,
            patterns=patterns_result,
            timeline_chains=timeline_result,
            risk_assessment=risk_result,
        )

    def _merge_person_data(self, data) -> Optional[pd.DataFrame]:
        if data is None:
            return None
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, dict):
            dfs = [df for df in data.values() if isinstance(df, pd.DataFrame) and not df.empty]
            if dfs:
                return pd.concat(dfs, ignore_index=True)
        return None

    def _extract_persons(self, *data_sources) -> list[str]:
        persons = set()
        for data in data_sources:
            if data is None:
                continue
            if isinstance(data, dict):
                persons.update(data.keys())
            elif isinstance(data, pd.DataFrame) and not data.empty and '本方姓名' in data.columns:
                persons.update(data['本方姓名'].dropna().unique().tolist())
        return sorted(list(persons))

    @staticmethod
    def _calc_platform_income_expense(df: pd.DataFrame, income_label: str, expense_label: str) -> None:
        if '交易金额' not in df.columns or '借贷标识' not in df.columns:
            return
        abs_amount = df['交易金额'].abs()
        income_mask = df['借贷标识'] == income_label
        expense_mask = df['借贷标识'] == expense_label
        df.loc[income_mask, '收入金额'] = abs_amount[income_mask]
        df.loc[expense_mask, '支出金额'] = abs_amount[expense_mask]

    def _calc_all_income_expense(self, bank_all, wechat_all, alipay_all) -> None:
        if wechat_all is not None and isinstance(wechat_all, pd.DataFrame) and not wechat_all.empty:
            if '收入金额' in wechat_all.columns and wechat_all['收入金额'].sum() == 0:
                self._calc_platform_income_expense(wechat_all, '入', '出')

        if alipay_all is not None and isinstance(alipay_all, pd.DataFrame) and not alipay_all.empty:
            if '收入金额' in alipay_all.columns and alipay_all['收入金额'].sum() == 0:
                self._calc_platform_income_expense(alipay_all, '收入', '支出')
