#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""报告数据构建器 - 将 AnalysisResult 转换为导出器可消费的 ReportData"""

import logging
from typing import Dict, List, Optional
from datetime import date

import pandas as pd
import numpy as np

from src.models.analysis_result import AnalysisResult
from src.models.report import ReportData, PersonReportData, ExportConfig

logger = logging.getLogger(__name__)


class ReportBuilder:
    """报告数据构建器 - 将分析结果转换为导出器可消费的 ReportData"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def build(self, analysis_result: AnalysisResult, raw_data: Optional[dict[str, pd.DataFrame]] = None) -> ReportData:
        """
        构建报告数据

        Args:
            analysis_result: 分析引擎输出的分析结果
            raw_data: 原始DataFrame数据引用，用于计算汇总值
                      {'bank': DataFrame, 'wechat': DataFrame, 'alipay': DataFrame, 'call': DataFrame}
        Returns:
            ReportData: 报告数据
        """
        self._raw_data = raw_data or {}

        # 1. 构建Excel数据
        summary_table = self._build_summary_table(analysis_result)
        bill_frequency = self._build_bill_frequency(analysis_result)
        call_frequency = self._build_call_frequency(analysis_result)
        cross_analysis = self._build_cross_analysis(analysis_result)
        bank_raw = self._build_bank_raw(analysis_result)
        wechat_raw = self._build_wechat_raw(analysis_result)
        alipay_raw = self._build_alipay_raw(analysis_result)
        advanced_analysis = self._build_advanced(analysis_result)
        fund_tracking = self._build_fund_tracking(analysis_result)

        # 新增：三阶解构Excel数据
        baseline_data = self._build_baseline_data(analysis_result)
        anomaly_data = self._build_anomaly_data(analysis_result)
        pattern_data = self._build_pattern_data(analysis_result)
        timeline_data = self._build_timeline_data(analysis_result)
        risk_data = self._build_risk_data(analysis_result)
        cash_detail_data = self._build_cash_detail_data(analysis_result)
        key_transaction_data = self._build_key_transaction_data(analysis_result)

        # 2. 构建Word数据（每人一份）
        person_reports = {}
        for person in analysis_result.persons:
            person_reports[person] = self._build_person_report(person, analysis_result)

        # 3. 构建综合交叉分析汇总
        cross_summary = self._build_cross_summary(analysis_result)

        # 4. 提取基本信息
        bank_names = self._extract_bank_names(analysis_result)
        data_types = list(analysis_result.data_sources.keys())

        return ReportData(
            persons=analysis_result.persons,
            data_types=data_types,
            bank_names=bank_names,
            summary_table=summary_table,
            bill_frequency=bill_frequency,
            call_frequency=call_frequency,
            cross_analysis=cross_analysis,
            bank_raw=bank_raw,
            wechat_raw=wechat_raw,
            alipay_raw=alipay_raw,
            advanced_analysis=advanced_analysis,
            fund_tracking=fund_tracking,
            person_reports=person_reports,
            cross_analysis_summary=cross_summary,
            # 新增
            baseline_data=baseline_data,
            anomaly_data=anomaly_data,
            pattern_data=pattern_data,
            timeline_data=timeline_data,
            risk_data=risk_data,
            cash_detail_data=cash_detail_data,
            key_transaction_data=key_transaction_data,
        )

    # ------------------------------------------------------------------ #
    #  Excel数据构建
    # ------------------------------------------------------------------ #

    def _build_summary_table(self, ar: AnalysisResult) -> list[dict]:
        """分析汇总表：每人每平台3行（存现汇总/取现汇总/转账汇总）"""
        rows = []

        # 银行数据
        bank_df = self._raw_data.get('bank')
        if bank_df is not None and not bank_df.empty:
            for person in ar.persons:
                person_data = bank_df[bank_df['本方姓名'] == person]
                if person_data.empty:
                    continue

                # 数据来源
                data_source = ''
                if '数据来源' in person_data.columns:
                    ds_vals = person_data['数据来源'].dropna().unique()
                    if len(ds_vals) > 0:
                        data_source = str(ds_vals[0])

                # 存现汇总
                deposit_data = person_data[person_data['存取现标识'] == '存现']
                deposit_amount = deposit_data['收入金额'].sum() if not deposit_data.empty else 0
                if deposit_amount > 0:
                    rows.append({
                        '分析类型': '存现',
                        '平台': '银行',
                        '数据来源': data_source,
                        '本方姓名': person,
                        '存现金额': deposit_amount,
                        '取现金额': 0,
                        '转入金额': 0,
                        '转出金额': 0,
                    })

                # 取现汇总
                withdraw_data = person_data[person_data['存取现标识'] == '取现']
                withdraw_amount = withdraw_data['支出金额'].sum() if not withdraw_data.empty else 0
                if withdraw_amount > 0:
                    rows.append({
                        '分析类型': '取现',
                        '平台': '银行',
                        '数据来源': data_source,
                        '本方姓名': person,
                        '存现金额': 0,
                        '取现金额': withdraw_amount,
                        '转入金额': 0,
                        '转出金额': 0,
                    })

                # 转账汇总
                transfer_data = person_data[person_data['存取现标识'] == '转账']
                income = transfer_data['收入金额'].sum() if not transfer_data.empty else 0
                expense = transfer_data['支出金额'].sum() if not transfer_data.empty else 0
                if income > 0 or expense > 0:
                    rows.append({
                        '分析类型': '转账',
                        '平台': '银行',
                        '数据来源': data_source,
                        '本方姓名': person,
                        '存现金额': 0,
                        '取现金额': 0,
                        '转入金额': income,
                        '转出金额': expense,
                    })

        # 微信数据
        wechat_df = self._raw_data.get('wechat')
        if wechat_df is not None and not wechat_df.empty:
            for person in ar.persons:
                person_data = wechat_df[wechat_df['本方姓名'] == person]
                if person_data.empty:
                    continue

                data_source = ''
                if '数据来源' in person_data.columns:
                    ds_vals = person_data['数据来源'].dropna().unique()
                    if len(ds_vals) > 0:
                        data_source = str(ds_vals[0])

                income = person_data['收入金额'].sum() if '收入金额' in person_data.columns else 0
                expense = person_data['支出金额'].sum() if '支出金额' in person_data.columns else 0
                if income > 0 or expense > 0:
                    rows.append({
                        '分析类型': '转账',
                        '平台': '微信',
                        '数据来源': data_source,
                        '本方姓名': person,
                        '存现金额': 0,
                        '取现金额': 0,
                        '转入金额': income,
                        '转出金额': expense,
                    })

        # 支付宝数据
        alipay_df = self._raw_data.get('alipay')
        if alipay_df is not None and not alipay_df.empty:
            for person in ar.persons:
                person_data = alipay_df[alipay_df['本方姓名'] == person]
                if person_data.empty:
                    continue

                data_source = ''
                if '数据来源' in person_data.columns:
                    ds_vals = person_data['数据来源'].dropna().unique()
                    if len(ds_vals) > 0:
                        data_source = str(ds_vals[0])

                income = person_data['收入金额'].sum() if '收入金额' in person_data.columns else 0
                expense = person_data['支出金额'].sum() if '支出金额' in person_data.columns else 0
                if income > 0 or expense > 0:
                    rows.append({
                        '分析类型': '转账',
                        '平台': '支付宝',
                        '数据来源': data_source,
                        '本方姓名': person,
                        '存现金额': 0,
                        '取现金额': 0,
                        '转入金额': income,
                        '转出金额': expense,
                    })

        return rows

    def _build_bill_frequency(self, ar: AnalysisResult) -> list[dict]:
        """账单类频率表"""
        rows = []
        for platform, items in ar.frequency.items():
            for item in items:
                row = item.model_dump()
                # 字段名映射：model 字段名 → Excel 列名
                row['平台'] = row.pop('platform', '')
                row['数据来源'] = row.pop('data_source', '')
                # NaN → 空字符串
                for k, v in row.items():
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        row[k] = ''
                rows.append(row)

        # 计算收入占比和支出占比
        total_income = sum(r.get('收入总额', 0) for r in rows)
        total_expense = sum(r.get('支出总额', 0) for r in rows)

        for r in rows:
            r['交易总金额'] = r.get('收入总额', 0) + r.get('支出总额', 0)
            if total_income > 0:
                r['收入占比'] = f"{r.get('收入总额', 0) / total_income * 100:.2f}%"
            else:
                r['收入占比'] = "0.00%"
            if total_expense > 0:
                r['支出占比'] = f"{r.get('支出总额', 0) / total_expense * 100:.2f}%"
            else:
                r['支出占比'] = "0.00%"

        # 排序：本方姓名 ASC, 交易总金额 DESC
        rows.sort(key=lambda x: (x.get('本方姓名', ''), -x.get('交易总金额', 0)))

        return rows

    def _build_call_frequency(self, ar: AnalysisResult) -> list[dict]:
        """话单类频率表"""
        rows = []
        for item in ar.call_frequency:
            row = item.model_dump()
            # 字段名映射
            row['平台'] = row.pop('platform', '')
            row['数据来源'] = row.pop('data_source', '')
            # NaN → 空字符串
            for k, v in row.items():
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    row[k] = ''
            rows.append(row)

        # 排序：本方姓名 ASC, 通话次数 DESC
        rows.sort(key=lambda x: (x.get('本方姓名', ''), -x.get('通话次数', 0)))

        return rows

    def _build_cross_analysis(self, ar: AnalysisResult) -> dict[str, list[dict]]:
        """综合分析表"""
        result = {}
        for base_name, items in ar.cross_analysis.items():
            rows = []
            for item in items:
                rows.append(item.model_dump())
            result[base_name] = rows
        return result

    def _build_bank_raw(self, ar: AnalysisResult) -> list[dict]:
        """银行分析原始数据+分析标记"""
        bank_df = self._raw_data.get('bank')
        if bank_df is None or bank_df.empty:
            return []

        # 预处理：NaN → 空字符串
        result = []
        for idx, row in bank_df.iterrows():
            raw_row = {}
            for col in row.index:
                val = row[col]
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    val = ''
                raw_row[col] = val
            # 添加分析类型标记
            tags = []
            if raw_row.get('存取现标识') == '存现':
                tags.append('存现数据')
            elif raw_row.get('存取现标识') == '取现':
                tags.append('取现数据')
            else:
                tags.append('转账数据')

            # 检查特殊金额
            person = raw_row.get('本方姓名', '')
            amount = abs(raw_row.get('交易金额', 0))
            special_items = ar.special_amounts.get(person, [])
            for si in special_items:
                if si.index == idx:
                    tags.append('特殊金额')
                    break

            # 检查整百数金额
            if amount > 0 and amount % 100 == 0 and amount >= 100:
                tags.append('整百数金额')

            # 检查特殊日期
            special_date_items = ar.special_dates.get(person, [])
            for sdi in special_date_items:
                if sdi.index == idx:
                    tags.append('特殊日期')
                    break

            # 检查重点收支
            key_items = ar.key_transactions.get(person, [])
            for ki in key_items:
                if ki.index == idx:
                    if ki.交易方向 == '收入':
                        tags.append('重点收入')
                    else:
                        tags.append('重点支出')
                    break

            raw_row['分析类型'] = '|'.join(tags)
            result.append(raw_row)

        return result

    def _build_wechat_raw(self, ar: AnalysisResult) -> list[dict]:
        """微信分析原始数据+分析标记"""
        wechat_df = self._raw_data.get('wechat')
        if wechat_df is None or wechat_df.empty:
            return []

        result = []
        for idx, row in wechat_df.iterrows():
            raw_row = {}
            for col in row.index:
                val = row[col]
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    val = ''
                raw_row[col] = val
            tags = ['转账数据']

            person = raw_row.get('本方姓名', '')
            amount = abs(raw_row.get('交易金额', 0))

            # 检查特殊金额
            special_items = ar.special_amounts.get(person, [])
            for si in special_items:
                if si.index == idx:
                    tags.append('特殊金额')
                    break

            # 整百数金额
            if amount > 0 and amount % 100 == 0 and amount >= 100:
                tags.append('整百数金额')

            # 特殊日期
            special_date_items = ar.special_dates.get(person, [])
            for sdi in special_date_items:
                if sdi.index == idx:
                    tags.append('特殊日期')
                    break

            # 重点收支
            key_items = ar.key_transactions.get(person, [])
            for ki in key_items:
                if ki.index == idx:
                    if ki.交易方向 == '收入':
                        tags.append('重点收入')
                    else:
                        tags.append('重点支出')
                    break

            raw_row['分析类型'] = '|'.join(tags)
            result.append(raw_row)

        return result

    def _build_alipay_raw(self, ar: AnalysisResult) -> list[dict]:
        """支付宝分析原始数据+分析标记"""
        alipay_df = self._raw_data.get('alipay')
        if alipay_df is None or alipay_df.empty:
            return []

        result = []
        for idx, row in alipay_df.iterrows():
            raw_row = {}
            for col in row.index:
                val = row[col]
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    val = ''
                raw_row[col] = val
            tags = ['转账数据']

            person = raw_row.get('本方姓名', '')
            amount = abs(raw_row.get('交易金额', 0))

            # 检查特殊金额
            special_items = ar.special_amounts.get(person, [])
            for si in special_items:
                if si.index == idx:
                    tags.append('特殊金额')
                    break

            # 整百数金额
            if amount > 0 and amount % 100 == 0 and amount >= 100:
                tags.append('整百数金额')

            # 特殊日期
            special_date_items = ar.special_dates.get(person, [])
            for sdi in special_date_items:
                if sdi.index == idx:
                    tags.append('特殊日期')
                    break

            # 重点收支
            key_items = ar.key_transactions.get(person, [])
            for ki in key_items:
                if ki.index == idx:
                    if ki.交易方向 == '收入':
                        tags.append('重点收入')
                    else:
                        tags.append('重点支出')
                    break

            raw_row['分析类型'] = '|'.join(tags)
            result.append(raw_row)

        return result

    def _build_advanced(self, ar: AnalysisResult) -> list[dict]:
        """高级分析表 - 将各类分析结果展平为统一的5列格式"""
        rows = []
        for person, adv_result in ar.advanced.items():
            # 时间模式
            for item in adv_result.时间模式:
                rows.extend(self._flatten_advanced_item(person, '时间模式', item))
            # 金额模式
            for item in adv_result.金额模式:
                rows.extend(self._flatten_advanced_item(person, '金额模式', item))
            # 异常交易
            for item in adv_result.异常交易:
                rows.extend(self._flatten_advanced_item(person, '异常交易', item))
            # 交易模式
            for item in adv_result.交易模式:
                rows.extend(self._flatten_advanced_item(person, '交易模式', item))

        return rows

    @staticmethod
    def _flatten_advanced_item(person: str, analysis_type: str, item: dict) -> list[dict]:
        """将高级分析的嵌套 dict 展平为统一格式行"""
        rows = []
        item_type = item.get('类型', item.get('指标', analysis_type))

        for key, value in item.items():
            if key in ('类型', '指标'):
                continue
            if isinstance(value, dict):
                # 嵌套结构，展开为多行
                sub_desc = ', '.join(f'{k}={v}' for k, v in value.items())
                rows.append({
                    '姓名': person,
                    '分析类型': analysis_type,
                    '具体指标': f"{item_type}-{key}",
                    '数值': '',
                    '说明': sub_desc,
                })
            else:
                rows.append({
                    '姓名': person,
                    '分析类型': analysis_type,
                    '具体指标': f"{item_type}-{key}",
                    '数值': str(value),
                    '说明': '',
                })

        return rows

    def _build_fund_tracking(self, ar: AnalysisResult) -> list[dict]:
        """大额资金跟踪表"""
        rows = []
        for item in ar.fund_tracking:
            rows.append(item.model_dump())
        for item in ar.cash_call_match:
            rows.append(item.model_dump())
        return rows

    # ------------------------------------------------------------------ #
    #  Word数据构建（逐人报告数据）
    # ------------------------------------------------------------------ #

    def _build_person_report(self, person: str, ar: AnalysisResult) -> PersonReportData:
        """构建单人报告数据"""
        # 基础资金体量
        bank_income, bank_expense = 0.0, 0.0
        wechat_income, wechat_expense = 0.0, 0.0
        alipay_income, alipay_expense = 0.0, 0.0
        time_span = ""
        account_balance = None
        most_used_bank = ""
        annual_top3 = {}
        opponent_top3 = []

        # 存取现
        deposit_total = 0.0
        withdraw_total = 0.0
        large_cash = {}
        bank_transfer_total = 0.0
        large_transfer = {}

        # 从原始数据计算
        bank_df = self._raw_data.get('bank')
        wechat_df = self._raw_data.get('wechat')
        alipay_df = self._raw_data.get('alipay')

        # ---------- 银行数据 ----------
        if bank_df is not None and not bank_df.empty:
            person_bank = bank_df[bank_df['本方姓名'] == person]
            if not person_bank.empty:
                # 银行总进账/出账（转账部分）
                transfer_data = person_bank[person_bank['存取现标识'] == '转账']
                bank_income = transfer_data['收入金额'].sum() if not transfer_data.empty else 0
                bank_expense = transfer_data['支出金额'].sum() if not transfer_data.empty else 0

                # 存取现
                deposit_data = person_bank[person_bank['存取现标识'] == '存现']
                withdraw_data = person_bank[person_bank['存取现标识'] == '取现']
                deposit_total = deposit_data['收入金额'].sum() if not deposit_data.empty else 0
                withdraw_total = withdraw_data['支出金额'].sum() if not withdraw_data.empty else 0

                # 单笔万以上存取现
                large_deposit = deposit_data[deposit_data['收入金额'] >= 10000] if not deposit_data.empty else pd.DataFrame()
                large_withdraw = withdraw_data[withdraw_data['支出金额'] >= 10000] if not withdraw_data.empty else pd.DataFrame()
                large_cash_count = len(large_deposit) + len(large_withdraw)
                large_cash_amount = (large_deposit['收入金额'].sum() if not large_deposit.empty else 0) + \
                                    (large_withdraw['支出金额'].sum() if not large_withdraw.empty else 0)
                large_cash = {'次数': large_cash_count, '金额': large_cash_amount}

                # 银行总转账金额
                bank_transfer_total = transfer_data['交易金额'].abs().sum() if not transfer_data.empty else 0

                # 单笔5万以上转账
                large_transfer_data = transfer_data[transfer_data['交易金额'].abs() >= 50000] if not transfer_data.empty else pd.DataFrame()
                large_transfer = {
                    '次数': len(large_transfer_data),
                    '金额': large_transfer_data['交易金额'].abs().sum() if not large_transfer_data.empty else 0,
                }

                # 时间跨度
                if '交易日期' in person_bank.columns:
                    dates = pd.to_datetime(person_bank['交易日期'], errors='coerce')
                    min_date = dates.min()
                    max_date = dates.max()
                    if pd.notna(min_date) and pd.notna(max_date):
                        time_span = f"{min_date.strftime('%Y-%m-%d')}~{max_date.strftime('%Y-%m-%d')}"

                # 账户余额
                if '账户余额' in person_bank.columns:
                    try:
                        sorted_data = person_bank.sort_values('交易日期')
                        last_balance = sorted_data['账户余额'].dropna().iloc[-1] if not sorted_data['账户余额'].dropna().empty else None
                        if last_balance is not None:
                            account_balance = float(last_balance)
                    except (IndexError, ValueError):
                        pass

                # 最常用银行
                if '银行类型' in person_bank.columns:
                    bank_counts = person_bank['银行类型'].value_counts()
                    if not bank_counts.empty:
                        most_used_bank = bank_counts.index[0]

                # 年度TOP3
                if '交易日期' in person_bank.columns:
                    person_bank_copy = person_bank.copy()
                    person_bank_copy['年份'] = pd.to_datetime(person_bank_copy['交易日期'], errors='coerce').dt.year
                    yearly = person_bank_copy.groupby('年份').agg({
                        '收入金额': 'sum', '支出金额': 'sum'
                    }).reset_index()
                    yearly['总体量'] = yearly['收入金额'] + yearly['支出金额']
                    top3 = yearly.nlargest(3, '总体量')
                    bank_top3 = []
                    for _, row in top3.iterrows():
                        bank_top3.append({
                            '年份': int(row['年份']),
                            '总进账': row['收入金额'],
                            '总出账': row['支出金额'],
                            '总体量': row['总体量'],
                        })
                    if bank_top3:
                        annual_top3['银行'] = bank_top3

                # 交易对手TOP3
                if '对方姓名' in person_bank.columns:
                    opponents = person_bank[person_bank['对方姓名'].notna() & (person_bank['对方姓名'] != '')]
                    if not opponents.empty:
                        opp_stats = opponents.groupby('对方姓名')['交易金额'].agg(['sum', 'count'])
                        opp_stats['总金额'] = opp_stats['sum'].abs()
                        top3_opp = opp_stats.nlargest(3, '总金额')
                        for name, row in top3_opp.iterrows():
                            opponent_top3.append({'对方姓名': name, '总金额': row['总金额']})

        # ---------- 微信数据 ----------
        if wechat_df is not None and not wechat_df.empty:
            person_wechat = wechat_df[wechat_df['本方姓名'] == person]
            if not person_wechat.empty:
                wechat_income = person_wechat['收入金额'].sum() if '收入金额' in person_wechat.columns else 0
                wechat_expense = person_wechat['支出金额'].sum() if '支出金额' in person_wechat.columns else 0

                # 补充时间跨度
                if not time_span and '交易日期' in person_wechat.columns:
                    dates = pd.to_datetime(person_wechat['交易日期'], errors='coerce')
                    min_d = dates.min()
                    max_d = dates.max()
                    if pd.notna(min_d) and pd.notna(max_d):
                        time_span = f"{min_d.strftime('%Y-%m-%d')}~{max_d.strftime('%Y-%m-%d')}"

                # 年度TOP3
                if '交易日期' in person_wechat.columns:
                    person_wechat_copy = person_wechat.copy()
                    person_wechat_copy['年份'] = pd.to_datetime(person_wechat_copy['交易日期'], errors='coerce').dt.year
                    yearly = person_wechat_copy.groupby('年份').agg({
                        '收入金额': 'sum', '支出金额': 'sum'
                    }).reset_index()
                    yearly['总体量'] = yearly['收入金额'] + yearly['支出金额']
                    top3 = yearly.nlargest(3, '总体量')
                    wechat_top3 = []
                    for _, row in top3.iterrows():
                        wechat_top3.append({
                            '年份': int(row['年份']),
                            '总进账': row['收入金额'],
                            '总出账': row['支出金额'],
                            '总体量': row['总体量'],
                        })
                    if wechat_top3:
                        annual_top3['微信'] = wechat_top3

                # 交易对手TOP3
                if '对方姓名' in person_wechat.columns:
                    opponents = person_wechat[person_wechat['对方姓名'].notna() & (person_wechat['对方姓名'] != '')]
                    if not opponents.empty:
                        opp_stats = opponents.groupby('对方姓名')['交易金额'].agg(['sum', 'count'])
                        opp_stats['总金额'] = opp_stats['sum'].abs()
                        top3_opp = opp_stats.nlargest(3, '总金额')
                        for name, row in top3_opp.iterrows():
                            opponent_top3.append({'对方姓名': name, '总金额': row['总金额']})

        # ---------- 支付宝数据 ----------
        if alipay_df is not None and not alipay_df.empty:
            person_alipay = alipay_df[alipay_df['本方姓名'] == person]
            if not person_alipay.empty:
                alipay_income = person_alipay['收入金额'].sum() if '收入金额' in person_alipay.columns else 0
                alipay_expense = person_alipay['支出金额'].sum() if '支出金额' in person_alipay.columns else 0

                # 补充时间跨度
                if not time_span and '交易日期' in person_alipay.columns:
                    dates = pd.to_datetime(person_alipay['交易日期'], errors='coerce')
                    min_d = dates.min()
                    max_d = dates.max()
                    if pd.notna(min_d) and pd.notna(max_d):
                        time_span = f"{min_d.strftime('%Y-%m-%d')}~{max_d.strftime('%Y-%m-%d')}"

                # 年度TOP3
                if '交易日期' in person_alipay.columns:
                    person_alipay_copy = person_alipay.copy()
                    person_alipay_copy['年份'] = pd.to_datetime(person_alipay_copy['交易日期'], errors='coerce').dt.year
                    yearly = person_alipay_copy.groupby('年份').agg({
                        '收入金额': 'sum', '支出金额': 'sum'
                    }).reset_index()
                    yearly['总体量'] = yearly['收入金额'] + yearly['支出金额']
                    top3 = yearly.nlargest(3, '总体量')
                    alipay_top3 = []
                    for _, row in top3.iterrows():
                        alipay_top3.append({
                            '年份': int(row['年份']),
                            '总进账': row['收入金额'],
                            '总出账': row['支出金额'],
                            '总体量': row['总体量'],
                        })
                    if alipay_top3:
                        annual_top3['支付宝'] = alipay_top3

        # 从AnalysisResult提取分析数据

        # 纯收入对手 / 纯支出对手
        pure_income_opponents = []
        pure_expense_opponents = []
        for platform, items in ar.frequency.items():
            for item in items:
                if item.本方姓名 != person:
                    continue
                if item.收入总额 > 0 and item.支出总额 == 0:
                    pure_income_opponents.append({'对方姓名': item.对方姓名, '金额': item.收入总额})
                if item.支出总额 > 0 and item.收入总额 == 0:
                    pure_expense_opponents.append({'对方姓名': item.对方姓名, '金额': item.支出总额})
        pure_income_opponents.sort(key=lambda x: x['金额'], reverse=True)
        pure_expense_opponents.sort(key=lambda x: x['金额'], reverse=True)

        # 特殊金额
        love_transactions = []
        other_special_transactions = []
        for item in ar.special_amounts.get(person, []):
            tx = {
                '日期': str(item.交易日期) if hasattr(item, '交易日期') else '',
                '金额': item.交易金额,
                '对方': '',
            }
            # 从原始数据获取对方姓名
            for platform_df in [bank_df, wechat_df, alipay_df]:
                if platform_df is not None and not platform_df.empty:
                    if item.index in platform_df.index:
                        tx['对方'] = platform_df.loc[item.index, '对方姓名']
                        break
            if item.特殊类型 == '爱情数字':
                love_transactions.append(tx)
            else:
                other_special_transactions.append(tx)

        # 特殊日期
        special_date_transactions = []
        for item in ar.special_dates.get(person, []):
            tx = {
                '日期': str(item.交易日期),
                '金额': 0,
                '对方': '',
                '节日名': item.特殊日期名称,
            }
            # 从原始数据获取金额和对方
            for platform_df in [bank_df, wechat_df, alipay_df]:
                if platform_df is not None and not platform_df.empty:
                    if item.index in platform_df.index:
                        row_data = platform_df.loc[item.index]
                        tx['金额'] = abs(row_data.get('交易金额', 0))
                        tx['对方'] = row_data.get('对方姓名', '')
                        break
            special_date_transactions.append(tx)

        # 重点收支
        work_income = {}
        property_income = {}
        property_expense = {}
        rent_income = {}
        rent_expense = {}
        vehicle_income = {}
        vehicle_expense = {}
        security_income = {}
        security_expense = {}

        for item in ar.key_transactions.get(person, []):
            tx_data = {
                '总额': item.交易金额,
                '时间范围': str(item.交易日期),
                '匹配关键词': item.匹配关键词,
            }
            if item.重点类型 == '工作收入' or item.重点子类 == '工资奖金':
                if work_income:
                    work_income['总额'] += item.交易金额
                else:
                    work_income = tx_data
            elif item.重点子类 in ('房产',):
                if item.交易方向 == '收入':
                    if property_income:
                        property_income['总额'] += item.交易金额
                    else:
                        property_income = tx_data
                else:
                    if property_expense:
                        property_expense['总额'] += item.交易金额
                    else:
                        property_expense = tx_data
            elif item.重点子类 in ('租金',):
                if item.交易方向 == '收入':
                    if rent_income:
                        rent_income['总额'] += item.交易金额
                    else:
                        rent_income = tx_data
                else:
                    if rent_expense:
                        rent_expense['总额'] += item.交易金额
                    else:
                        rent_expense = tx_data
            elif item.重点子类 in ('车辆', '车辆收入'):
                if item.交易方向 == '收入':
                    if vehicle_income:
                        vehicle_income['总额'] += item.交易金额
                    else:
                        vehicle_income = tx_data
                else:
                    if vehicle_expense:
                        vehicle_expense['总额'] += item.交易金额
                    else:
                        vehicle_expense = tx_data
            elif item.重点子类 in ('证券', '证券收入'):
                if item.交易方向 == '收入':
                    if security_income:
                        security_income['总额'] += item.交易金额
                    else:
                        security_income = tx_data
                else:
                    if security_expense:
                        security_expense['总额'] += item.交易金额
                    else:
                        security_expense = tx_data

        # 存取现话单匹配
        cash_call_match = []
        for item in ar.cash_call_match:
            if item.核心人员 == person:
                cash_call_match.append(item.model_dump())

        # 大额资金话单匹配
        large_amount_call_match = []
        for item in ar.fund_tracking:
            if item.核心人员 == person and item.交易金额 is not None and abs(item.交易金额) >= 50000:
                if item.通话对方:
                    large_amount_call_match.append(item.model_dump())

        # 大额资金跟踪层级
        fund_tracking_levels = []
        for item in ar.fund_tracking:
            if item.核心人员 == person:
                fund_tracking_levels.append(item.model_dump())

        # 交易对手TOP3去重排序
        seen = set()
        unique_opp_top3 = []
        for o in opponent_top3:
            if o['对方姓名'] not in seen:
                seen.add(o['对方姓名'])
                unique_opp_top3.append(o)
            if len(unique_opp_top3) >= 3:
                break

        return PersonReportData(
            本方姓名=person,
            银行总进账=bank_income,
            银行总出账=bank_expense,
            微信总进账=wechat_income,
            微信总出账=wechat_expense,
            支付宝总进账=alipay_income,
            支付宝总出账=alipay_expense,
            时间跨度=time_span,
            账户余额=account_balance,
            最常用银行=most_used_bank,
            各平台年度TOP3=annual_top3,
            交易对手TOP3=unique_opp_top3,
            存现总额=deposit_total,
            取现总额=withdraw_total,
            单笔万以上存取现=large_cash,
            银行总转账金额=bank_transfer_total,
            单笔5万以上转账=large_transfer,
            纯收入对手=pure_income_opponents[:5],
            纯支出对手=pure_expense_opponents[:5],
            爱情数字交易=love_transactions,
            其他特殊金额交易=other_special_transactions,
            特殊日期交易=special_date_transactions,
            工作收入=work_income,
            房产收入=property_income,
            房产支出=property_expense,
            租金收入=rent_income,
            租金支出=rent_expense,
            车辆收入=vehicle_income,
            车辆支出=vehicle_expense,
            证券收入=security_income,
            证券支出=security_expense,
            存取现话单匹配=cash_call_match,
            大额资金话单匹配=large_amount_call_match,
            大额资金跟踪层级=fund_tracking_levels,
            # 新增：三阶解构数据
            行为基线=ar.baseline.get(person),
            异常列表=ar.anomalies.get(person, []),
            行为模式=ar.patterns.get(person, []),
            时序链=ar.timeline_chains.get(person, []),
            风险研判=ar.risk_assessment.get(person),
        )

    def _build_cross_summary(self, ar: AnalysisResult) -> list[dict]:
        """综合交叉分析汇总"""
        rows = []
        for base_name, items in ar.cross_analysis.items():
            for item in items:
                row = item.model_dump()
                row['分析基准'] = base_name
                rows.append(row)
        return rows

    def _extract_bank_names(self, ar: AnalysisResult) -> list[str]:
        """提取涉及的银行名称"""
        bank_names = set()
        bank_df = self._raw_data.get('bank')
        if bank_df is not None and not bank_df.empty and '银行类型' in bank_df.columns:
            bank_names.update(bank_df['银行类型'].dropna().unique().tolist())
        return sorted(list(bank_names))

    # ------------------------------------------------------------------ #
    #  新增：三阶解构 Excel 数据构建
    # ------------------------------------------------------------------ #

    def _build_baseline_data(self, ar: AnalysisResult) -> list[dict]:
        """行为基线表"""
        rows = []
        for person, bl in ar.baseline.items():
            rows.append({
                '本方姓名': bl.本方姓名,
                '数据充足度': bl.数据充足度,
                '数据月数': bl.数据月数,
                '月均收入': bl.月均收入,
                '月均收入_标准差': bl.月均收入_std,
                '月均支出': bl.月均支出,
                '月均支出_标准差': bl.月均支出_std,
                '月均交易次数': bl.月均交易次数,
                '月均交易对手数': bl.月均交易对手数,
                '单笔金额均值': bl.单笔金额均值,
                '单笔金额中位数': bl.单笔金额中位数,
                '单笔金额P25': bl.单笔金额_P25,
                '单笔金额P75': bl.单笔金额_P75,
                '工作时间交易占比': bl.工作时间交易占比,
                '深夜交易占比': bl.深夜交易占比,
                '周末交易占比': bl.周末交易占比,
                '存取现月均金额': bl.存取现月均金额,
                '存取现占收支比': bl.存取现占收支比,
                '月均通话次数': bl.月均通话次数,
                '月均通话时长分钟': bl.月均通话时长分钟,
                '收入趋势': bl.收入趋势,
                '支出趋势': bl.支出趋势,
                '对手数趋势': bl.对手数趋势,
            })
        return rows

    def _build_anomaly_data(self, ar: AnalysisResult) -> list[dict]:
        """异常明细表"""
        rows = []
        for person, anomalies in ar.anomalies.items():
            for a in anomalies:
                rows.append({
                    '本方姓名': a.本方姓名,
                    '异常类型': a.异常类型,
                    '异常子类': a.异常子类,
                    '偏离度': a.偏离度,
                    '严重程度': a.严重程度,
                    '对方姓名': a.对方姓名,
                    '交易日期': a.交易日期,
                    '交易金额': a.交易金额,
                    '基线均值': a.基线均值,
                    '说明': a.说明,
                })
        return rows

    def _build_pattern_data(self, ar: AnalysisResult) -> list[dict]:
        """行为模式表"""
        rows = []
        for person, patterns in ar.patterns.items():
            for p in patterns:
                rows.append({
                    '本方姓名': person,
                    '模式编号': p.模式编号,
                    '模式名称': p.模式名称,
                    '匹配度': f"{p.匹配度:.0%}",
                    '涉及对手': p.涉及对手,
                    '关键证据': p.关键证据,
                    '满足条件': '；'.join(p.满足条件),
                    '未满足条件': '；'.join(p.未满足条件),
                    '报告用语': p.报告用语,
                })
        return rows

    def _build_timeline_data(self, ar: AnalysisResult) -> list[dict]:
        """时序链表"""
        rows = []
        for person, chains in ar.timeline_chains.items():
            for c in chains:
                # 时序链的事件描述
                event_desc = []
                for evt in c.事件序列:
                    if evt.事件类型 == '通话':
                        event_desc.append(f"{evt.时间}通话({evt.方向})")
                    else:
                        event_desc.append(f"{evt.时间}{evt.平台}{evt.方向}{evt.金额:,.0f}元")

                rows.append({
                    '本方姓名': c.本方姓名,
                    '对方姓名': c.对方姓名,
                    '链模式': c.链模式,
                    '链强度': c.链强度,
                    '重复次数': c.重复次数,
                    '时间窗口小时': c.时间窗口,
                    '事件序列': ' → '.join(event_desc),
                    '关键证据描述': c.关键证据描述,
                })
        return rows

    def _build_risk_data(self, ar: AnalysisResult) -> list[dict]:
        """风险研判表"""
        rows = []
        for person, ra in ar.risk_assessment.items():
            # 重点人员
            key_person_desc = []
            for kp in ra.重点人员:
                unit_info = f"({kp.对方单位})" if kp.对方单位 else ""
                key_person_desc.append(f"TOP{kp.排名} {kp.对方姓名}{unit_info} 通话{kp.通话次数}次 资金{kp.资金往来金额:,.0f}元")

            rows.append({
                '本方姓名': ra.本方姓名,
                '综合风险分数': ra.综合风险分数,
                '综合风险等级': ra.综合风险等级,
                '异常偏离度得分': ra.异常偏离度得分,
                '异常偏离度说明': ra.异常偏离度说明,
                '行为模式得分': ra.行为模式得分,
                '行为模式说明': ra.行为模式说明,
                '证据链得分': ra.证据链得分,
                '证据链说明': ra.证据链说明,
                '规模得分': ra.规模得分,
                '规模说明': ra.规模说明,
                '证据充分度': ra.证据充分度,
                '已有证据': '；'.join(ra.已有证据),
                '待补充证据': '；'.join(ra.待补充证据),
                '重点人员': '；'.join(key_person_desc),
                '调查方向建议': '；'.join(ra.调查方向建议),
                '重点时段': ra.重点时段,
            })
        return rows

    def _build_cash_detail_data(self, ar: AnalysisResult) -> list[dict]:
        """存取现明细表 — 逐笔存取现记录+大额标记+备注"""
        bank_df = self._raw_data.get('bank')
        if bank_df is None or bank_df.empty:
            return []

        rows = []
        for person in ar.persons:
            person_bank = bank_df[bank_df['本方姓名'] == person]
            if person_bank.empty:
                continue

            cash_data = person_bank[person_bank['存取现标识'].isin(['存现', '取现'])]
            if cash_data.empty:
                continue

            # 预计算：同日存取的日期集合
            date_cash_types = {}
            for _, row in cash_data.iterrows():
                date_val = row.get('交易日期', '')
                date_str = str(date_val)[:10] if pd.notna(date_val) and date_val else ''
                cash_type = row.get('存取现标识', '')
                if date_str:
                    if date_str not in date_cash_types:
                        date_cash_types[date_str] = set()
                    date_cash_types[date_str].add(cash_type)
            same_day_dates = {d for d, types in date_cash_types.items() if len(types) >= 2}

            # 预计算：连续取现的日期集合
            withdraw_dates = sorted(set(
                str(row.get('交易日期', ''))[:10]
                for _, row in cash_data.iterrows()
                if row.get('存取现标识') == '取现' and pd.notna(row.get('交易日期'))
            ))
            consecutive_dates = set()
            for i in range(1, len(withdraw_dates)):
                try:
                    from datetime import datetime as _dt
                    d1 = _dt.strptime(withdraw_dates[i - 1], '%Y-%m-%d')
                    d2 = _dt.strptime(withdraw_dates[i], '%Y-%m-%d')
                    if (d2 - d1).days == 1:
                        consecutive_dates.add(withdraw_dates[i - 1])
                        consecutive_dates.add(withdraw_dates[i])
                except (ValueError, TypeError):
                    pass

            for _, row in cash_data.iterrows():
                # 金额：存现取收入金额，取现取支出金额
                if row.get('存取现标识') == '存现':
                    amount = row.get('收入金额', 0)
                else:
                    amount = row.get('支出金额', 0)
                if not amount or (isinstance(amount, float) and pd.isna(amount)):
                    amount = abs(row.get('交易金额', 0)) if pd.notna(row.get('交易金额')) else 0

                date_val = row.get('交易日期', '')
                date_str = str(date_val)[:10] if pd.notna(date_val) and date_val else ''

                # 备注
                notes = []
                if date_str in same_day_dates:
                    notes.append('同日存取')
                if date_str in consecutive_dates and row.get('存取现标识') == '取现':
                    notes.append('连续取现')

                rows.append({
                    '本方姓名': person,
                    '交易日期': date_val,
                    '存取现标识': row.get('存取现标识', ''),
                    '交易金额': amount,
                    '对方姓名': row.get('对方姓名', ''),
                    '银行类型': row.get('银行类型', ''),
                    '账户余额': row.get('账户余额', ''),
                    '大额标记': '★' if abs(amount) >= 50000 else '',
                    '备注': '；'.join(notes) if notes else '',
                })

        return rows

    def _build_key_transaction_data(self, ar: AnalysisResult) -> list[dict]:
        """重点收支明细表 — 逐笔重点收支+特殊金额+特殊日期+规避阈值"""
        rows = []

        # ---------- 1. 重点收支（工作收入/房产/租金/车辆/证券/大额/规避阈值） ----------
        for person in ar.persons:
            key_items = ar.key_transactions.get(person, [])
            if not key_items:
                continue

            # 获取原始数据用于补充信息
            bank_df = self._raw_data.get('bank')
            wechat_df = self._raw_data.get('wechat')
            alipay_df = self._raw_data.get('alipay')

            for item in key_items:
                # 从原始数据获取更多字段
                counterparty = ''
                platform = ''
                for platform_name, platform_df in [('银行', bank_df), ('微信', wechat_df), ('支付宝', alipay_df)]:
                    if platform_df is not None and not platform_df.empty and item.index in platform_df.index:
                        try:
                            row_data = platform_df.loc[item.index]
                            counterparty = row_data.get('对方姓名', '')
                            if pd.notna(counterparty):
                                counterparty = str(counterparty)
                            else:
                                counterparty = ''
                            platform = platform_name
                        except (KeyError, TypeError):
                            pass
                        break

                rows.append({
                    '本方姓名': person,
                    '交易日期': str(item.交易日期) if item.交易日期 else '',
                    '交易方向': item.交易方向,
                    '交易金额': item.交易金额,
                    '对方姓名': counterparty,
                    '重点类型': item.重点类型,
                    '重点子类': item.重点子类,
                    '特征描述': item.匹配关键词,
                    '置信度': f"{item.置信度:.0%}",
                    '数据来源': platform,
                })

        # ---------- 2. 特殊金额交易（过滤≥500元的有意义交易） ----------
        for person in ar.persons:
            special_amount_items = ar.special_amounts.get(person, [])
            if not special_amount_items:
                continue

            bank_df = self._raw_data.get('bank')
            wechat_df = self._raw_data.get('wechat')
            alipay_df = self._raw_data.get('alipay')

            for item in special_amount_items:
                # 只保留≥500元的特殊金额交易
                if item.交易金额 < 500:
                    continue

                # 从原始数据获取补充信息
                counterparty = ''
                tx_date = ''
                tx_direction = ''
                platform = ''
                for platform_name, platform_df in [('银行', bank_df), ('微信', wechat_df), ('支付宝', alipay_df)]:
                    if platform_df is not None and not platform_df.empty and item.index in platform_df.index:
                        try:
                            row_data = platform_df.loc[item.index]
                            counterparty = row_data.get('对方姓名', '')
                            if pd.notna(counterparty):
                                counterparty = str(counterparty)
                            else:
                                counterparty = ''
                            tx_date = str(row_data.get('交易日期', '')) if pd.notna(row_data.get('交易日期')) else ''
                            tx_direction = '收入' if row_data.get('收入金额', 0) > 0 else '支出'
                            platform = platform_name
                        except (KeyError, TypeError):
                            pass
                        break

                # 判断是否已被重点收支覆盖（避免重复）
                already_covered = any(
                    r.get('本方姓名') == person and r.get('重点类型') != '特殊金额'
                    and r.get('交易金额') == item.交易金额
                    for r in rows
                )
                if already_covered:
                    continue

                rows.append({
                    '本方姓名': person,
                    '交易日期': tx_date,
                    '交易方向': tx_direction,
                    '交易金额': item.交易金额,
                    '对方姓名': counterparty,
                    '重点类型': '特殊金额',
                    '重点子类': item.特殊类型,
                    '特征描述': f"特殊金额{item.特殊金额名}",
                    '置信度': '',
                    '数据来源': platform,
                })

        # ---------- 3. 特殊日期交易（过滤≥5000元大额） ----------
        for person in ar.persons:
            special_date_items = ar.special_dates.get(person, [])
            if not special_date_items:
                continue

            bank_df = self._raw_data.get('bank')
            wechat_df = self._raw_data.get('wechat')
            alipay_df = self._raw_data.get('alipay')

            for item in special_date_items:
                # 从原始数据获取补充信息
                counterparty = ''
                tx_amount = 0
                tx_direction = ''
                platform = ''
                for platform_name, platform_df in [('银行', bank_df), ('微信', wechat_df), ('支付宝', alipay_df)]:
                    if platform_df is not None and not platform_df.empty and item.index in platform_df.index:
                        try:
                            row_data = platform_df.loc[item.index]
                            counterparty = row_data.get('对方姓名', '')
                            if pd.notna(counterparty):
                                counterparty = str(counterparty)
                            else:
                                counterparty = ''
                            tx_amount = abs(float(row_data.get('交易金额', 0))) if pd.notna(row_data.get('交易金额')) else 0
                            tx_direction = '收入' if row_data.get('收入金额', 0) > 0 else '支出'
                            platform = platform_name
                        except (KeyError, TypeError):
                            pass
                        break

                # 只保留≥5000元的特殊日期交易
                if tx_amount < 5000:
                    continue

                # 判断是否已被重点收支覆盖
                already_covered = any(
                    r.get('本方姓名') == person and r.get('重点类型') != '特殊日期'
                    and r.get('交易金额') == tx_amount
                    for r in rows
                )
                if already_covered:
                    continue

                rows.append({
                    '本方姓名': person,
                    '交易日期': str(item.交易日期) if item.交易日期 else '',
                    '交易方向': tx_direction,
                    '交易金额': tx_amount,
                    '对方姓名': counterparty,
                    '重点类型': '特殊日期',
                    '重点子类': item.特殊日期名称,
                    '特征描述': f"节假日交易",
                    '置信度': '',
                    '数据来源': platform,
                })

        # 排序：按重点类型排序（工作收入>资产>规避阈值>大额>特殊金额>特殊日期）
        type_order = {
            '工作收入': 0, '资产收入': 1, '资产支出': 2,
            '规避阈值': 3, '大额收入': 4, '大额支出': 5,
            '特殊金额': 6, '特殊日期': 7,
        }
        rows.sort(key=lambda x: (
            x.get('本方姓名', ''),
            type_order.get(x.get('重点类型', ''), 9),
            -abs(x.get('交易金额', 0)),
        ))

        return rows
