#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import logging
from typing import Dict, List, Optional
from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

class WordExporter:
    def __init__(self, output_dir: str = 'output', config=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir
        self.config = config
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def generate_comprehensive_report(self, report_title: str, data_models: Dict, analyzers: Dict):
        """
        生成统一的、以人为核心的综合分析报告。
        """
        self.data_models = data_models
        
        doc = Document()
        doc.add_heading(report_title, level=1).alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        persons_with_financials = self._get_persons_with_financial_data(data_models)
        
        if not persons_with_financials:
            doc.add_paragraph("在所有数据中未能识别出任何持有金融账户（银行、微信、支付宝）的分析对象，无法生成个人详细报告。")
            # 即使没有金融账户，仍然可以尝试生成综合交叉分析
            if analyzers.get('comprehensive'):
                self.generate_comprehensive_cross_analysis_section(doc, analyzers)
            self._save_document(doc, report_title)
            return

        # 1. 基本信息 (全局)
        self.generate_global_basic_info(doc, persons_with_financials, data_models)

        # 2. 个人详细分析
        doc.add_heading('二、个人详细分析', level=2)
        doc.add_paragraph("本章节仅针对持有金融账户的个人进行详细分析。")
        for i, person_name in enumerate(persons_with_financials):
            doc.add_heading(f'（{self._to_chinese_numeral(i + 1)}）{person_name}的综合分析', level=3)
            
            # 生成各类型的详细分析内容
            if analyzers.get('bank'):
                self.generate_person_bank_analysis(doc, person_name, analyzers['bank'])
            if analyzers.get('wechat'):
                self.generate_person_payment_analysis(doc, person_name, analyzers['wechat'], '微信')
            if analyzers.get('alipay'):
                self.generate_person_payment_analysis(doc, person_name, analyzers['alipay'], '支付宝')
            if analyzers.get('call'):
                self.generate_person_call_analysis(doc, person_name, analyzers['call'])

        # 3. 综合交叉分析
        if analyzers.get('comprehensive'):
            self.generate_comprehensive_cross_analysis_section(doc, analyzers)

        self._save_document(doc, report_title)

    def _collect_person_summary_data(self, person_name: str, analyzers: Dict) -> Dict:
        """收集个人总结所需的数据"""
        summary_data = {
            'person_name': person_name,
            'bank_data': {},
            'payment_data': {},
            'call_data': {},
            'advanced_analysis': {}
        }

        # 银行数据分析
        if analyzers.get('bank'):
            bank_analyzer = analyzers['bank']
            person_data = bank_analyzer.bank_model.get_data_by_person(person_name)
            if not person_data.empty:
                # 基础统计
                total_income = person_data[person_data['收入金额'] > 0]['收入金额'].sum()
                total_expense = person_data[person_data['支出金额'] > 0]['支出金额'].sum()
                transaction_count = len(person_data)

                # 存取现分析
                cash_data = person_data[person_data['存取现标识'].isin(['存现', '取现'])]
                deposit_data = person_data[person_data['存取现标识'] == '存现']
                withdraw_data = person_data[person_data['存取现标识'] == '取现']

                # 时间跨度
                date_range = person_data[bank_analyzer.bank_model.date_column].agg(['min', 'max'])
                time_span_days = (date_range['max'] - date_range['min']).days

                summary_data['bank_data'] = {
                    'total_income': total_income,
                    'total_expense': total_expense,
                    'net_flow': total_income - total_expense,
                    'transaction_count': transaction_count,
                    'time_span_days': time_span_days,
                    'cash_transaction_count': len(cash_data),
                    'deposit_count': len(deposit_data),
                    'withdraw_count': len(withdraw_data),
                    'deposit_amount': deposit_data['收入金额'].sum() if not deposit_data.empty else 0,
                    'withdraw_amount': withdraw_data['支出金额'].sum() if not withdraw_data.empty else 0,
                    'avg_transaction_amount': person_data['交易金额'].abs().mean(),
                    'max_transaction_amount': person_data['交易金额'].abs().max(),
                    'date_range': date_range
                }

                # 高级分析数据
                try:
                    # 时间模式分析
                    time_patterns = bank_analyzer.advanced_analysis_engine.analyze_time_patterns(
                        person_data, bank_analyzer.bank_model.date_column, '交易时间'
                    )

                    # 金额模式分析
                    amount_patterns = bank_analyzer.advanced_analysis_engine.analyze_amount_patterns(
                        person_data, '交易金额'
                    )

                    # 异常检测
                    anomalies = bank_analyzer.advanced_analysis_engine.detect_anomalies(
                        person_data, '本方姓名', '交易金额', bank_analyzer.bank_model.date_column, '交易时间'
                    )

                    # 交易模式识别
                    transaction_patterns = bank_analyzer.advanced_analysis_engine.analyze_transaction_patterns(
                        person_data, '本方姓名', '交易金额', bank_analyzer.bank_model.date_column
                    )

                    summary_data['advanced_analysis'] = {
                        'time_patterns': time_patterns,
                        'amount_patterns': amount_patterns,
                        'anomalies': anomalies,
                        'transaction_patterns': transaction_patterns
                    }
                except Exception as e:
                    self.logger.warning(f"高级分析数据收集失败: {e}")

        # 通话数据分析
        if analyzers.get('call'):
            call_analyzer = analyzers['call']
            person_data = call_analyzer.call_model.get_data_by_person(person_name)
            if not person_data.empty:
                total_calls = len(person_data)
                total_duration = person_data[call_analyzer.call_model.duration_column].sum()
                unique_contacts = person_data[call_analyzer.call_model.opposite_phone_column].nunique()

                summary_data['call_data'] = {
                    'total_calls': total_calls,
                    'total_duration_minutes': total_duration / 60,
                    'unique_contacts': unique_contacts,
                    'avg_call_duration': total_duration / total_calls if total_calls > 0 else 0
                }

        return summary_data

    def _generate_summary_paragraphs(self, person_name: str, summary_data: Dict) -> List[str]:
        """生成总结段落"""
        paragraphs = []

        # 基础交易概况
        bank_data = summary_data.get('bank_data', {})
        if bank_data:
            # 交易概况
            transaction_count = bank_data.get('transaction_count', 0)
            time_span_days = bank_data.get('time_span_days', 0)
            total_income = bank_data.get('total_income', 0)
            total_expense = bank_data.get('total_expense', 0)
            net_flow = bank_data.get('net_flow', 0)

            if transaction_count > 0:
                paragraphs.append(
                    f"【交易概况】{person_name}在{time_span_days}天内共发生{transaction_count}笔银行交易，"
                    f"总收入{total_income:,.2f}元，总支出{total_expense:,.2f}元，"
                    f"净流入{net_flow:,.2f}元。"
                )

        # 交易行为特征分析
        advanced_data = summary_data.get('advanced_analysis', {})
        if advanced_data:
            behavior_insights = self._analyze_behavior_patterns(advanced_data, bank_data)
            if behavior_insights:
                paragraphs.append(f"【行为特征】{behavior_insights}")

        # 存取现行为分析
        if bank_data:
            cash_insights = self._analyze_cash_behavior(bank_data)
            if cash_insights:
                paragraphs.append(f"【存取现行为】{cash_insights}")

        # 异常行为提醒
        if advanced_data.get('anomalies', {}).get('anomalies'):
            anomaly_insights = self._analyze_anomalies(advanced_data['anomalies'])
            if anomaly_insights:
                paragraphs.append(f"【异常提醒】{anomaly_insights}")

        # 规律性分析
        if advanced_data:
            pattern_insights = self._analyze_regular_patterns(advanced_data, bank_data)
            if pattern_insights:
                paragraphs.append(f"【规律性分析】{pattern_insights}")

        # 通话行为分析
        call_data = summary_data.get('call_data', {})
        if call_data:
            call_insights = self._analyze_call_behavior(call_data)
            if call_insights:
                paragraphs.append(f"【通话行为】{call_insights}")

        return paragraphs

    def _analyze_behavior_patterns(self, advanced_data: Dict, bank_data: Dict) -> str:
        """分析行为模式"""
        insights = []

        # 时间偏好分析
        time_patterns = advanced_data.get('time_patterns', {})
        if time_patterns:
            weekday_dist = time_patterns.get('weekday_distribution', {})
            working_hours = time_patterns.get('working_hours_analysis', {})

            if weekday_dist:
                workday_ratio = weekday_dist.get('工作日占比', 0)
                if workday_ratio > 0.8:
                    insights.append("偏好工作日交易")
                elif workday_ratio < 0.3:
                    insights.append("偏好周末交易")

            if working_hours:
                work_time_ratio = working_hours.get('工作时间占比', 0)
                if work_time_ratio > 0.8:
                    insights.append("主要在工作时间进行交易")
                elif work_time_ratio < 0.3:
                    insights.append("主要在非工作时间进行交易")

        # 金额偏好分析
        amount_patterns = advanced_data.get('amount_patterns', {})
        if amount_patterns:
            round_analysis = amount_patterns.get('round_number_analysis', {})
            if round_analysis:
                round_ratio = round_analysis.get('整百金额占比', 0)
                if round_ratio > 0.7:
                    insights.append("强烈偏好整数金额")
                elif round_ratio > 0.4:
                    insights.append("较偏好整数金额")

        # 交易模式分析
        transaction_patterns = advanced_data.get('transaction_patterns', {})
        if transaction_patterns:
            person_patterns = transaction_patterns.get('person_patterns', {})
            if person_patterns:
                for person, pattern in person_patterns.items():
                    if pattern.get('是否有规律时间间隔', False):
                        interval = pattern.get('最常见时间间隔', 0)
                        if interval == 30:
                            insights.append("疑似每月固定交易（如工资、房租）")
                        elif interval == 7:
                            insights.append("疑似每周固定交易")
                        elif interval > 0:
                            insights.append(f"存在{interval}天的规律性交易间隔")

        return "；".join(insights) + "。" if insights else ""

    def _analyze_cash_behavior(self, bank_data: Dict) -> str:
        """分析存取现行为"""
        insights = []

        cash_count = bank_data.get('cash_transaction_count', 0)
        deposit_count = bank_data.get('deposit_count', 0)
        withdraw_count = bank_data.get('withdraw_count', 0)
        deposit_amount = bank_data.get('deposit_amount', 0)
        withdraw_amount = bank_data.get('withdraw_amount', 0)
        total_count = bank_data.get('transaction_count', 0)

        if cash_count > 0:
            cash_ratio = cash_count / total_count if total_count > 0 else 0
            if cash_ratio > 0.5:
                insights.append("频繁进行存取现操作")
            elif cash_ratio > 0.2:
                insights.append("较常进行存取现操作")

            if deposit_count > withdraw_count:
                insights.append("存现次数多于取现")
            elif withdraw_count > deposit_count:
                insights.append("取现次数多于存现")

            if deposit_amount > withdraw_amount * 2:
                insights.append("存现金额显著大于取现金额")
            elif withdraw_amount > deposit_amount * 2:
                insights.append("取现金额显著大于存现金额")
        else:
            insights.append("无存取现交易记录")

        return "；".join(insights) + "。" if insights else ""

    def _analyze_anomalies(self, anomalies: Dict) -> str:
        """分析异常情况"""
        insights = []

        anomaly_list = anomalies.get('anomalies', [])
        for anomaly in anomaly_list:
            anomaly_type = anomaly.get('type', '')
            if anomaly_type == '高频交易':
                count = anomaly.get('count', 0)
                insights.append(f"存在高频交易异常（{count}次）")
            elif anomaly_type == '金额异常':
                amounts = anomaly.get('outlier_amounts', [])
                if amounts:
                    max_amount = max(amounts)
                    insights.append(f"存在异常大额交易（{max_amount:,.0f}元）")
            elif anomaly_type == '时间间隔异常':
                insights.append("存在短时间连续交易")

        return "；".join(insights) + "。" if insights else ""

    def _analyze_regular_patterns(self, advanced_data: Dict, bank_data: Dict) -> str:
        """分析规律性模式"""
        insights = []

        avg_amount = bank_data.get('avg_transaction_amount', 0)

        # 推测可能的固定支出
        if avg_amount > 0:
            if 2000 <= avg_amount <= 8000:
                insights.append("平均交易金额符合工资水平特征")
            elif 1000 <= avg_amount <= 5000:
                insights.append("平均交易金额符合房租或贷款特征")
            elif avg_amount < 500:
                insights.append("以小额日常消费为主")
            elif avg_amount > 20000:
                insights.append("以大额交易为主")

        # 分析金额分布
        amount_patterns = advanced_data.get('amount_patterns', {})
        if amount_patterns:
            ranges = amount_patterns.get('amount_ranges', {})
            if ranges:
                small_ratio = ranges.get('小额', {}).get('占比', 0)
                large_ratio = ranges.get('大额', {}).get('占比', 0)

                if small_ratio > 0.7:
                    insights.append("主要为日常小额消费")
                elif large_ratio > 0.3:
                    insights.append("存在较多大额交易")

        return "；".join(insights) + "。" if insights else ""

    def _analyze_call_behavior(self, call_data: Dict) -> str:
        """分析通话行为"""
        insights = []

        total_calls = call_data.get('total_calls', 0)
        unique_contacts = call_data.get('unique_contacts', 0)
        avg_duration = call_data.get('avg_call_duration', 0)

        if total_calls > 0:
            if total_calls > 1000:
                insights.append("通话频率极高")
            elif total_calls > 500:
                insights.append("通话频率较高")
            elif total_calls < 50:
                insights.append("通话频率较低")

            if unique_contacts > 0:
                contact_ratio = total_calls / unique_contacts
                if contact_ratio > 10:
                    insights.append("与少数人频繁通话")
                elif contact_ratio < 2:
                    insights.append("联系人分布较广泛")

            if avg_duration > 300:  # 5分钟
                insights.append("通话时长较长")
            elif avg_duration < 60:  # 1分钟
                insights.append("通话时长较短")

        return "；".join(insights) + "。" if insights else ""

    def _save_document(self, doc: Document, title: str) -> Optional[str]:
        try:
            safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename = f"{safe_title}.docx"
            filepath = os.path.join(self.output_dir, filename)
            doc.save(filepath)
            self.logger.info(f"分析报告已导出到 {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"保存Word报告时出错: {e}")
            return None

    def _get_persons_with_financial_data(self, data_models: Dict) -> List[str]:
        """获取所有金融数据源中的所有不重复的本方姓名"""
        financial_persons = set()
        financial_data_types = ['bank', 'wechat', 'alipay']
        for data_type in financial_data_types:
            model = data_models.get(data_type)
            if model and not model.data.empty and model.name_column in model.data.columns:
                financial_persons.update(model.data[model.name_column].dropna().unique().tolist())
        return sorted(list(financial_persons))

    def _get_all_persons(self, data_models: Dict) -> List[str]:
        """获取所有数据源中的所有不重复的本方姓名"""
        all_persons = set()
        for model in data_models.values():
            if model and not model.data.empty and model.name_column in model.data.columns:
                all_persons.update(model.data[model.name_column].dropna().unique().tolist())
        return sorted(list(all_persons))

    def generate_global_basic_info(self, doc: Document, all_persons: List[str], data_models: Dict):
        doc.add_heading('一、基本信息', level=2)
        
        doc.add_paragraph(f"本报告共包含 {len(all_persons)} 个被分析对象，包括: {', '.join(all_persons)}。")
        
        used_data_types = [model_name.capitalize() for model_name, model in data_models.items() if model and not model.data.empty]
        doc.add_paragraph(f"本次分析使用了以下类型的数据: {', '.join(used_data_types)}。")

        bank_names = set()
        if data_models.get('bank') and not data_models['bank'].data.empty:
            bank_model = data_models['bank']
            if bank_model.bank_name_column in bank_model.data.columns:
                bank_names.update(bank_model.data[bank_model.bank_name_column].dropna().unique())
        doc.add_paragraph(f"所涉银行名称: {', '.join(sorted(list(bank_names))) if bank_names else '无'}")



    def generate_person_bank_analysis(self, doc: Document, person_name: str, analyzer):
        doc.add_heading('银行资金分析', level=4)
        person_data = analyzer.bank_model.get_data_by_person(person_name)
        if person_data.empty:
            doc.add_paragraph(f"未找到 {person_name} 的银行数据。")
            return

        section_num = 1
        has_content = False

        # 转账分析
        transfer_freq_df = analyzer.analyze_frequency(person_data)
        if not transfer_freq_df.empty:
            self._generate_frequency_summary_paragraph(doc, person_name, transfer_freq_df, person_data, "银行转账", analyzer.bank_model, section_num)
            section_num += 1
            self._add_top_opponent_tables(doc, transfer_freq_df)
            has_content = True
        
        # 存取现分析
        cash_ops_df = analyzer.analyze_cash_operations(person_data)
        if not cash_ops_df.empty:
            deposit_df = cash_ops_df[cash_ops_df['存取现标识'] == '存现']
            if not deposit_df.empty:
                 self._generate_cash_summary_paragraph(doc, person_name, deposit_df, '存现', analyzer.bank_model, section_num)
                 section_num += 1
                 has_content = True
            
            withdraw_df = cash_ops_df[cash_ops_df['存取现标识'] == '取现']
            if not withdraw_df.empty:
                 self._generate_cash_summary_paragraph(doc, person_name, withdraw_df, '取现', analyzer.bank_model, section_num)
                 section_num += 1
                 has_content = True
        
        # 特殊分析
        # Pass a copy of person_data to avoid side effects if the method modifies it
        special_amounts_df = analyzer.analyze_special_amounts(person_data.copy())
        special_dates_df = analyzer.analyze_special_dates(person_data.copy())
        if not special_amounts_df.empty or not special_dates_df.empty:
            self._generate_special_analysis_summary(doc, person_name, analyzer, special_amounts_df, special_dates_df, "银行", section_num)
            section_num += 1
            has_content = True

        # 整数金额分析
        integer_amounts_df = analyzer.analyze_integer_amounts(person_data.copy())
        if not integer_amounts_df.empty:
            self._generate_integer_amount_summary(doc, person_name, analyzer, integer_amounts_df, "银行", section_num)
            section_num += 1
            has_content = True

        # 重点收支分析
        self._generate_key_transactions_summary(doc, person_name, analyzer, person_data, section_num)
        section_num += 1
        has_content = True
        
        if not has_content:
             doc.add_paragraph(f"未找到 {person_name} 的有效银行交易数据。")

    def generate_person_payment_analysis(self, doc: Document, person_name: str, analyzer, payment_type: str):
        doc.add_heading(f'{payment_type}资金分析', level=4)
        person_data = analyzer.data_model.get_data_by_person(person_name)
        if person_data.empty:
            doc.add_paragraph(f"未找到 {person_name} 的{payment_type}数据。")
            return
        
        section_num = 1
        has_content = False

        freq_df = analyzer.analyze_frequency(person_data)
        if not freq_df.empty:
            self._generate_frequency_summary_paragraph(doc, person_name, freq_df, person_data, f"{payment_type}交易", analyzer.data_model, section_num)
            section_num += 1
            self._add_top_opponent_tables(doc, freq_df)
            has_content = True
        
        # 特殊分析
        special_amounts_df = analyzer.analyze_special_amounts(person_data.copy())
        special_dates_df = analyzer.analyze_special_dates(person_data.copy())
        if not special_amounts_df.empty or not special_dates_df.empty:
            self._generate_special_analysis_summary(doc, person_name, analyzer, special_amounts_df, special_dates_df, payment_type, section_num)
            section_num += 1
            has_content = True

        # 整数金额分析
        platform_type = 'wechat' if payment_type == '微信' else 'alipay'
        integer_amounts_df = analyzer.analyze_integer_amounts(person_data.copy(), platform_type)
        if not integer_amounts_df.empty:
            self._generate_integer_amount_summary(doc, person_name, analyzer, integer_amounts_df, payment_type, section_num)
            section_num += 1
            has_content = True

        # 重点收支分析
        self._generate_payment_key_transactions_summary(doc, person_name, analyzer, payment_type, section_num)
        has_content = True

        if not has_content:
            doc.add_paragraph(f"未找到 {person_name} 的{payment_type}交易数据。")

    def generate_person_call_analysis(self, doc: Document, person_name: str, analyzer):
        doc.add_heading('话单通联分析', level=4)
        person_data = analyzer.call_model.get_data_by_person(person_name)
        if person_data.empty:
            doc.add_paragraph(f"未找到 {person_name} 的话单数据。")
            return

        freq_df = analyzer.analyze_call_frequency(person_data)
        if not freq_df.empty:
            self._generate_call_summary_paragraph(doc, person_name, freq_df, person_data)
            
            # 过滤掉对方姓名为"未知"的记录
            known_contacts = freq_df[~freq_df['对方姓名'].isin(['未知', 'N/A', '']) & ~freq_df['对方姓名'].isna()]
            
            # 如果没有已知联系人，尝试使用其他数据
            if known_contacts.empty:
                # 使用所有数据，但后续会按号码等其他信息排序
                known_contacts = freq_df.copy()
                # 按通话次数排序
                top_contacts = known_contacts.nlargest(5, '通话次数')
            else:
                # 只使用已知联系人，按通话次数排序
                top_contacts = known_contacts.nlargest(5, '通话次数')
            
            # 如果有数据可显示
            if not top_contacts.empty:
                doc.add_paragraph(f"最密切联系人TOP {min(5, len(top_contacts))}:")
                
                # 创建一个新的DataFrame用于显示
                display_df = pd.DataFrame()
                
                # 复制基本列
                display_df['对方姓名'] = top_contacts['对方姓名']
                display_df['对方号码'] = top_contacts['对方号码']
                display_df['通话次数'] = top_contacts['通话次数']
                display_df['通话总时长(分钟)'] = top_contacts['通话总时长(分钟)']
                
                # 处理对方单位列
                # 首先检查是否有带后缀的对方单位列（来自话单数据）
                unit_col = next((col for col in top_contacts.columns if '对方单位名称_' in col), None)
                if unit_col:
                    display_df['对方单位'] = top_contacts[unit_col]
                elif '对方单位' in top_contacts.columns:
                    # 如果有多个单位（用|分隔），只取第一个
                    display_df['对方单位'] = top_contacts['对方单位'].apply(
                        lambda x: x.split('|')[0] if pd.notna(x) and '|' in str(x) else x
                    )
                else:
                    display_df['对方单位'] = None
                
                # 填充空值
                display_df['对方单位'] = display_df['对方单位'].fillna('N/A')
                
                # 定义最终显示列的顺序
                display_columns = [
                    '对方姓名', '对方单位', '对方号码', 
                    '通话次数', '通话总时长(分钟)'
                ]
                
                # 添加到文档
                self._add_df_to_doc(doc, display_df[display_columns])
            else:
                doc.add_paragraph("没有找到有效的联系人信息。")
        else:
            doc.add_paragraph(f"未找到 {person_name} 的通话频率数据。")

    def _generate_call_summary_paragraph(self, doc: Document, person_name: str, freq_df: pd.DataFrame, person_data: pd.DataFrame):
        """生成话单分析的概要段落"""
        total_contacts = freq_df['对方号码'].nunique()
        total_calls = freq_df['通话次数'].sum()
        total_duration_min = freq_df['通话总时长(分钟)'].sum()
        
        start_date = person_data[person_data.columns[person_data.columns.str.contains('日期')][0]].min()
        end_date = person_data[person_data.columns[person_data.columns.str.contains('日期')][0]].max()
        time_span_str = f"{start_date.strftime('%Y-%m-%d')}至{end_date.strftime('%Y-%m-%d')}"
        
        main_outgoing = person_data.get('主叫次数', pd.Series(0)).sum()
        main_incoming = person_data.get('被叫次数', pd.Series(0)).sum()

        summary = (
            f"在 {time_span_str} 期间，{person_name} 共通话 {total_calls} 次，总时长 {total_duration_min:,.2f} 分钟，联系了 {total_contacts} 个不同对象。"
            f"其中主叫 {main_outgoing} 次，被叫 {main_incoming} 次。"
        )
        doc.add_paragraph(summary)

    def _generate_frequency_summary_paragraph(self, doc: Document, person_name: str, frequency_df: pd.DataFrame, person_data: pd.DataFrame, analysis_type_str: str, data_model, section_num: int):
        total_income = frequency_df['总收入'].sum()
        total_expense = frequency_df['总支出'].abs().sum()
        net_flow = total_income - total_expense
        total_transactions_count = frequency_df['交易次数'].sum()

        start_date = person_data[data_model.date_column].min()
        end_date = person_data[data_model.date_column].max()
        time_span_str = f"{start_date.strftime('%Y-%m-%d')}至{end_date.strftime('%Y-%m-%d')}"

        # 计算主要时间集中
        person_data = person_data.copy()
        person_data['年份月份'] = person_data[data_model.date_column].dt.strftime('%Y年%m月')
        monthly_counts = person_data.groupby('年份月份').size().reset_index(name='次数')
        top_months = monthly_counts.nlargest(3, '次数')
        major_time_str = ", ".join([f"{row['年份月份']} ({row['次数']}次)" for _, row in top_months.iterrows()])

        # 单笔主要金额，添加对方姓名，避免重复对象
        all_amounts = person_data.sort_values(by=data_model.amount_column, ascending=False)
        top_amounts_with_names = []

        # 跟踪已添加的对象，避免重复
        added_opponents = set()

        # 获取前三笔金额，优先选择不同对手方
        for _, row in all_amounts.iterrows():
            amount = row[data_model.amount_column]
            opponent = row[data_model.opposite_name_column]
            opponent_str = str(opponent) if pd.notna(opponent) else "未知"

            # 如果这个对手方还没有被添加过，或者已经有3个不同的对手方了
            if opponent_str not in added_opponents and len(top_amounts_with_names) < 3:
                formatted_amount = f"{amount:.2f}"
                top_amounts_with_names.append(f"{formatted_amount}元（{opponent_str}）")
                added_opponents.add(opponent_str)
        
        top_single_amounts = "、".join(top_amounts_with_names)

        # 重复最多的金额（前三名）
        amount_counts = person_data[data_model.amount_column].value_counts()
        top_frequent_amounts = []
        for i in range(min(3, len(amount_counts))):
            amount = amount_counts.index[i]
            count = amount_counts.iloc[i]
            top_frequent_amounts.append(f"{amount:.2f}元 ({count}次)")
        most_frequent_amount_info = "、".join(top_frequent_amounts)

        # 生成概览段落，使用实际的换行而不是\n
        p = doc.add_paragraph()
        p.add_run(f"{section_num}、{analysis_type_str}概览").bold = True
        p.add_run(f"：在 {time_span_str} 期间，总收入 {total_income:.2f} 元，总支出 {total_expense:.2f} 元，")
        p.add_run(f"净流水 {net_flow:.2f} 元，共计 {total_transactions_count} 笔交易。")
        p.add_run(f"主要时间集中在：{major_time_str}。")
        p.add_run(f'单笔主要金额：{top_single_amounts}。')

        # 检查是否有大额金额需要加粗显示
        has_large_amount = any(abs(amount_counts.index[i]) > 5000 for i in range(min(3, len(amount_counts))))

        if has_large_amount:
            p.add_run('重复最多的金额为 ')
            # 对大额金额加粗显示
            for i, amount_info in enumerate(top_frequent_amounts):
                amount = amount_counts.index[i]
                if abs(amount) > 5000:
                    p.add_run(amount_info).bold = True
                else:
                    p.add_run(amount_info)
                if i < len(top_frequent_amounts) - 1:
                    p.add_run('、')
            p.add_run('。')
        else:
            p.add_run(f'重复最多的金额为 {most_frequent_amount_info}。')

    def _generate_cash_summary_paragraph(self, doc: Document, person_name: str, cash_df: pd.DataFrame, cash_type: str, bank_model, section_num: int):
        """生成存取现分析的概要段落"""
        if cash_df.empty:
            return

        total_amount = cash_df['总金额'].sum()
        total_count = cash_df['交易次数'].sum()
        
        p = doc.add_paragraph()
        p.add_run(f"{section_num}、{cash_type}概览").bold = True
        p.add_run(f"：总{cash_type}{total_count}笔，总金额")

        amount_run = p.add_run(f"{total_amount:.2f}元")
        if cash_type == '存现':
            amount_run.bold = True
            if total_amount >= 1_000_000:
                amount_run.underline = True
        p.add_run("。")
        
        # 查找相关的原始交易数据
        person_cash_data = bank_model.data[
            (bank_model.data[bank_model.name_column] == person_name) & 
            (bank_model.data['存取现标识'] == cash_type)
        ].copy()

        if person_cash_data.empty:
            return

        # 仅对"存现"进行详细分析和生成表格
        if cash_type == '存现':
            # 计算主要时间集中
            person_cash_data['年份月份'] = person_cash_data[bank_model.date_column].dt.strftime('%Y年%m月')
            monthly_counts = person_cash_data.groupby('年份月份').size().reset_index(name='次数')
            top_months = monthly_counts.nlargest(3, '次数')
            major_time_str = ", ".join([f"{row['年份月份']} ({row['次数']}次)" for _, row in top_months.iterrows()])
            p.add_run(f"主要时间集中在：{major_time_str}。")

            # 单笔主要金额（重复最多的金额前三名）
            amount_counts = person_cash_data[bank_model.amount_column].abs().value_counts()
            if not amount_counts.empty:
                # 在同一段落中添加
                p.add_run("单笔主要金额：")

                # 获取前三名重复最多的金额
                top_frequent_amounts = []
                for i in range(min(3, len(amount_counts))):
                    amount = amount_counts.index[i]
                    count = amount_counts.iloc[i]
                    amount_text = f"{amount:.2f}元 ({count}次)"

                    # 如果金额大于等于10000元，加粗显示
                    if amount >= 10000:
                        amount_run = p.add_run(amount_text)
                        amount_run.bold = True
                    else:
                        p.add_run(amount_text)

                    if i < min(3, len(amount_counts)) - 1:
                        p.add_run("、")

                p.add_run("。")

            # 单笔金额前五名
            doc.add_paragraph(f"单笔{cash_type}金额前五名：")
            top_5_transactions = person_cash_data.nlargest(5, bank_model.amount_column, keep='first')
            
            if not top_5_transactions.empty:
                table = doc.add_table(rows=1, cols=4, style='Table Grid')
                hdr_cells = table.rows[0].cells
                headers = ['交易日期', '交易金额', '对方名称', '交易摘要']
                for i, header in enumerate(headers):
                    hdr_cells[i].text = header

                for _, row in top_5_transactions.iterrows():
                    row_cells = table.add_row().cells
                    row_cells[0].text = row[bank_model.date_column].strftime('%Y-%m-%d')
                    
                    amount = row[bank_model.amount_column]
                    amount_p = row_cells[1].paragraphs[0]
                    amount_p.text = ''  # 清空单元格默认段落
                    run = amount_p.add_run(f"{amount:.2f}")
                    # 所有大于等于10000元的金额都加粗
                    if amount >= 10000:
                        run.bold = True
                    
                    opponent_name = row[bank_model.opposite_name_column]
                    row_cells[2].text = str(opponent_name) if pd.notna(opponent_name) else 'N/A'
                    
                    summary = row[bank_model.summary_column]
                    row_cells[3].text = str(summary) if pd.notna(summary) else 'N/A'
        else:
            # 对于"取现"，只显示单笔最高金额，不换行
            top_transaction = person_cash_data.loc[person_cash_data[bank_model.amount_column].abs().idxmax()]
            top_amount = top_transaction[bank_model.amount_column]
            top_date = top_transaction[bank_model.date_column].strftime('%Y年%m月%d日')
            p.add_run(f"其中，单笔最高{cash_type}金额为 {abs(top_amount):.2f}元，发生于{top_date}。")

    def _generate_special_analysis_summary(self, doc: Document, person_name: str, analyzer, special_amounts_df: pd.DataFrame, special_dates_df: pd.DataFrame, analysis_type_str: str, section_num: int):
        """生成特殊分析的概要段落"""
        p = doc.add_paragraph()
        p.add_run(f"{section_num}、特殊分析：").bold = True

        # (1) 特殊金额
        if not special_amounts_df.empty:
            p.add_run("(1)特殊金额：")

            love_amounts = [520, 1314, 521]
            amount_col = analyzer.data_model.amount_column
            opponent_col = analyzer.data_model.opposite_name_column
            
            unique_amounts = special_amounts_df[amount_col].abs().unique()
            unique_amounts_str = "、".join([f"{amt:.2f}" for amt in unique_amounts])
            total_times = len(special_amounts_df)
            
            p.add_run(f"{person_name}发生{unique_amounts_str}等特殊金额{total_times}次，")
            
            # 收集特殊金额信息，按金额分组
            amount_groups = {}
            
            # 首先处理爱情数字，确保它们总是被列出
            for love_amount in love_amounts:
                love_txns = special_amounts_df[special_amounts_df[amount_col].abs() == love_amount]
                if not love_txns.empty:
                    # 收集该金额下所有不同的对手方和次数，不过滤名字长度
                    opponents_count = love_txns[opponent_col].value_counts()
                    opponents_list = []
                    for opponent, count in opponents_count.items():
                        if pd.notna(opponent):
                            opponents_list.append(f"{str(opponent)}，{count}次")
                        else:
                            opponents_list.append(f"未知，{count}次")

                    opponents_str = "、".join(opponents_list)
                    if not opponents_str:
                        opponents_str = "未知，1次"
                    amount_groups[love_amount] = opponents_str
            
            # 然后处理其他特殊金额，只显示前三个不同金额
            other_amounts = [amt for amt in unique_amounts if abs(amt) not in love_amounts]
            top_other_amounts = sorted(other_amounts, key=abs, reverse=True)[:3]
            
            for amount in top_other_amounts:
                abs_amount = abs(amount)
                # 收集该金额下所有不同的对手方和次数，但最多只显示3个
                amount_txns = special_amounts_df[special_amounts_df[amount_col].abs() == abs_amount]
                if not amount_txns.empty:
                    opponents_count = amount_txns[opponent_col].value_counts()
                    opponents_list = []
                    # 只取前3个对手方
                    for opponent, count in opponents_count.head(3).items():
                        if pd.notna(opponent):
                            opponents_list.append(f"{str(opponent)}，{count}次")
                        else:
                            opponents_list.append(f"未知，{count}次")

                    opponents_str = "、".join(opponents_list)
                    if not opponents_str:
                        opponents_str = "未知，1次"
                    amount_groups[abs_amount] = opponents_str
            
            # 构建显示字符串
            p.add_run("特殊金额包括：")
            
            # 先显示爱情数字
            love_items = []
            for amount in love_amounts:
                if amount in amount_groups:
                    love_run = p.add_run(f"{amount:.2f}元（{amount_groups[amount]}）")
                    love_run.bold = True
                    love_items.append(amount)
            
            # 再显示其他特殊金额（如果有）
            other_items = [amt for amt in amount_groups.keys() if amt not in love_items]
            
            # 如果既有爱情数字又有其他数字，添加分隔符
            if love_items and other_items:
                p.add_run("，以及")
            
            # 添加其他特殊金额
            for i, amount in enumerate(other_items):
                p.add_run(f"{amount:.2f}元（{amount_groups[amount]}）")
                if i < len(other_items) - 1:
                    p.add_run("、")
            
            p.add_run("。")
        else:
            p.add_run("(1)特殊金额：未发现特殊金额交易。")

        # (2) 特殊日期
        if not special_dates_df.empty:
            p.add_run("(2)特殊日期：")

            # 获取金额列
            amount_col = analyzer.data_model.amount_column if hasattr(analyzer, 'data_model') else analyzer.bank_model.amount_column

            # 按特殊日期名称分组，计算总额和次数
            date_stats = special_dates_df.groupby('特殊日期名称').agg({
                amount_col: ['sum', 'count']
            }).reset_index()

            # 重命名列
            date_stats.columns = ['特殊日期名称', '总额', '次数']
            date_stats['总额'] = date_stats['总额'].abs()  # 取绝对值

            # 按总额排序，取前三名
            top_dates = date_stats.nlargest(3, '总额')

            date_descriptions = []
            for _, row in top_dates.iterrows():
                date_name = row['特殊日期名称']
                total_amount = row['总额']
                count = row['次数']
                date_descriptions.append(f"{date_name}（总额{total_amount:.2f}元、{count}次）")

            if date_descriptions:
                p.add_run("、".join(date_descriptions) + "。")
            else:
                p.add_run("未发现特殊日期交易。")
        else:
            p.add_run("(2)特殊日期：未发现特殊日期交易。")

    def _generate_integer_amount_summary(self, doc: Document, person_name: str, analyzer, integer_amounts_df: pd.DataFrame, analysis_type_str: str, section_num: int):
        """生成整百数金额分析的概要段落"""
        if not integer_amounts_df.empty:
            p = doc.add_paragraph()
            p.add_run(f"{section_num}、整数金额：").bold = True

            amount_col = analyzer.data_model.amount_column if hasattr(analyzer, 'data_model') else analyzer.bank_model.amount_column

            # 统计整百数金额的出现次数
            amount_counts = integer_amounts_df[amount_col].abs().value_counts()

            # 获取前三名出现次数最多的整百数金额
            top_integer_amounts = []
            for i in range(min(3, len(amount_counts))):
                amount = amount_counts.index[i]
                count = amount_counts.iloc[i]
                top_integer_amounts.append(f"{amount:.0f}元 ({count}次)")

            total_times = len(integer_amounts_df)
            integer_amounts_str = "、".join(top_integer_amounts)

            p.add_run(f"{person_name}发生整百数金额交易{total_times}次，")
            p.add_run(f"出现次数最多的整百数金额为：{integer_amounts_str}。")

    def _generate_key_transactions_summary(self, doc: Document, person_name: str, analyzer, person_data: pd.DataFrame, section_num: int):
        """生成重点收支分析的概要段落"""
        try:
            # 导入重点收支识别引擎
            from src.utils.key_transactions import KeyTransactionEngine

            # 初始化重点收支识别引擎
            key_engine = KeyTransactionEngine(analyzer.config)

            if not key_engine.enabled:
                return

            # 识别重点收支
            key_data = key_engine.identify_key_transactions(
                person_data,
                analyzer.bank_model.summary_column,
                analyzer.bank_model.remark_column,
                analyzer.bank_model.type_column,
                analyzer.bank_model.amount_column,
                analyzer.bank_model.opposite_name_column
            )

            # 筛选出重点收支数据
            key_transactions = key_data[key_data['是否重点收支']].copy()

            if key_transactions.empty:
                return

            # 生成统计信息
            key_stats = key_engine.generate_statistics(
                key_data,
                analyzer.bank_model.name_column,
                analyzer.bank_model.amount_column,
                analyzer.bank_model.date_column,
                analyzer.bank_model.opposite_name_column
            )

            if key_stats.empty:
                return

            person_stats = key_stats[key_stats['姓名'] == person_name]
            if person_stats.empty:
                return

            stats = person_stats.iloc[0]

            # 检查是否有任何重点收支数据
            work_income_total = stats.get('工作收入总额', 0)
            work_income_count = stats.get('工作收入次数', 0)
            work_units_count = stats.get('可能工作单位数', 0)
            work_units = stats.get('可能工作单位', '')
            asset_income_total = stats.get('资产收入总额', 0)
            asset_expense_total = stats.get('资产支出总额', 0)
            large_income_total = stats.get('大额收入总额', 0)
            large_expense_total = stats.get('大额支出总额', 0)

            # 使用通用的重点收支内容生成方法
            self._generate_key_transactions_content(doc, person_name, stats, section_num, '银行')



        except Exception as e:
            # 如果出现错误，记录日志但不影响报告生成
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"生成重点收支分析时出错: {e}", exc_info=True)

    def generate_comprehensive_cross_analysis_section(self, doc: Document, analyzers: Dict):
        doc.add_heading('三、综合交叉分析', level=2)
        doc.add_paragraph("本章节旨在展示不同数据源之间关联度最高的对手方信息。")
        
        comprehensive_analyzer = analyzers.get('comprehensive')
        if not comprehensive_analyzer:
            return

        available_sources = [
            data_type for data_type, model in comprehensive_analyzer.data_models.items() 
            if model and not model.data.empty
        ]
        
        if len(available_sources) < 2:
            return

        all_results_df = pd.DataFrame()

        for base_source in available_sources:
            results = comprehensive_analyzer.analyze(base_source=base_source)
            result_key = f"综合分析_以{comprehensive_analyzer._get_chinese_data_type_name(base_source)}为基准"
            
            if results and result_key in results and not results[result_key].empty:
                all_results_df = pd.concat([all_results_df, results[result_key]], ignore_index=True)

        if all_results_df.empty:
            doc.add_paragraph("未生成有效的交叉分析结果。")
            return

        # 全局去重，保留关联数据源数量最多的记录
        count_cols = [col for col in all_results_df.columns if '次数' in col or '总额' in col]
        if not count_cols:
             # 如果没有可用于计数的列，则无法计算关联数据源，直接按姓名去重
            display_df = all_results_df.drop_duplicates(subset=['本方姓名', '对方姓名'])
        else:
            all_results_df['关联数据源数量'] = all_results_df[count_cols].notna().sum(axis=1)
            # 按"本方姓名"和"对方姓名"分组，保留每个分组中"关联数据源数量"最大的那一行
            deduplicated_df = all_results_df.loc[all_results_df.groupby(['本方姓名', '对方姓名'])['关联数据源数量'].idxmax()]

            # 在去重后的结果中，筛选出至少在两个不同类型数据中都出现过的对手方
            display_df = deduplicated_df[deduplicated_df['关联数据源数量'] > 1]

        # 先按关联数据源数量全局排序，取前10条关联度最高的记录
        if '关联数据源数量' in display_df.columns:
            display_df = display_df.sort_values(by='关联数据源数量', ascending=False).head(10)
            # 然后按本方姓名排序，确保相同本方姓名的记录聚集在一起
            display_df = display_df.sort_values(by=['本方姓名', '关联数据源数量'], ascending=[True, False])
        else:
            display_df = display_df.sort_values(by='本方姓名').head(10)

        if display_df.empty:
            doc.add_paragraph("未发现显著关联的对手方。")
            return
        
        # 创建一个新的DataFrame用于显示
        final_df = pd.DataFrame()
        
        # 复制基本列
        final_df['本方姓名'] = display_df['本方姓名']
        final_df['对方姓名'] = display_df['对方姓名']
        
        # 处理对方单位列
        # 首先检查是否有带后缀的对方单位列（来自话单数据）
        unit_col = next((col for col in display_df.columns if '对方单位名称_' in col), None)
        if unit_col:
            final_df['对方单位'] = display_df[unit_col]
        elif '对方单位' in display_df.columns:
            # 如果有多个单位（用|分隔），只取第一个
            final_df['对方单位'] = display_df['对方单位'].apply(
                lambda x: x.split('|')[0] if pd.notna(x) and '|' in str(x) else x
            )
        else:
            final_df['对方单位'] = None
        
        # 处理金额和次数列
        # 银行总额
        if '银行总额' in display_df.columns:
            final_df['银行总金额'] = display_df['银行总额']
        elif '交易总金额' in display_df.columns and base_source == 'bank':
            final_df['银行总金额'] = display_df['交易总金额']
        else:
            final_df['银行总金额'] = None
            
        # 微信总额
        if '微信总额' in display_df.columns:
            final_df['微信总金额'] = display_df['微信总额']
        elif '交易总金额' in display_df.columns and base_source == 'wechat':
            final_df['微信总金额'] = display_df['交易总金额']
        else:
            final_df['微信总金额'] = None
            
        # 支付宝总额
        if '支付宝总额' in display_df.columns:
            final_df['支付宝总金额'] = display_df['支付宝总额']
        elif '交易总金额' in display_df.columns and base_source == 'alipay':
            final_df['支付宝总金额'] = display_df['交易总金额']
        else:
            final_df['支付宝总金额'] = None
            
        # 通话次数
        if '通话次数' in display_df.columns:
            final_df['通话次数'] = display_df['通话次数']
        else:
            final_df['通话次数'] = None
        
        # 填充空值
        final_df = final_df.fillna('N/A')
        
        # 格式化数值列
        self._format_dataframe_numbers(final_df)
        
        # 定义最终显示列的顺序
        display_columns = [
            '本方姓名', '对方姓名', '对方单位',
            '银行总金额', '微信总金额', '支付宝总金额', '通话次数'
        ]
        
        # 直接显示表格，相同本方姓名的记录会连续显示
        self._add_df_to_doc(doc, final_df[display_columns])

    def _add_grouped_df_to_doc(self, doc: Document, df: pd.DataFrame, group_by: str):
        """
        按指定列分组显示DataFrame，使结果更清晰

        Parameters:
        -----------
        doc : Document
            Word文档对象
        df : pd.DataFrame
            要显示的数据框
        group_by : str
            分组列名
        """
        if df.empty:
            doc.add_paragraph("无数据可显示。")
            return

        # 按分组列分组
        grouped = df.groupby(group_by)

        for group_name, group_df in grouped:
            # 添加分组标题
            doc.add_paragraph(f"【{group_name}】", style='Heading 3')

            # 为该分组创建表格，不显示分组列（因为已经在标题中显示了）
            display_df = group_df.drop(columns=[group_by]).reset_index(drop=True)

            # 添加表格
            self._add_df_to_doc(doc, display_df)

            # 添加空行分隔
            doc.add_paragraph("")

    def _add_top_opponent_tables(self, doc: Document, frequency_df: pd.DataFrame):
        """为资金分析添加Top5对手方表格"""
        # 只保留有收入的记录
        income_df = frequency_df[frequency_df['总收入'] > 0].copy()
        if not income_df.empty:
            # 计算总收入占比
            income_df['收入占比'] = income_df['总收入'] / (income_df['总收入'] - income_df['总支出']) * 100
            # 筛选出收入占比为100%的记录，并严格保留前5名
            pure_income_df = income_df[income_df['收入占比'] == 100].nlargest(5, '总收入', keep='first')
            
            if not pure_income_df.empty:
                doc.add_paragraph("转入金额占比100%的金额前5名对手方如下：")
                
                # 准备显示列
                display_cols = ['对方姓名', '对方单位', '总收入', '交易次数']
                
                # 创建一个新的DataFrame用于显示
                display_df = pd.DataFrame()
                
                # 复制必需的列
                display_df['对方姓名'] = pure_income_df['对方姓名']
                display_df['总收入'] = pure_income_df['总收入'].apply(lambda x: f"{x:.2f}")
                display_df['交易次数'] = pure_income_df['交易次数']
                
                # 处理对方单位列
                # 首先检查是否有带后缀的对方单位列（来自话单数据）
                unit_col = next((col for col in pure_income_df.columns if '对方单位名称_' in col), None)
                if unit_col:
                    display_df['对方单位'] = pure_income_df[unit_col]
                elif '对方单位' in pure_income_df.columns:
                    # 如果有多个单位（用|分隔），只取第一个
                    display_df['对方单位'] = pure_income_df['对方单位'].apply(
                        lambda x: x.split('|')[0] if pd.notna(x) and '|' in str(x) else x
                    )
                else:
                    # 尝试从话单数据中获取对方单位信息
                    company_position_map = {}
                    if hasattr(self, 'data_models') and 'call' in self.data_models and self.data_models['call'] and not self.data_models['call'].data.empty:
                        call_data = self.data_models['call'].data
                        if '对方单位名称' in call_data.columns:
                            # 创建一个映射，用对方姓名作为键
                            company_map = call_data.groupby('对方姓名')['对方单位名称'].agg(
                                lambda x: '|'.join(sorted(set(x.dropna().astype(str))))
                            ).to_dict()
                            company_position_map.update(company_map)
                    
                    # 使用映射获取对方单位
                    display_df['对方单位'] = display_df['对方姓名'].map(company_position_map)
                
                # 填充空值
                display_df['对方单位'] = display_df['对方单位'].fillna('N/A')
                
                # 添加到文档
                self._add_df_to_doc(doc, display_df[display_cols])

    def _to_chinese_numeral(self, num: int) -> str:
        numerals = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九', '十']
        if 0 <= num < len(numerals):
            return numerals[num]
        return str(num)

    def _format_dataframe_numbers(self, df: pd.DataFrame):
        """格式化DataFrame中的数字列"""
        for col in df.columns:
            col_name_lower = col.lower()

            # 金额列保留2位小数
            if any(keyword in col for keyword in ['金额', '总额', '收入', '支出', '余额', '价格']):
                df[col] = df[col].apply(
                    lambda x: f"{float(x):.2f}" if self._is_numeric_value(x) else x
                )
            # 次数、序号等整数列
            elif any(keyword in col for keyword in ['次数', '序号', '数量', '笔数', '个数', '排名']):
                df[col] = df[col].apply(
                    lambda x: str(int(float(x))) if self._is_numeric_value(x) else x
                )
            # 电话号码、银行卡号等保持原样（文本格式）
            elif any(keyword in col for keyword in ['电话', '号码', '手机', '银行卡', '身份证', '卡号']):
                df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else x)

    def _is_numeric_value(self, x):
        """检查值是否为数字"""
        if pd.isna(x):
            return False
        if isinstance(x, (int, float)):
            return True
        if isinstance(x, str):
            try:
                float(x)
                return True
            except ValueError:
                return False
        return False

    def _add_df_to_doc(self, doc: Document, df: pd.DataFrame):
        if df.empty:
            doc.add_paragraph("无相关数据。")
            return

        # 格式化数值列
        df_copy = df.copy()
        self._format_dataframe_numbers(df_copy)

        df_copy = df_copy.fillna('N/A').astype(str)

        table = doc.add_table(rows=1, cols=len(df_copy.columns))
        table.style = 'Table Grid'

        # 设置表头
        for i, column in enumerate(df_copy.columns):
            table.cell(0, i).text = str(column)

        # 添加数据行
        for _, row in df_copy.iterrows():
            cells = table.add_row().cells
            for i, value in enumerate(row):
                cells[i].text = str(value)

    def _generate_payment_key_transactions_summary(self, doc: Document, person_name: str, analyzer, payment_type: str, section_num: int):
        """
        为微信/支付宝数据生成重点收支分析段落

        Parameters:
        -----------
        doc : Document
            Word文档对象
        person_name : str
            人员姓名
        analyzer : object
            分析器对象
        payment_type : str
            支付类型（微信/支付宝）
        section_num : int
            段落编号
        """
        try:
            # 导入重点收支识别引擎
            from src.utils.key_transactions import KeyTransactionEngine

            # 获取配置对象
            config = self.config if hasattr(self, 'config') and self.config else getattr(analyzer.data_model, 'config', None)

            # 初始化重点收支识别引擎
            key_engine = KeyTransactionEngine(config)

            if not key_engine.enabled:
                return

            # 获取该人员的数据
            person_data = analyzer.data_model.get_data_by_person(person_name)
            if person_data.empty:
                return

            # 确定数据类型
            data_type = 'wechat' if payment_type == '微信' else 'alipay'

            # 微信和支付宝没有摘要列，使用备注列作为匹配文本
            summary_column = None
            remark_column = getattr(analyzer.data_model, 'remark_column', None)
            type_column = getattr(analyzer.data_model, 'type_column', None)

            # 识别重点收支
            key_data = key_engine.identify_key_transactions(
                person_data,
                summary_column,
                remark_column,
                type_column,
                analyzer.data_model.amount_column,
                analyzer.data_model.opposite_name_column
            )

            # 生成统计数据
            key_stats = key_engine.generate_statistics(
                key_data,
                analyzer.data_model.name_column,
                analyzer.data_model.amount_column,
                analyzer.data_model.date_column,
                analyzer.data_model.opposite_name_column
            )

            if key_stats.empty:
                return

            # 获取该人员的统计数据
            # 重点收支统计数据中的列名是'姓名'，而不是原始数据中的列名
            person_stats = key_stats[key_stats['姓名'] == person_name]
            if person_stats.empty:
                return

            stats = person_stats.iloc[0].to_dict()

            # 生成重点收支段落
            self._generate_key_transactions_content(doc, person_name, stats, section_num, payment_type)

        except Exception as e:
            self.logger.error(f"生成{payment_type}重点收支分析时出错: {e}", exc_info=True)

    def _generate_key_transactions_content(self, doc: Document, person_name: str, stats: dict, section_num: int, data_type: str = '银行'):
        """
        生成重点收支内容段落

        Parameters:
        -----------
        doc : Document
            Word文档对象
        person_name : str
            人员姓名
        stats : dict
            统计数据字典
        section_num : int
            段落编号
        data_type : str
            数据类型（银行/微信/支付宝）
        """
        # 检查是否有任何重点收支数据
        work_income_total = stats.get('工作收入总额', 0)
        work_income_count = stats.get('工作收入次数', 0)
        work_units_count = stats.get('可能工作单位数', 0)
        work_units = stats.get('可能工作单位', '')
        asset_income_total = stats.get('资产收入总额', 0)
        asset_expense_total = stats.get('资产支出总额', 0)
        large_income_total = stats.get('大额收入总额', 0)
        large_expense_total = stats.get('大额支出总额', 0)

        # 如果没有任何重点收支，则不显示这个段落
        if (work_income_total == 0 and asset_income_total == 0 and asset_expense_total == 0 and
            large_income_total == 0 and large_expense_total == 0):
            return

        # 生成重点收支概要段落
        p = doc.add_paragraph()
        p.add_run(f"{section_num}、{data_type}重点收支：").bold = True

        # 工作收入部分 - 按照新格式：工作收入XX元（XX年XX月至XX年XX月，XX次，注意不需要具体日期和时间），疑似工作单位有X个（XX，XX，XX，最多列举前三个，如果对方姓名为空的，合并算一个）
        if work_income_total > 0:
            time_range = stats.get('时间范围', '')
            # 提取年月信息，去掉具体日期
            time_range_formatted = self._format_time_range_to_year_month(time_range)

            work_income_run = p.add_run("工作收入")
            work_income_run.underline = True
            p.add_run(f"{work_income_total:,.0f}元（{time_range_formatted}，{work_income_count}次）")

            # 工作单位信息
            if work_units_count > 0:
                p.add_run("，疑似工作单位有")
                units_run = p.add_run(f"{work_units_count}个")
                units_run.underline = True

                if work_units:
                    p.add_run(f"（{work_units}）")

            p.add_run("；")

        # 资产收入部分 - 按照新格式：资产收入XX元（XX年XX月至XX年XX月，疑似与房有关XX次XX元、与车有关XX次XX元、与租金有关XX次XX元、理财XX次XX元）
        if asset_income_total > 0:
            time_range = stats.get('时间范围', '')
            time_range_formatted = self._format_time_range_to_year_month(time_range)

            property_count = stats.get('房产收入次数', 0)
            property_amount = stats.get('房产收入金额', 0)
            rental_count = stats.get('租金收入次数', 0)
            rental_amount = stats.get('租金收入金额', 0)
            vehicle_income_count = stats.get('车辆收入次数', 0)
            vehicle_income_amount = stats.get('车辆收入金额', 0)
            securities_income_count = stats.get('证券收入次数', 0)
            securities_income_amount = stats.get('证券收入金额', 0)

            asset_income_run = p.add_run("资产收入")
            asset_income_run.underline = True
            p.add_run(f"{asset_income_total:,.0f}元（{time_range_formatted}，")

            asset_income_details = []
            # 房产相关
            if property_count > 0:
                asset_income_details.append(f"疑似与房有关{property_count}次{property_amount:,.0f}元")

            # 车辆收入相关
            if vehicle_income_count > 0:
                asset_income_details.append(f"与车有关{vehicle_income_count}次{vehicle_income_amount:,.0f}元")

            # 租金相关
            if rental_count > 0:
                asset_income_details.append(f"与租金有关{rental_count}次{rental_amount:,.0f}元")

            # 理财（证券）相关
            if securities_income_count > 0:
                asset_income_details.append(f"理财{securities_income_count}次{securities_income_amount:,.0f}元")

            if asset_income_details:
                p.add_run("、".join(asset_income_details))
            else:
                p.add_run("其他资产收入")

            p.add_run("）；")

        # 资产支出部分 - 按照新格式：资产支出XX元（XX年XX月至XX年XX月，疑似与房有关XX次XX元、与车有关XX次XX元、与租金有关XX次XX元、理财XX次XX元）
        if asset_expense_total > 0:
            time_range = stats.get('时间范围', '')
            time_range_formatted = self._format_time_range_to_year_month(time_range)

            property_expense_count = stats.get('房产支出次数', 0)
            property_expense_amount = stats.get('房产支出金额', 0)
            vehicle_expense_count = stats.get('车辆支出次数', 0)
            vehicle_expense_amount = stats.get('车辆支出金额', 0)
            rental_expense_count = stats.get('租金支出次数', 0)
            rental_expense_amount = stats.get('租金支出金额', 0)
            securities_expense_count = stats.get('证券支出次数', 0)
            securities_expense_amount = stats.get('证券支出金额', 0)

            asset_expense_run = p.add_run("资产支出")
            asset_expense_run.underline = True
            p.add_run(f"{asset_expense_total:,.0f}元（{time_range_formatted}，")

            asset_expense_details = []
            # 房产支出相关
            if property_expense_count > 0:
                asset_expense_details.append(f"疑似与房有关{property_expense_count}次{property_expense_amount:,.0f}元")

            # 车辆支出相关
            if vehicle_expense_count > 0:
                asset_expense_details.append(f"与车有关{vehicle_expense_count}次{vehicle_expense_amount:,.0f}元")

            # 租金支出相关
            if rental_expense_count > 0:
                asset_expense_details.append(f"与租金有关{rental_expense_count}次{rental_expense_amount:,.0f}元")

            # 理财（证券）支出相关
            if securities_expense_count > 0:
                asset_expense_details.append(f"理财{securities_expense_count}次{securities_expense_amount:,.0f}元")

            if asset_expense_details:
                p.add_run("、".join(asset_expense_details))
            else:
                p.add_run("其他资产支出")

            p.add_run("）；")

        # 大额收入部分 - 按照新格式：大额收入XX元（10万-50万XX次；50万-100万XX次、100万及以上的XX次）
        if large_income_total > 0:
            large_income_10_50 = stats.get('大额收入_10万-50万_次数', 0)
            large_income_50_100 = stats.get('大额收入_50万-100万_次数', 0)
            large_income_100_plus = stats.get('大额收入_100万及以上_次数', 0)

            large_income_run = p.add_run("大额收入")
            large_income_run.underline = True
            p.add_run(f"{large_income_total:,.0f}元（")

            income_details = []
            if large_income_10_50 > 0:
                income_details.append(f"10万-50万{large_income_10_50}次")
            if large_income_50_100 > 0:
                income_details.append(f"50万-100万{large_income_50_100}次")
            if large_income_100_plus > 0:
                income_details.append(f"100万及以上{large_income_100_plus}次")

            p.add_run("；".join(income_details))
            p.add_run("）；")

        # 大额支出部分 - 按照新格式：大额支出XX元（10万-50万XX次；50万-100万XX次、100万及以上的XX次）
        if large_expense_total > 0:
            large_expense_10_50 = stats.get('大额支出_10万-50万_次数', 0)
            large_expense_50_100 = stats.get('大额支出_50万-100万_次数', 0)
            large_expense_100_plus = stats.get('大额支出_100万及以上_次数', 0)

            large_expense_run = p.add_run("大额支出")
            large_expense_run.underline = True
            p.add_run(f"{large_expense_total:,.0f}元（")

            expense_details = []
            if large_expense_10_50 > 0:
                expense_details.append(f"10万-50万{large_expense_10_50}次")
            if large_expense_50_100 > 0:
                expense_details.append(f"50万-100万{large_expense_50_100}次")
            if large_expense_100_plus > 0:
                expense_details.append(f"100万及以上{large_expense_100_plus}次")

            p.add_run("；".join(expense_details))
            p.add_run("）。")
        else:
            # 如果没有大额支出，去掉最后一个分号，改为句号
            if p.runs and p.runs[-1].text.endswith("；"):
                p.runs[-1].text = p.runs[-1].text[:-1] + "。"

    def _format_time_range_to_year_month(self, time_range: str) -> str:
        """
        将时间范围格式化为年月格式，去掉具体日期

        Parameters:
        -----------
        time_range : str
            原始时间范围，格式如 "2023-01-15 至 2023-12-30"

        Returns:
        --------
        str
            格式化后的时间范围，格式如 "2023年01月至2023年12月"
        """
        try:
            if ' 至 ' in time_range:
                start_date, end_date = time_range.split(' 至 ')

                # 提取年月信息
                if '-' in start_date:
                    start_parts = start_date.split('-')
                    if len(start_parts) >= 2:
                        start_year, start_month = start_parts[0], start_parts[1]
                        start_formatted = f"{start_year}年{start_month}月"
                    else:
                        start_formatted = start_date
                else:
                    start_formatted = start_date

                if '-' in end_date:
                    end_parts = end_date.split('-')
                    if len(end_parts) >= 2:
                        end_year, end_month = end_parts[0], end_parts[1]
                        end_formatted = f"{end_year}年{end_month}月"
                    else:
                        end_formatted = end_date
                else:
                    end_formatted = end_date

                return f"{start_formatted}至{end_formatted}"
            else:
                return time_range
        except Exception:
            return time_range