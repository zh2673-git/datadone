#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import logging
from typing import Dict, List, Optional
from docx import Document
from docx.shared import Pt, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH

class WordExporter:
    def __init__(self, output_dir: str = 'output'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_dir = output_dir
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
            
            # 为每个人生成各类型的分析内容
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
        
        # 特殊金额分析
        # Pass a copy of person_data to avoid side effects if the method modifies it
        special_amounts_df = analyzer.analyze_special_amounts(person_data.copy())
        if not special_amounts_df.empty:
            self._generate_special_amount_summary(doc, person_name, analyzer, special_amounts_df, "银行", section_num)
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
        
        # 特殊金额分析
        special_amounts_df = analyzer.analyze_special_amounts(person_data.copy())
        if not special_amounts_df.empty:
            self._generate_special_amount_summary(doc, person_name, analyzer, special_amounts_df, payment_type, section_num)
            section_num += 1
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
            
            doc.add_paragraph("最密切联系人TOP 5:")
            top_5 = freq_df.nlargest(5, '通话次数')
            
            # 创建一个新的DataFrame用于显示
            display_df = pd.DataFrame()
            
            # 复制基本列
            display_df['对方姓名'] = top_5['对方姓名']
            display_df['对方号码'] = top_5['对方号码']
            display_df['通话次数'] = top_5['通话次数']
            display_df['通话总时长(分钟)'] = top_5['通话总时长(分钟)']
            
            # 处理对方单位列
            # 首先检查是否有带后缀的对方单位列（来自话单数据）
            unit_col = next((col for col in top_5.columns if '对方单位名称_' in col), None)
            if unit_col:
                display_df['对方单位'] = top_5[unit_col]
            elif '对方单位' in top_5.columns:
                # 如果有多个单位（用|分隔），只取第一个
                display_df['对方单位'] = top_5['对方单位'].apply(
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

        # 单笔主要金额
        all_amounts = person_data[data_model.amount_column].dropna().sort_values(ascending=False)
        top_single_amounts = ", ".join([f"{x:,.2f}元" for x in all_amounts.head(3).tolist()])

        # 重复最多的金额
        amount_counts = person_data[data_model.amount_column].value_counts()
        most_frequent_amount = amount_counts.index[0]
        most_frequent_count = amount_counts.iloc[0]
        most_frequent_amount_info = f"{most_frequent_amount:,.2f}元 ({most_frequent_count}次)"

        # 生成概览段落，使用实际的换行而不是\n
        p = doc.add_paragraph()
        p.add_run(f"{section_num}、{analysis_type_str}概览").bold = True
        p.add_run(f"：在 {time_span_str} 期间，总收入 {total_income:,.2f} 元，总支出 {total_expense:,.2f} 元，")
        p.add_run(f"净流水 {net_flow:,.2f} 元，共计 {total_transactions_count} 笔交易。")
        
        doc.add_paragraph(f"主要时间集中在：{major_time_str}。")
        doc.add_paragraph(f'单笔主要金额：{top_single_amounts}。')
        
        if abs(most_frequent_amount) > 5000:
            p.add_run('重复最多的金额为').bold = False
            p.add_run(f' {most_frequent_amount_info}，请注意核查。').bold = True
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
        p.add_run(f"：总{cash_type}{total_count}笔，总金额{total_amount:,.2f}元。")
        
        # 查找单笔金额最高的交易
        person_cash_data = bank_model.data[
            (bank_model.data['本方姓名'] == person_name) & 
            (bank_model.data['存取现标识'] == cash_type)
        ]
        if not person_cash_data.empty:
            top_transaction = person_cash_data.loc[person_cash_data[bank_model.amount_column].abs().idxmax()]
            top_amount = top_transaction[bank_model.amount_column]
            top_date = top_transaction[bank_model.date_column].strftime('%Y年%m月%d日')
            doc.add_paragraph(f"其中，单笔最高{cash_type}金额为 {abs(top_amount):,.2f}元，发生于{top_date}。")

    def _generate_special_amount_summary(self, doc: Document, person_name: str, analyzer, special_amounts_df: pd.DataFrame, analysis_type_str: str, section_num: int):
        """生成特殊金额分析的概要段落"""
        if not special_amounts_df.empty:
            p = doc.add_paragraph()
            p.add_run(f"{section_num}、特殊金额：").bold = True

            love_amounts = [520, 1314, 521]
            amount_col = analyzer.data_model.amount_column
            
            unique_amounts = special_amounts_df[amount_col].abs().unique()
            unique_amounts_str = "、".join(map(str, unique_amounts))
            total_times = len(special_amounts_df)
            
            p.add_run(f"{person_name}发生{unique_amounts_str}等特殊金额{total_times}次，主要金额为")
            
            top_txns = special_amounts_df.sort_values(by=amount_col, key=abs, ascending=False).head(3)
            
            for i, row in top_txns.iterrows():
                amount = row[amount_col]
                opponent = row[analyzer.data_model.opposite_name_column]
                
                # 处理金额的run
                amount_run = p.add_run(f"{amount:,.2f}元")
                if abs(amount) in love_amounts:
                    amount_run.bold = True
                
                # 处理对方姓名的run
                p.add_run(f"（{opponent}）")

                if i != top_txns.index[-1]:
                    p.add_run("、")

            p.add_run("。")

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

        for base_source in available_sources:
            doc.add_heading(f"以{comprehensive_analyzer._get_chinese_data_type_name(base_source)}为基础的交叉分析", level=3)
            
            results = comprehensive_analyzer.analyze(base_source=base_source)
            result_key = f"综合分析_以{comprehensive_analyzer._get_chinese_data_type_name(base_source)}为基准"
            
            if not results or result_key not in results or results[result_key].empty:
                doc.add_paragraph("未生成有效的交叉分析结果。")
                continue

            df = results[result_key]

            # 筛选出至少在两个不同类型数据中都出现过的对手方
            count_cols = [col for col in df.columns if '次数' in col or '总额' in col]
            
            if not count_cols:
                display_df = df.head(10)
            else:
                df['关联数据源数量'] = df[count_cols].notna().sum(axis=1)
                correlated_df = df[df['关联数据源数量'] > 1].sort_values(by='关联数据源数量', ascending=False)
                display_df = correlated_df.head(10) 

            if display_df.empty:
                doc.add_paragraph("未发现显著关联的对手方。")
                continue
            
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
            
            # 格式化金额列
            for col in ['银行总金额', '微信总金额', '支付宝总金额']:
                final_df[col] = final_df[col].apply(
                    lambda x: f"{float(x):,.2f}" if isinstance(x, (int, float)) or (isinstance(x, str) and x.replace('.', '').replace('-', '').isdigit()) else x
                )
            
            # 格式化通话次数列
            final_df['通话次数'] = final_df['通话次数'].apply(
                lambda x: str(int(float(x))) if isinstance(x, (int, float)) or (isinstance(x, str) and x.replace('.', '').isdigit()) else x
            )
            
            # 定义最终显示列的顺序
            display_columns = [
                '本方姓名', '对方姓名', '对方单位',
                '银行总金额', '微信总金额', '支付宝总金额', '通话次数'
            ]
            
            # 添加到文档
            self._add_df_to_doc(doc, final_df[display_columns])

    def _add_top_opponent_tables(self, doc: Document, frequency_df: pd.DataFrame):
        """为资金分析添加Top5对手方表格"""
        # 只保留有收入的记录
        income_df = frequency_df[frequency_df['总收入'] > 0].copy()
        if not income_df.empty:
            # 计算总收入占比
            income_df['收入占比'] = income_df['总收入'] / (income_df['总收入'] - income_df['总支出']) * 100
            # 筛选出收入占比为100%的记录
            pure_income_df = income_df[income_df['收入占比'] == 100].nlargest(5, '总收入')
            
            if not pure_income_df.empty:
                doc.add_paragraph("转入金额占比100%的金额前5名对手方如下：")
                
                # 准备显示列
                display_cols = ['对方姓名', '对方单位', '总收入', '交易次数']
                
                # 创建一个新的DataFrame用于显示
                display_df = pd.DataFrame()
                
                # 复制必需的列
                display_df['对方姓名'] = pure_income_df['对方姓名']
                display_df['总收入'] = pure_income_df['总收入']
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

    def _add_df_to_doc(self, doc: Document, df: pd.DataFrame):
        if df.empty:
            doc.add_paragraph("无相关数据。")
            return
        
        df = df.fillna('N/A').astype(str)
        
        table = doc.add_table(rows=1, cols=len(df.columns))
        table.style = 'Table Grid'
        
        # 设置表头
        for i, column in enumerate(df.columns):
            table.cell(0, i).text = str(column)
            
        # 添加数据行
        for _, row in df.iterrows():
            cells = table.add_row().cells
            for i, value in enumerate(row):
                cells[i].text = str(value)