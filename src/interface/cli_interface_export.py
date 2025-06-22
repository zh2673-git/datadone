#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Optional, Union, Any

from src.export import ExcelExporter, WordExporter

# 注意：此文件是cli_interface.py的一部分，包含导出和查看结果相关方法

def export_results_menu(self):
    """
    导出结果菜单
    """
    options = [
        "导出分析结果到Excel",
        "生成综合分析报告(Word)"
    ]
    
    while True:
        choice = self.display_menu(options, "导出结果")
        
        if choice == -1:  # 返回
            break
        
        if choice == 0:  # 导出为Excel
            self.export_to_excel()
        elif choice == 1:  # 生成Word报告
            self.generate_word_report()

def export_to_excel(self):
    """导出所有分析结果到单个Excel文件"""
    if not self.analysis_results:
        self.display_error("没有分析结果可以导出。")
        return
    
    default_filename = f"分析结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    filename = self.get_input(f"请输入Excel文件名 (默认为: {default_filename})") or default_filename
    
    try:
        self.excel_exporter.export(
            self.analysis_results,
            filename=filename,
            data_models=self.data_models
        )
        self.display_success(f"所有分析结果已成功导出到: output/{filename}")
    except Exception as e:
        self.display_error(f"导出Excel时出错: {str(e)}")
        self.logger.error(f"导出Excel失败: {e}", exc_info=True)

def generate_word_report(self):
    """
    生成统一的综合分析Word报告
    """
    self.display_message("\n生成综合分析报告 (Word)")
    self.display_message("-" * 20)
    
    if not self.analyzers:
        self.display_error("没有可用的分析器，无法生成报告。请先加载数据。")
        return
    
    default_title = f"综合分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    report_title = self.get_input(f"请输入报告标题 (默认为: {default_title})") or default_title
    
    try:
        self.display_message("正在生成报告，这可能需要一些时间...")
        
        # WordExporter的实例化应在主程序逻辑中完成，这里直接使用self.word_exporter
        self.word_exporter.generate_comprehensive_report(
            report_title=report_title,
            data_models=self.data_models,
            analyzers=self.analyzers
        )
        # 成功信息已在exporter内部打印
    except Exception as e:
        self.display_error(f"生成Word报告时发生未知错误: {str(e)}")
        self.logger.error(f"生成Word报告失败: {e}", exc_info=True)

def view_exportable_results(self):
    """
    查看可导出的结果
    """
    print("\n可导出的结果")
    print("-" * 15)
    
    if not self.analysis_results:
        self.display_warning("当前无可导出的结果，请先执行分析")
        return
    
    print(f"共有 {len(self.analysis_results)} 个结果可供导出:")
    
    for key, df in self.analysis_results.items():
        print(f"- {key}: {len(df)} 行, {len(df.columns)} 列")
        # 可以添加更多信息，如数据范围等

def view_results_menu(self):
    """
    查看结果菜单
    """
    # 检查是否有结果可查看
    if not self.analysis_results:
        self.display_error("没有结果可查看，请先执行分析")
        return
    
    # 显示所有结果
    result_names = list(self.analysis_results.keys())
    
    while True:
        print("\n可查看的结果:")
        for i, name in enumerate(result_names, 1):
            df = self.analysis_results[name]
            print(f"{i}. {name} ({len(df)} 条记录)")
        
        print("0. 返回上级菜单")
        
        try:
            choice = int(input("\n请选择要查看的结果（0表示返回）: "))
            
            if choice == 0:
                break
            
            if 1 <= choice <= len(result_names):
                name = result_names[choice - 1]
                self.view_result(name)
            else:
                print(f"无效的选项，请输入0-{len(result_names)}之间的数字")
        
        except ValueError:
            print("请输入有效的数字")

def view_result(self, result_name: str):
    """
    查看特定结果
    
    Parameters:
    -----------
    result_name : str
        结果名称
    """
    if result_name not in self.analysis_results:
        self.display_error(f"找不到结果 '{result_name}'")
        return
    
    df = self.analysis_results[result_name]
    
    # 显示数据
    self.display_data(df, f"{result_name} 分析结果", max_rows=20)
    
    # 显示统计信息
    numeric_cols = df.select_dtypes(include=['number']).columns
    
    if not numeric_cols.empty:
        print("\n数值列统计:")
        
        # 保存原始显示设置
        original_max_rows = pd.get_option('display.max_rows')
        original_max_columns = pd.get_option('display.max_columns')
        original_width = pd.get_option('display.width')
        
        # 设置临时显示设置
        pd.set_option('display.max_rows', 20)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 120)
        
        try:
            print(df[numeric_cols].describe().round(2))
        finally:
            # 恢复原始显示设置
            pd.set_option('display.max_rows', original_max_rows)
            pd.set_option('display.max_columns', original_max_columns)
            pd.set_option('display.width', original_width)
    
    # 显示选项
    print("\n选项:")
    print("1. 排序查看")
    print("2. 筛选查看")
    print("0. 返回")
    
    try:
        choice = int(input("\n请选择操作（0表示返回）: "))
        
        if choice == 1:  # 排序查看
            self.sort_result(result_name)
        elif choice == 2:  # 筛选查看
            self.filter_result(result_name)
    
    except ValueError:
        print("请输入有效的数字")

def sort_result(self, result_name: str):
    """
    排序查看结果
    
    Parameters:
    -----------
    result_name : str
        结果名称
    """
    df = self.analysis_results[result_name]
    
    # 选择排序列
    columns = df.columns.tolist()
    column_idx = self.get_multiple_choice(columns, prompt="请选择排序列", multiple=False)
    sort_column = columns[column_idx]
    
    # 选择排序方式
    sort_options = ["升序", "降序"]
    sort_idx = self.get_multiple_choice(sort_options, prompt="请选择排序方式", multiple=False)
    ascending = (sort_idx == 0)
    
    # 排序
    sorted_df = df.sort_values(by=sort_column, ascending=ascending)
    
    # 显示排序结果
    sort_type = "升序" if ascending else "降序"
    self.display_data(sorted_df, f"{result_name} - 按 {sort_column} {sort_type}排序", max_rows=20)

def filter_result(self, result_name: str):
    """
    筛选查看结果
    
    Parameters:
    -----------
    result_name : str
        结果名称
    """
    df = self.analysis_results[result_name]
    
    # 选择筛选列
    columns = df.columns.tolist()
    column_idx = self.get_multiple_choice(columns, prompt="请选择筛选列", multiple=False)
    filter_column = columns[column_idx]
    
    # 获取该列的数据类型
    column_type = df[filter_column].dtype
    
    # 根据数据类型选择筛选方式
    if pd.api.types.is_numeric_dtype(column_type):
        # 数值列筛选
        print(f"\n{filter_column} 列为数值类型")
        print(f"最小值: {df[filter_column].min()}")
        print(f"最大值: {df[filter_column].max()}")
        print(f"平均值: {df[filter_column].mean()}")
        
        # 获取筛选范围
        min_value = self.get_input(f"请输入最小值（默认为{df[filter_column].min()}）", default=str(df[filter_column].min()))
        max_value = self.get_input(f"请输入最大值（默认为{df[filter_column].max()}）", default=str(df[filter_column].max()))
        
        try:
            min_value = float(min_value)
            max_value = float(max_value)
            
            # 筛选
            filtered_df = df[(df[filter_column] >= min_value) & (df[filter_column] <= max_value)]
            
            # 显示筛选结果
            self.display_data(filtered_df, f"{result_name} - {filter_column} 在 [{min_value}, {max_value}] 范围内", max_rows=20)
        
        except ValueError:
            self.display_error("请输入有效的数值")
    
    else:
        # 非数值列筛选
        print(f"\n{filter_column} 列为非数值类型")
        
        # 获取唯一值
        unique_values = df[filter_column].unique().tolist()
        
        if len(unique_values) > 20:
            print(f"该列有 {len(unique_values)} 个唯一值，过多无法全部显示")
            filter_value = self.get_input(f"请输入要筛选的值")
            
            # 筛选
            filtered_df = df[df[filter_column] == filter_value]
        else:
            # 选择筛选值
            value_idx = self.get_multiple_choice(unique_values, prompt="请选择筛选值", multiple=False)
            filter_value = unique_values[value_idx]
            
            # 筛选
            filtered_df = df[df[filter_column] == filter_value]
        
        # 显示筛选结果
        self.display_data(filtered_df, f"{result_name} - {filter_column} = {filter_value}", max_rows=20)

def export_current_results(self, stage_name):
    """
    导出当前阶段的分析结果
    
    Parameters:
    -----------
    stage_name : str
        阶段名称，用于文件命名
    """
    if not self.analysis_results:
        self.display_warning("没有分析结果可导出")
        return
    
    try:
        # 导出Excel
        filename = f"分析结果_{stage_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # 使用统一的导出方法
        self.excel_exporter.export(
            self.analysis_results,
            filename=filename,
            data_models=self.data_models
        )
        self.display_success(f"分析结果已导出至 output/{filename}")
        
        # 生成Word报告
        report_title = f"{stage_name}_分析报告_{datetime.now().strftime('%Y%m%d')}"
        
        # 获取用户选择的报告内容
        include_sections = self.get_report_sections()
        
        # 调用Word导出器
        word_exporter = WordExporter(output_dir=self.output_dir)
        word_exporter.generate_report(
            report_title=report_title,
            group_name=stage_name,
            data_models=self.data_models,
            analyzers=self.analyzers,
            sections=include_sections
        )

        self.logger.info("Word报告导出完成")
    except Exception as e:
        self.display_error(f"导出结果时出错: {str(e)}")

def get_report_sections(self):
    """
    获取用户选择的报告内容
    
    Returns:
    --------
    List[int]
        用户选择的报告章节编号列表
    """
    self.display_info("请选择要包含在报告中的内容（默认全选）：")
    options = [
        "1. 基本信息",
        "2. 转账分析",
        "3. 存现分析",
        "4. 取现分析",
        "5. 密切联系人分析"
    ]
    
    selected = self.get_multiple_choice(options, default_all=True)
    
    # 将选项转换为章节编号
    sections = []
    for i, option in enumerate(options):
        if i+1 in selected:
            sections.append(i+1)
    
    return sections

def _export_frequency_results(self):
    """
    导出频率分析结果
    """
    # 获取分组信息
    groups = self._get_groups()
    
    # 检查有哪些数据源可用
    available_sources = []
    if 'bank' in self.data_models and self.data_models['bank'] is not None:
        available_sources.append('bank')
    if 'call' in self.data_models and self.data_models['call'] is not None:
        available_sources.append('call')
    if 'wechat' in self.data_models and self.data_models['wechat'] is not None:
        available_sources.append('wechat')
    if 'alipay' in self.data_models and self.data_models['alipay'] is not None:
        available_sources.append('alipay')
    
    if not available_sources:
        print("没有可用的数据源，无法导出频率分析结果")
        return
    
    # 显示可用的数据源
    print("可用的数据源：")
    for i, source in enumerate(available_sources, 1):
        print(f"{i}. {source}")
    
    # 选择数据源
    choice = input("请选择要导出的数据源（输入编号，多个用逗号分隔，全部导出请直接回车）：")
    
    selected_sources = []
    if choice.strip():
        try:
            indices = [int(idx.strip()) - 1 for idx in choice.split(',')]
            selected_sources = [available_sources[idx] for idx in indices if 0 <= idx < len(available_sources)]
        except (ValueError, IndexError):
            print("无效的选择")
            return
    else:
        selected_sources = available_sources
    
    # 导出每个分组的频率分析结果
    for group_name in groups:
        for source in selected_sources:
            analyzer = self.analyzers.get(source)
            if analyzer is None:
                print(f"没有 {source} 分析器，无法导出频率分析结果")
                continue
            
            print(f"正在导出 {group_name} 的 {source} 频率分析结果...")
            # 获取频率分析结果
            frequency_data = analyzer.analyze_frequency_by_group(group_name)
            if frequency_data.empty:
                print(f"没有 {group_name} 的 {source} 频率分析结果")
                continue
            
            # 导出到Excel
            output_file = self.excel_exporter.export_to_excel(
                {f"{group_name}_{source}_频率": frequency_data},
                f"{group_name}_{source}_频率分析"
            )
            if output_file:
                print(f"导出成功：{output_file}")
            else:
                print(f"导出 {group_name} 的 {source} 频率分析结果失败")

def _export_comprehensive_results(self):
    """
    导出综合分析结果
    """
    # 获取分组信息
    groups = self._get_groups()
    
    # 检查是否有综合分析器
    comprehensive_analyzer = self.analyzers.get('comprehensive')
    if comprehensive_analyzer is None:
        print("没有综合分析器，无法导出综合分析结果")
        return
    
    # 检查有哪些数据源可用
    available_sources = []
    if 'bank' in self.data_models and self.data_models['bank'] is not None:
        available_sources.append('bank')
    if 'call' in self.data_models and self.data_models['call'] is not None:
        available_sources.append('call')
    if 'wechat' in self.data_models and self.data_models['wechat'] is not None:
        available_sources.append('wechat')
    if 'alipay' in self.data_models and self.data_models['alipay'] is not None:
        available_sources.append('alipay')
    
    if not available_sources:
        print("没有可用的数据源，无法导出综合分析结果")
        return
    
    # 显示可用的基础数据源
    print("可用的基础数据源：")
    for i, source in enumerate(available_sources, 1):
        print(f"{i}. {source}")
    
    # 选择基础数据源
    choice = input("请选择要作为基础的数据源（输入编号，默认为call）：")
    
    base_source = 'call'  # 默认使用call作为基础数据源
    if choice.strip():
        try:
            idx = int(choice.strip()) - 1
            if 0 <= idx < len(available_sources):
                base_source = available_sources[idx]
            else:
                print(f"无效的选择，使用默认基础数据源 {base_source}")
        except ValueError:
            print(f"无效的选择，使用默认基础数据源 {base_source}")
    
    # 导出每个分组的综合分析结果
    for group_name in groups:
        print(f"正在导出 {group_name} 的综合分析结果（基础数据源：{base_source}）...")
        # 获取综合分析结果
        comprehensive_data = comprehensive_analyzer.analyze(group_name=group_name, base_source=base_source)
        if comprehensive_data.empty:
            print(f"没有 {group_name} 的综合分析结果")
            continue
        
        # 导出到Excel
        output_file = self.excel_exporter.export_to_excel(
            {f"{group_name}_综合分析": comprehensive_data},
            f"{group_name}_综合分析"
        )
        if output_file:
            print(f"导出成功：{output_file}")
        else:
            print(f"导出 {group_name} 的综合分析结果失败")

def _get_groups(self):
    """
    获取分组信息
    
    Returns:
    --------
    list
        分组名称列表
    """
    # 如果有分组管理器，则使用分组管理器中的分组
    if self.group_manager and self.group_manager.groups:
        return list(self.group_manager.groups.keys())
    
    # 否则，使用所有数据源中的所有人名作为分组
    all_persons = set()
    for model_name, model in self.data_models.items():
        if model is not None:
            all_persons.update(model.get_persons())
    
    return list(all_persons)

def _export_all_to_excel(self):
    """
    导出所有分析结果到Excel
    """
    # 导出频率分析结果
    self._export_frequency_results()
    
    # 导出存取现分析结果
    self._export_cash_operation_results()
    
    # 导出综合分析结果
    self._export_comprehensive_results()
    
    # 导出银行交易明细
    self._export_bank_transactions()

    # 获取文件名
    filename = self.get_path_input(f"请输入要保存的文件名 (默认为: {default_filename})", must_exist=False, is_dir=False)
    if not filename:
        filename = default_filename

    try:
        # 导出数据
        filepath = self.excel_exporter.export(
            self.analysis_results, 
            filename=filename,
            data_models=self.data_models
        )
        
        if filepath:
            self.display_success(f"结果已成功导出到: {filepath}")
        else:
            self.display_error("导出失败")
    except Exception as e:
        self.display_error(f"导出时出错: {str(e)}") 