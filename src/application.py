#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Application 入口类 - 统一协调所有模块"""

import logging
import os
from typing import Dict, Optional

import pandas as pd

from src.models.analysis_result import AnalysisResult
from src.models.report import ExportConfig

logger = logging.getLogger(__name__)


class Application:
    """应用入口 - 协调数据层、分析层、导出层的完整流程"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        # 配置层
        from src.config.settings import AppSettings
        from src.config.keywords import KeywordsManager
        from src.config.thresholds import ThresholdsManager
        self.settings = AppSettings()
        self.keywords = KeywordsManager(self.settings)
        self.thresholds = ThresholdsManager(self.settings)

        # 数据层
        from src.datasource.bank_loader import BankLoader
        from src.datasource.wechat_loader import WechatLoader
        from src.datasource.alipay_loader import AlipayLoader
        from src.datasource.call_loader import CallLoader
        self.bank_loader = BankLoader(self.settings.get('data_sources'))
        self.wechat_loader = WechatLoader(self.settings.get('data_sources'))
        self.alipay_loader = AlipayLoader(self.settings.get('data_sources'))
        self.call_loader = CallLoader(self.settings.get('data_sources'))

        # 分析层
        from src.analysis.engine import AnalysisEngine
        self.analysis_engine = AnalysisEngine(self.keywords, self.thresholds)

        # 导出层
        from src.export.report_builder import ReportBuilder
        from src.export.excel_exporter import ExcelExporter
        from src.export.word_exporter import WordExporter
        self.report_builder = ReportBuilder()
        self.excel_exporter = ExcelExporter()
        self.word_exporter = WordExporter()

        # 全局状态
        self._state: dict = {
            'data_loaded': False,
            'analysis_result': None,
            'raw_data': {},
        }

    def load_data(self, data_paths: dict[str, str]) -> None:
        """
        加载所有数据源

        Args:
            data_paths: 数据源类型→文件路径的映射
                {'bank': 'path/to/bank.xlsx', 'wechat': 'path/to/wechat.csv', ...}
        """
        self.logger.info("开始加载数据...")

        raw_data = {}

        # 加载银行数据
        if 'bank' in data_paths:
            try:
                bank_result = self.bank_loader.load(data_paths['bank'])
                raw_data['bank'] = bank_result
                self.logger.info(f"银行数据加载完成: {data_paths['bank']}")
            except Exception as e:
                self.logger.warning(f"银行数据加载失败: {e}")

        # 加载微信数据
        if 'wechat' in data_paths:
            try:
                wechat_result = self.wechat_loader.load(data_paths['wechat'])
                raw_data['wechat'] = wechat_result
                self.logger.info(f"微信数据加载完成: {data_paths['wechat']}")
            except Exception as e:
                self.logger.warning(f"微信数据加载失败: {e}")

        # 加载支付宝数据
        if 'alipay' in data_paths:
            try:
                alipay_result = self.alipay_loader.load(data_paths['alipay'])
                raw_data['alipay'] = alipay_result
                self.logger.info(f"支付宝数据加载完成: {data_paths['alipay']}")
            except Exception as e:
                self.logger.warning(f"支付宝数据加载失败: {e}")

        # 加载话单数据
        if 'call' in data_paths:
            try:
                call_result = self.call_loader.load(data_paths['call'])
                raw_data['call'] = call_result
                self.logger.info(f"话单数据加载完成: {data_paths['call']}")
            except Exception as e:
                self.logger.warning(f"话单数据加载失败: {e}")

        self._state['raw_data'] = raw_data
        self._state['data_loaded'] = True
        self._state['data_paths'] = data_paths
        self.logger.info("数据加载完成")

    def analyze(self, analysis_type: str = 'all') -> AnalysisResult:
        """
        执行分析

        Args:
            analysis_type: 分析类型
                'all' | 'frequency' | 'cash' | 'special' | 'cross' |
                'key_transaction' | 'fund_tracking' | 'advanced'
        Returns:
            AnalysisResult: 分析结果
        """
        if not self._state.get('data_loaded'):
            raise RuntimeError("请先加载数据")

        self.logger.info(f"开始执行分析，类型: {analysis_type}")

        result = self.analysis_engine.analyze(self._state, analysis_type)
        self._state['analysis_result'] = result

        self.logger.info(f"分析完成，共分析 {len(result.persons)} 人")
        return result

    def export(self, export_format: str = 'both',
               config: Optional[ExportConfig] = None) -> dict[str, str]:
        """
        导出报告

        Args:
            export_format: 导出格式 'excel' | 'word' | 'both'
            config: 导出配置
        Returns:
            {格式: 文件路径}
        """
        analysis_result = self._state.get('analysis_result')
        if analysis_result is None:
            raise RuntimeError("请先执行分析")

        if config is None:
            config = ExportConfig()

        # 构建报告数据
        self.logger.info("构建报告数据...")
        report_data = self.report_builder.build(
            analysis_result,
            raw_data=self._state.get('raw_data', {})
        )

        output_paths = {}

        if export_format in ('excel', 'both'):
            self.logger.info("导出Excel报告...")
            excel_path = self.excel_exporter.export(report_data, config)
            output_paths['excel'] = excel_path

        if export_format in ('word', 'both'):
            self.logger.info("导出Word报告...")
            word_path = self.word_exporter.export(report_data, config)
            output_paths['word'] = word_path

        return output_paths

    def run(self, data_paths: dict[str, str],
            analysis_type: str = 'all',
            export_format: str = 'both',
            config: Optional[ExportConfig] = None) -> dict[str, str]:
        """
        一键运行：加载→分析→导出

        Args:
            data_paths: 数据源类型→文件路径
            analysis_type: 分析类型
            export_format: 导出格式
            config: 导出配置
        Returns:
            {格式: 文件路径}
        """
        # 1. 加载数据
        self.load_data(data_paths)

        # 2. 执行分析
        self.analyze(analysis_type)

        # 3. 导出报告
        return self.export(export_format, config)

    def run_combined(self, person_folders: dict[str, dict[str, str]],
                     analysis_type: str = 'all',
                     export_format: str = 'both',
                     output_dir: str = 'output') -> dict[str, str]:
        """
        合并分析：将所有人员数据加载在一起统一分析，生成一份合并报告。
        这样可以识别人物之间的资金往来关系、交叉交易对手等。

        Args:
            person_folders: {人物名: {数据类型: 文件路径}}
            analysis_type: 分析类型
            export_format: 导出格式
            output_dir: 输出目录
        Returns:
            {格式: 文件路径}
        """
        self.logger.info(f"开始合并分析，共 {len(person_folders)} 人: {', '.join(person_folders.keys())}")

        # 合并所有人员的原始数据
        combined_raw: dict[str, dict[str, pd.DataFrame]] = {
            'bank': {}, 'wechat': {}, 'alipay': {}, 'call': {}
        }

        for person_name, data_paths in person_folders.items():
            self.logger.info(f"  加载 {person_name} 的数据...")

            if 'bank' in data_paths:
                try:
                    result = self.bank_loader.load(data_paths['bank'])
                    combined_raw['bank'].update(result)
                except Exception as e:
                    self.logger.warning(f"  {person_name} 银行数据加载失败: {e}")

            if 'wechat' in data_paths:
                try:
                    result = self.wechat_loader.load(data_paths['wechat'])
                    combined_raw['wechat'].update(result)
                except Exception as e:
                    self.logger.warning(f"  {person_name} 微信数据加载失败: {e}")

            if 'alipay' in data_paths:
                try:
                    result = self.alipay_loader.load(data_paths['alipay'])
                    combined_raw['alipay'].update(result)
                except Exception as e:
                    self.logger.warning(f"  {person_name} 支付宝数据加载失败: {e}")

            if 'call' in data_paths:
                try:
                    result = self.call_loader.load(data_paths['call'])
                    combined_raw['call'].update(result)
                except Exception as e:
                    self.logger.warning(f"  {person_name} 话单数据加载失败: {e}")

        # 设置状态
        self._state = {
            'data_loaded': True,
            'analysis_result': None,
            'raw_data': combined_raw,
            'data_paths': {dtype: ','.join(paths.values()) for dtype, paths in person_folders.items()},
        }

        # 执行分析
        self.analyze(analysis_type)

        # 导出报告
        config = ExportConfig(output_dir=os.path.join(output_dir, '合并分析'))
        return self.export(export_format, config)

    def run_batch(self, person_folders: dict[str, dict[str, str]],
                  analysis_type: str = 'all',
                  export_format: str = 'both',
                  base_output_dir: str = 'output') -> dict[str, dict[str, str]]:
        """
        批量运行：对每个人物文件夹分别执行 加载→分析→导出

        Args:
            person_folders: {人物名: {数据类型: 文件路径}}
            analysis_type: 分析类型
            export_format: 导出格式
            base_output_dir: 输出根目录
        Returns:
            {人物名: {格式: 文件路径}}
        """
        all_results: dict[str, dict[str, str]] = {}

        for person_name, data_paths in person_folders.items():
            self.logger.info(f"========== 开始处理: {person_name} ==========")
            print(f"\n>>> 正在处理: {person_name}")

            try:
                # 重置状态，确保每人独立
                self._state = {
                    'data_loaded': False,
                    'analysis_result': None,
                    'raw_data': {},
                }

                # 输出目录: output/人物名/
                person_output_dir = os.path.join(base_output_dir, person_name)
                config = ExportConfig(output_dir=person_output_dir)

                paths = self.run(data_paths, analysis_type, export_format, config)
                all_results[person_name] = paths

                # 打印结果
                for fmt, path in paths.items():
                    size = os.path.getsize(path) if os.path.exists(path) else 0
                    print(f"  {fmt}: {path} ({size:,} bytes)")
                print(f"<<< {person_name} 处理完成")

            except Exception as e:
                self.logger.error(f"处理 {person_name} 失败: {e}")
                print(f"  [错误] {person_name}: {e}")
                all_results[person_name] = {'error': str(e)}

        return all_results
