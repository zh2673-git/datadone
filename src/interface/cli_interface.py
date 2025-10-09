#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime
import traceback

from ..model.base_model import BaseDataModel
from ..model.bank_model import BankDataModel
from ..model.call_model import CallDataModel
from ..model.wechat_model import WeChatDataModel
from ..model.alipay_model import AlipayDataModel
from src.utils.group import GroupManager
from src.analysis import BankAnalyzer, CallAnalyzer, WeChatAnalyzer, AlipayAnalyzer, ComprehensiveAnalyzer
from src.export import ExcelExporter, WordExporter

from .base_interface import BaseInterface
from .cli_interface_group import *
from .cli_interface_export import *
from src.utils.config import Config

class CommandLineInterface(BaseInterface):
    """
    �����н�����??    """
    def __init__(self, data_path: str = 'data', config=None):
        """
        ��ʼ�������н�������
        
        Parameters:
        -----------
        data_path : str, optional
            �����ļ���·����Ĭ��??'data'
        config : Config, optional
            ���ö���Ĭ��Ϊ None
        """
        super().__init__(data_path)
        
        # 初始化数据模型
        self.data_models: Dict[str, Optional[BaseDataModel]] = {'bank': None, 'call': None, 'wechat': None, 'alipay': None}
        
        # 初始化分组管理器
        self.group_manager = GroupManager()
        
        # 初始化分析器字典
        self.analyzers: Dict[str, Optional[Any]] = {}
        
        # 初始化分析结果字典
        self.analysis_results: Dict[str, pd.DataFrame] = {}
        
        # 初始化配置
        self.config = config or Config()

        # 初始化导出器
        self.excel_exporter = ExcelExporter(config=self.config)
        self.word_exporter = WordExporter(config=self.config)
        self.logger = logging.getLogger('main')
        
        # 如果配置了自动重载，启动文件监控
        if self.config.get('app', {}).get('config_auto_reload', False):
            self.config.start_watching()
        
        # 初始化分析器
        self._initialize_analyzers()
        
        # 绑定方法
        self.export_results_menu = export_results_menu.__get__(self)
        self.export_to_excel = export_to_excel.__get__(self)
        self.generate_word_report = generate_word_report.__get__(self)
        
        self.logger.info("命令行界面初始化完成")
    
    def start(self):
        """
        启动命令行界面主循环
        """
        super().start()
        
        # 主循环
        while True:
            choice = self.display_main_menu()
            
            if choice == -1:  # 退出系统
                # 停止加载文件
                if self.config.get('app', {}).get('config_auto_reload', False):
                    self.config.stop_watching()
                break
            
            self.handle_main_menu_choice(choice)
    
    def display_main_menu(self) -> int:
        """
        显示主菜单
        
        Returns:
        --------
        int
            用户选择的选项索引(0-based)，-1表示退出
        """
        options = [
            "加载数据",
            "执行分析",
            "导出已有分析结果",
            "退出系统"
        ]
        
        choice = self.display_menu(options, "主菜单")
        
        if choice == 3:  # 退出系统
            return -1
        
        return choice
    
    def handle_main_menu_choice(self, choice: int):
        """
        处理主菜单选择
        
        Parameters:
        -----------
        choice : int
            选择的选项索引(0-based)
        """
        menu_actions = {
            0: self.load_data,
            1: self.run_analysis_menu,
            2: self.export_results_menu,
        }
        action = menu_actions.get(choice)
        if action:
            action()
    
    def load_data(self):
        """
        加载数据
        自动加载data文件夹下的所有Excel文件
        """
        print("\n=> 步骤 1: 加载数据")
        print("-" * 20)
        
        self.auto_load_all_data()
        
        # 数据加载后重新初始化分析器
        self._initialize_analyzers()
        self.display_success("数据加载和预处理完成")
    
    def auto_load_all_data(self):
        """
        自动加载data文件夹中的所有Excel文件
        """
        print("\n自动加载数据中...")
        print("-" * 20)
        
        data_dir = "data"
        if not os.path.exists(data_dir):
            self.display_error(f"数据文件夹 {data_dir} 不存在")
            return
        
        # 获取所有Excel文件
        excel_files = [os.path.join(r, f) for r, _, files in os.walk(data_dir) for f in files if f.endswith(('.xlsx', '.xls'))]
        
        if not excel_files:
            self.display_error(f"在 {data_dir} 文件夹中没有找到Excel文件")
            return
        
        self.logger.info(f"找到 {len(excel_files)} 个Excel文件")
        print(f"找到 {len(excel_files)} 个Excel文件:")
        for i, file in enumerate(excel_files, 1):
            print(f"{i}. {file}")
        
        # 对每个文件进行自动识别类型并加载
        loaded_count = 0
        for file_path in excel_files:
            try:
                # 读取前几行数据进行类型识别
                df_preview = pd.read_excel(file_path, nrows=5)
                columns = df_preview.columns.tolist()
                
                data_type = self.identify_data_type(columns)
                if data_type:
                    self.logger.info(f"识别 {file_path} 为{data_type}数据")
                    self.load_specific_data(data_type, file_path)
                    loaded_count += 1
                else:
                    self.logger.warning(f"无法识别 {file_path} 的数据类型")
            except Exception as e:
                self.logger.error(f"加载 {file_path} 失败: {str(e)}")
        
        if loaded_count == 0:
            self.display_warning("未能成功加载任何数据")
        else:
            self.display_success(f"成功加载 {loaded_count} 个数据文件")
            
    def identify_data_type(self, columns):
        """
        根据列名识别数据类型，基于配置文件中的字段配置
        
        Parameters:
        -----------
        columns : List[str]
            列名列表
            
        Returns:
        --------
        str
            数据类型 'bank', 'call', 'wechat', 'alipay' 或None(无法识别)
        """
        columns_set = set(columns)
        
        # 从配置文件中获取每种数据类型的字段配置
        data_source_configs = self.config.get('data_sources', {})
        
        # 定义每种数据类型的关键字段列表（基于配置文件）
        bank_fields = [
            data_source_configs.get('bank', {}).get('name_column', '本方姓名'),
            data_source_configs.get('bank', {}).get('date_column', '交易日期'),
            data_source_configs.get('bank', {}).get('amount_column', '交易金额'),
            data_source_configs.get('bank', {}).get('balance_column', '账户余额'),
            data_source_configs.get('bank', {}).get('type_column', '交易类型'),
            data_source_configs.get('bank', {}).get('direction_column', '借贷标识'),
            data_source_configs.get('bank', {}).get('opposite_name_column', '对方姓名'),
            data_source_configs.get('bank', {}).get('bank_name_column', '银行类型'),
            data_source_configs.get('bank', {}).get('account_column', '本方账号')
        ]
        
        call_fields = [
            data_source_configs.get('call', {}).get('name_column', '本方姓名'),
            data_source_configs.get('call', {}).get('date_column', '呼叫日期'),
            data_source_configs.get('call', {}).get('time_column', '时间'),
            data_source_configs.get('call', {}).get('duration_column', '通话时长'),
            data_source_configs.get('call', {}).get('opposite_name_column', '对方姓名'),
            data_source_configs.get('call', {}).get('opposite_number_column', '对方号码'),
            data_source_configs.get('call', {}).get('call_type_column', '呼叫类型'),
            data_source_configs.get('call', {}).get('special_date_column', '特殊日期名称')
        ]
        
        wechat_fields = [
            data_source_configs.get('wechat', {}).get('name_column', '本方姓名'),
            data_source_configs.get('wechat', {}).get('account_column', '本方微信账号'),
            data_source_configs.get('wechat', {}).get('date_column', '交易日期'),
            data_source_configs.get('wechat', {}).get('time_column', '交易时间'),
            data_source_configs.get('wechat', {}).get('amount_column', '交易金额'),
            data_source_configs.get('wechat', {}).get('balance_column', '账户余额'),
            data_source_configs.get('wechat', {}).get('type_column', '交易类型'),
            data_source_configs.get('wechat', {}).get('direction_column', '借贷标识'),
            data_source_configs.get('wechat', {}).get('opposite_name_column', '对方姓名'),
            data_source_configs.get('wechat', {}).get('opposite_account_column', '对方微信账号'),
            data_source_configs.get('wechat', {}).get('payment_method_column', '支付方式'),
            data_source_configs.get('wechat', {}).get('merchant_name_column', '商户名称')
        ]
        
        alipay_fields = [
            data_source_configs.get('alipay', {}).get('name_column', '本方姓名'),
            data_source_configs.get('alipay', {}).get('account_column', '本方账号'),
            data_source_configs.get('alipay', {}).get('date_column', '交易日期'),
            data_source_configs.get('alipay', {}).get('time_column', '交易时间'),
            data_source_configs.get('alipay', {}).get('amount_column', '交易金额'),
            data_source_configs.get('alipay', {}).get('type_column', '交易类型'),
            data_source_configs.get('alipay', {}).get('direction_column', '借贷标识'),
            data_source_configs.get('alipay', {}).get('opposite_name_column', '对方姓名'),
            data_source_configs.get('alipay', {}).get('opposite_account_column', '对方账号'),
            data_source_configs.get('alipay', {}).get('payment_method_column', '支付方式'),
            data_source_configs.get('alipay', {}).get('product_name_column', '交易商品名称')
        ]

        # 计算每种数据类型的匹配分数（基于配置文件中的字段）
        bank_score = sum(1 for field in bank_fields if any(field in col for col in columns))
        call_score = sum(1 for field in call_fields if any(field in col for col in columns))
        wechat_score = sum(1 for field in wechat_fields if any(field in col for col in columns))
        alipay_score = sum(1 for field in alipay_fields if any(field in col for col in columns))

        # 增加特定关键词的权重：如果包含特定关键词，则增加分数
        if any('支付宝' in col for col in columns):
            alipay_score += 5  # 大幅增加支付宝关键词的权重
        if any('微信' in col for col in columns):
            wechat_score += 5  # 大幅增加微信关键词的权重
        if any('银行' in col for col in columns):
            bank_score += 3  # 增加银行关键词的权重
        if any('通话' in col for col in columns) or any('电话' in col for col in columns):
            call_score += 3  # 增加通话/电话关键词的权重

        # 设置阈值，匹配分数达到3分以上才识别
        scores = {
            'bank': bank_score,
            'call': call_score, 
            'wechat': wechat_score,
            'alipay': alipay_score
        }
        
        # 找到最高分的数据类型
        max_score_type = max(scores, key=scores.get)
        max_score = scores[max_score_type]
        
        # 如果最高分达到阈值，但存在分数相同的情况，优先识别更具体的数据类型
        if max_score >= 3:
            # 检查是否有多个类型达到相同分数
            tied_types = [t for t, s in scores.items() if s == max_score]
            
            if len(tied_types) > 1:
                # 优先顺序：微信/支付宝 > 银行 > 话单
                priority_order = ['wechat', 'alipay', 'bank', 'call']
                for data_type in priority_order:
                    if data_type in tied_types:
                        return data_type
            
            return max_score_type
        
        return None
            
    def load_specific_data(self, data_type, file_path):
        """
        加载特定类型的数据，并支持将相同类型的文件合并
        
        Parameters:
        -----------
        data_type : str
            数据类型 'bank', 'call', 'wechat', 'alipay'
        file_path : str
            数据文件路径
        """
        try:
            model_class = self.get_model_class(data_type)
            if not model_class:
                return

            if self.data_models.get(data_type):
                # 如果模型已存在，则将新数据合并到现有数据中
                existing_model = self.data_models[data_type]
                self.logger.info(f"模型 {data_type} 已存在，追加数据: {file_path}")
                
                # 创建临时模型加载新文件
                temp_model = model_class(data_path=file_path)
                
                if not temp_model.data.empty:
                    # 合并数据
                    combined_data = pd.concat([existing_model.data, temp_model.data], ignore_index=True)
                    existing_model.data = combined_data
                    # 重新预处理合并后的数据
                    existing_model.preprocess() 
                    self.display_success(f"已将 {file_path} 合并到现有的 {data_type} 数据中。总记录数: {len(combined_data)}")
            else:
                # 如果模型不存在，则创建新模型
                self.logger.info(f"模型 {data_type} 不存在，创建新模型: {file_path}")
                model = model_class(data_path=file_path)
                self.data_models[data_type] = model
                self.analyzers[data_type] = self.create_analyzer(data_type, model)
                self.display_success(f"成功加载{data_type}数据: {file_path}，共 {len(model.data)} 条记录")

        except Exception as e:
            self.logger.error(f"加载 {data_type} 数据 ({file_path}) 失败: {str(e)}")
            self.display_error(f"加载 {data_type} 数据 ({file_path}) 失败: {str(e)}")

    def get_model_class(self, data_type: str):
        """获取数据类型对应的模型类"""
        if data_type == 'bank':
            from ..model.bank_model import BankDataModel
            return BankDataModel
        elif data_type == 'call':
            from ..model.call_model import CallDataModel
            return CallDataModel
        elif data_type == 'wechat':
            from ..model.wechat_model import WeChatDataModel
            return WeChatDataModel
        elif data_type == 'alipay':
            from ..model.alipay_model import AlipayDataModel
            return AlipayDataModel
        return None
            
    def create_analyzer(self, data_type, model):
        """
        创建分析器
        
        Parameters:
        -----------
        data_type : str
            数据类型 'bank', 'call', 'wechat', 'alipay'
        model : BaseDataModel
            数据模型
            
        Returns:
        --------
        BaseAnalyzer
            分析器对象
        """
        if data_type == 'bank':
            from src.analysis import BankAnalyzer
            return BankAnalyzer(model, self.group_manager, self.config)
        elif data_type == 'call':
            from src.analysis import CallAnalyzer
            return CallAnalyzer(model, self.group_manager, self.config)
        elif data_type == 'wechat':
            from src.analysis import WeChatAnalyzer
            return WeChatAnalyzer(model, self.group_manager, self.config)
        elif data_type == 'alipay':
            from src.analysis import AlipayAnalyzer
            return AlipayAnalyzer(model, self.group_manager, self.config)
        return None
    
    def display_data_status(self):
        """
        显示已加载数据状态
        """
        print("\n当前数据加载状态：")
        print("-" * 20)
        
        any_data_loaded = False
        for data_type, model in self.data_models.items():
            if model and not model.data.empty:
                print(f" - {data_type.capitalize()} 数据: 已加载，共 {len(model.data)} 条记录")
                any_data_loaded = True
        
        if not any_data_loaded:
            print("尚未加载任何数据")
            
    def _initialize_analyzers(self):
        """
        根据加载的数据模型重新初始化分析器
        """
        # 清空所有分析器
        self.analyzers = {}

        # 为每个有数据的模型创建分析器
        for data_type, model in self.data_models.items():
            if model and not model.data.empty:
                self.analyzers[data_type] = self.create_analyzer(data_type, model)
        
        # 如果存在多个数据类型则创建综合分析器
        if len(self.analyzers) > 1:
            from src.analysis import ComprehensiveAnalyzer
            self.analyzers['comprehensive'] = ComprehensiveAnalyzer(self.data_models, self.group_manager, self.config)
            
        self.logger.info("分析器重新初始化完成")

    # -----------------------------------------------------
    # 分析和导出相关的交互逻辑
    # -----------------------------------------------------
    def run_analysis_menu(self):
        """
        显示分析功能的多级菜单
        """
        options = [
            "对已加载数据执行全部分析",
            "对特定数据类型进行专项分析",
            "返回主菜单"
        ]
        
        while True:
            choice = self.display_menu(options, "执行分析")
            
            if choice == -1 or choice == 2:  # 用户选择返回或取消
                break
            
            if choice == 0:
                self.run_all_analysis()
            elif choice == 1:
                self.run_specific_analysis_menu()

    def run_specific_analysis_menu(self):
        """
        显示分析专项功能的子菜单，只列出已加载数据的选项
        """
        analyzer_map = {
            "银行数据分析": self.run_bank_analysis,
            "通话数据分析": self.run_call_analysis,
            "微信数据分析": self.run_wechat_analysis,
            "支付宝数据分析": self.run_alipay_analysis,
            "综合分析": self.run_comprehensive_analysis,
        }
        
        menu_options = {name: func for name, func in analyzer_map.items() if self.is_analyzer_available(name)}

        if not menu_options:
            self.display_error("没有已加载且可用的数据类型。请先加载数据。")
            return

        options = list(menu_options.keys())
        choice = self.display_menu(options, "选择要执行的专项分析")

        if choice != -1:
            action_name = options[choice]
            action = menu_options[action_name]
            action()

    def is_analyzer_available(self, analyzer_key_name: str) -> bool:
        """检查一个分析器是否可用 (用于菜单过滤)"""
        key_map = {
            "银行数据分析": "bank",
            "通话数据分析": "call",
            "微信数据分析": "wechat",
            "支付宝数据分析": "alipay",
            "综合分析": "comprehensive"
        }
        analyzer_key = key_map.get(analyzer_key_name)
        return analyzer_key in self.analyzers and self.analyzers[analyzer_key] is not None

    def run_bank_analysis(self):
        self._run_analysis_by_source(analyzer_name='bank', analysis_type_options=['frequency', 'cash', 'all'])

    def run_call_analysis(self):
        self._run_analysis_by_source(analyzer_name='call')

    def run_wechat_analysis(self):
        self._run_analysis_by_source(analyzer_name='wechat', analysis_type_options=['frequency', 'all'])

    def run_alipay_analysis(self):
        self._run_analysis_by_source(analyzer_name='alipay', analysis_type_options=['frequency', 'all'])

    def _run_analysis_by_source(self, analyzer_name: str, analysis_type_options: Optional[List[str]] = None, **kwargs):
        """
        执行指定分析器的分析，支持按数据来源选择
        
        Parameters:
        ----------
        analyzer_name : str
            分析器名称
        analysis_type_options : Optional[List[str]], optional
            分析类型选项列表
        **kwargs : Any
            其他参数
        """
        try:
            # 获取分析器
            analyzer = self.analyzers.get(analyzer_name)
            if not analyzer:
                self.display_error(f"分析器 {analyzer_name} 不存在或未初始化")
                return
            
            # 获取该分析器可用的数据来源
            sources = analyzer.data_model.get_data_sources()
            if not sources:
                self.display_error(f"{analyzer_name} 数据中没有可用的数据来源")
                return
            
            # 如果有多个数据来源，让用户选择
            source = None
            if len(sources) > 1:
                source_options = ["所有数据来源"] + sources
                source_choice = self.display_menu(source_options, f"请选择要分析的{analyzer_name}数据", allow_empty=True)
                if source_choice == -1:
                    return  # 用户取消
                elif source_choice == 0:
                    source = None  # 分析所有数据来源
                else:
                    source = source_options[source_choice]
            else:
                source = sources[0]  # 只有一个数据来源
            
            # 如果有分析类型选项，让用户选择
            if analysis_type_options:
                type_options_display = [f"{opt}" for opt in analysis_type_options]
                type_choice = self.display_menu(type_options_display, f"请选择{analyzer_name}分析类型", allow_empty=True)
                if type_choice == -1:
                    return  # 用户取消
                
                selected_analysis_type = analysis_type_options[type_choice]
                kwargs['analysis_type'] = selected_analysis_type
                self.logger.info(f"选择的分析类型: {selected_analysis_type}")
            
            # 执行分析
            if source:
                self.logger.info(f"针对来源 {source} 执行 {analyzer_name} 分析...")
            else:
                self.logger.info(f"针对所有数据来源执行 {analyzer_name} 分析...")
            
            # 使用kwargs传递analysis_type等参数
            results = analyzer.analyze(source_name=source, **kwargs)

            if results:
                for key, df in results.items():
                    self.analysis_results[key] = df
                if source:
                    self.display_success(f"成功针对来源 '{source}' 执行 {analyzer_name} 分析")
                else:
                    self.display_success(f"成功针对所有数据来源执行 {analyzer_name} 分析")
            else:
                self.display_warning(f"{analyzer_name} 分析没有返回结果")
        except Exception as e:
            self.logger.error(f"执行 {analyzer_name} 分析失败: {e}")
            self.logger.debug(traceback.format_exc())
            self.display_error(f"执行 {analyzer_name} 分析失败: {str(e)}")

    def run_comprehensive_analysis(self):
        """
        执行综合分析, 以每种可用的数据类型为基准分别进行一次分析
        """
        print("\n=> 执行综合分析")
        print("-" * 20)
        
        if 'comprehensive' not in self.analyzers or not self.analyzers['comprehensive']:
            self.display_error("综合分析器未初始化，请先加载多种类型数据")
            return
            
        # 找出所有已加载且不为空的数据类型
        available_sources = [
            data_type for data_type, model in self.data_models.items() 
            if model and not model.data.empty
        ]
        
        if len(available_sources) < 2:
            self.display_warning("综合分析需要多种类型数据，当前数据不足")
            return

        total_results = {}
        self.display_info(f"将基于以下基准进行综合分析: {', '.join(available_sources)}")

        for base_source in available_sources:
            try:
                self.display_info(f"以 {base_source} 为基准进行分析...")
                results = self.analyzers['comprehensive'].analyze(base_source=base_source)
                if results:
                    total_results.update(results)
                    self.display_success(f"以 {base_source} 为基准综合分析完成")
                else:
                    self.display_warning(f"以 {base_source} 为基准综合分析未产生任何结果")
            except Exception as e:
                self.display_error(f"以 {base_source} 为基准综合分析失败: {e}")
                self.logger.error(f"以 {base_source} 为基准综合分析失败: {e}\n{traceback.format_exc()}")
        
        if total_results:
            self.analysis_results.update(total_results)
            self.display_success("\n综合分析完成")
            self.logger.info(f"综合分析产生的结果键: {list(total_results.keys())}")
            self.display_results_summary(total_results)
        else:
            self.display_warning("\n综合分析未产生任何结果")

    def run_all_analysis(self):
        """对所有可用数据源执行所有类型的分析"""
        self.display_message("执行全部分析")
        self.analysis_results = {}
        
        analyzers_to_run = {
            'bank': {'analysis_type': 'all'}, 'call': {}, 'wechat': {}, 'alipay': {}
        }
        
        has_data = any(self.analyzers.get(key) for key in analyzers_to_run)
        
        if not has_data:
            self.display_error("没有加载任何数据，无法执行分析")
            return

        for an_type, an_params in analyzers_to_run.items():
            if self.analyzers.get(an_type):
                self.display_message(f"--- 开始执行 {an_type} 分析 ---")
                try:
                    results = self.analyzers[an_type].analyze(**an_params)
                    if results:
                        self.analysis_results.update(results)
                except Exception as e:
                    self.display_error(f"{an_type} 分析失败: {e}")
        
        if self.analyzers.get('comprehensive'):
            self.display_message("--- 执行综合分析 ---")
            try:
                comp_results = self.analyzers['comprehensive'].analyze()
                if comp_results:
                    self.analysis_results.update(comp_results)
            except Exception as e:
                self.display_error(f"综合分析失败: {e}")

        self.display_success("\n分析完成")
        
        if self.analysis_results:
            if self.get_yes_no_input("是否继续执行分析?"):
                self.export_results_menu()

    def get_yes_no_input(self, prompt: str) -> bool:
        """
        获取用户的yes/no输入
        """
        while True:
            response = input(f"{prompt} (y/n): ").lower()
            if response in ['y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                self.display_warning("无效输入，请输入 'y' 或 'n'.")

    def run(self):
        """
        Run the command line interface.
        """
        self.start()

