#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""CLI界面 - 纯调度层，不包含数据处理逻辑"""

import os
import sys
import logging
from typing import Optional

from src.application import Application

logger = logging.getLogger(__name__)

# 文件名关键词 → 数据类型映射（按优先级排序，支付宝在微信之后避免误判）
_FILE_TYPE_RULES = [
    ('alipay', ['支付宝']),
    ('wechat', ['微信']),
    ('bank',   ['账单', '银行', '流水']),
    ('call',   ['话单', '通话']),
]

_DATA_DIR = 'data'


def scan_data_dir(data_dir: str = _DATA_DIR) -> dict[str, str]:
    """
    扫描 data/ 目录，根据文件名关键词自动识别数据类型。
    返回 {数据类型: 文件绝对路径}
    仅扫描顶层目录，兼容旧版单文件模式。
    """
    if not os.path.isdir(data_dir):
        return {}

    result = {}
    for fname in os.listdir(data_dir):
        if not fname.endswith(('.xlsx', '.xls', '.csv')):
            continue
        fpath = os.path.join(data_dir, fname)
        if not os.path.isfile(fpath):
            continue
        name_lower = fname.lower()
        for dtype, keywords in _FILE_TYPE_RULES:
            if any(kw in name_lower for kw in keywords):
                if dtype not in result:
                    result[dtype] = os.path.abspath(fpath)
                break

    return result


def scan_person_folders(data_dir: str = _DATA_DIR) -> dict[str, dict[str, str]]:
    """
    递归扫描 data/ 目录，识别人物文件夹并自动匹配文件类型。

    目录结构约定：
      data/
        任意层级子目录/
          人物文件夹/          ← 包含至少1个数据文件的文件夹
            XX账单数据预览.xlsx  ← bank
            XX微信账单浏览.xlsx  ← wechat
            XX支付宝账单.xlsx   ← alipay
            话单数据预览.xlsx    ← call

    返回 {人物文件夹名: {数据类型: 文件绝对路径}}
    """
    if not os.path.isdir(data_dir):
        return {}

    person_folders: dict[str, dict[str, str]] = {}

    for root, dirs, files in os.walk(data_dir):
        # 跳过隐藏目录和 output 目录
        dirs[:] = [d for d in dirs if not d.startswith('.') and d.lower() != 'output']

        # 在当前目录中匹配数据文件
        folder_files: dict[str, str] = {}
        for fname in files:
            if not fname.endswith(('.xlsx', '.xls', '.csv')):
                continue
            fpath = os.path.join(root, fname)
            name_lower = fname.lower()
            for dtype, keywords in _FILE_TYPE_RULES:
                if any(kw in name_lower for kw in keywords):
                    if dtype not in folder_files:
                        folder_files[dtype] = os.path.abspath(fpath)
                    break

        # 只有包含至少1个数据文件的文件夹才视为人物文件夹
        if folder_files:
            # 用文件夹名作为人物标识
            person_name = os.path.basename(root)
            person_folders[person_name] = folder_files

    return person_folders


class CLI:
    """命令行交互界面 - 纯调度层"""

    def __init__(self):
        self.app = Application()

    def start(self):
        """交互式菜单"""
        print("=" * 60)
        print("  多源数据分析系统 - 纪委初核分析助手")
        print("=" * 60)

        while True:
            # 自动扫描：先检测人物文件夹，再检测顶层文件
            person_folders = scan_person_folders()
            auto_paths = scan_data_dir() if not person_folders else {}

            # 显示识别结果
            if person_folders:
                print(f"\n[已识别 {len(person_folders)} 个人物文件夹]")
                for i, (name, files) in enumerate(person_folders.items(), 1):
                    file_types = '/'.join(files.keys())
                    print(f"  {i}. {name} ({file_types})")
            elif auto_paths:
                print(f"\n[已识别 data/ 目录下 {len(auto_paths)} 个数据文件]")
                for dtype, path in auto_paths.items():
                    print(f"  {dtype}: {os.path.basename(path)}")

            # 动态菜单
            print("\n请选择操作：")
            if person_folders:
                print("  1. 选择人员并批量分析（推荐）")
                print("  2. 全部人员批量分析")
                print("  3. 合并分析（所有人放一起分析，生成统一报告）")
                print("  4. 加载数据（手动指定文件）")
                print("  5. 执行分析")
                print("  6. 导出报告")
                print("  7. 一键运行（单人模式）")
                print("  8. 退出")
            else:
                print("  1. 加载数据")
                print("  2. 执行分析")
                print("  3. 导出报告")
                print("  4. 一键运行（加载→分析→导出）")
                print("  5. 退出")

            choice = input("\n请输入选项: ").strip()

            if person_folders:
                self._handle_batch_menu(choice, person_folders, auto_paths)
            else:
                self._handle_single_menu(choice, auto_paths)

    def _handle_batch_menu(self, choice: str, person_folders: dict, auto_paths: dict):
        """有人物文件夹时的菜单分发"""
        if choice == '1':
            self._select_persons_menu(person_folders)
        elif choice == '2':
            self._batch_run(person_folders)
        elif choice == '3':
            self._combined_run(person_folders)
        elif choice == '4':
            self._load_data_menu(auto_paths)
        elif choice == '5':
            self._analyze_menu()
        elif choice == '6':
            self._export_menu()
        elif choice == '7':
            self._run_all_menu(auto_paths)
        elif choice == '8':
            print("感谢使用，再见！")
            sys.exit(0)
        else:
            print("无效选项，请重新输入。")

    def _handle_single_menu(self, choice: str, auto_paths: dict):
        """无人物文件夹时的旧版菜单"""
        if choice == '1':
            self._load_data_menu(auto_paths)
        elif choice == '2':
            self._analyze_menu()
        elif choice == '3':
            self._export_menu()
        elif choice == '4':
            self._run_all_menu(auto_paths)
        elif choice == '5':
            print("感谢使用，再见！")
            sys.exit(0)
        else:
            print("无效选项，请重新输入。")

    def _load_data_menu(self, auto_paths: dict = None):
        """加载数据菜单"""
        print("\n--- 加载数据 ---")

        # 如果已有自动识别的数据，直接用
        if auto_paths:
            print(f"已自动识别 {len(auto_paths)} 个数据文件：")
            for dtype, path in auto_paths.items():
                print(f"  {dtype}: {os.path.basename(path)}")
            confirm = input("\n使用以上文件？(Y/n): ").strip().lower()
            if confirm != 'n':
                try:
                    self.app.load_data(auto_paths)
                    print("数据加载完成！")
                except Exception as e:
                    print(f"数据加载失败：{e}")
                return

        # 手动输入
        data_paths = self._manual_input_paths()
        if not data_paths:
            print("未指定任何数据文件。")
            return

        try:
            self.app.load_data(data_paths)
            print("数据加载完成！")
        except Exception as e:
            print(f"数据加载失败：{e}")

    def _select_persons_menu(self, person_folders: dict[str, dict[str, str]]):
        """选择人员并批量分析"""
        print("\n--- 选择人员 ---")
        persons = list(person_folders.keys())
        for i, name in enumerate(persons, 1):
            file_types = '/'.join(person_folders[name].keys())
            print(f"  {i}. {name} ({file_types})")

        print(f"\n输入方式：")
        print(f"  - 输入编号，多个用逗号分隔（如: 1,3,5）")
        print(f"  - 输入 a 全选")

        raw = input("\n请选择: ").strip().lower()

        if raw == 'a':
            selected = person_folders
        else:
            selected = {}
            try:
                indices = [int(x.strip()) for x in raw.split(',') if x.strip()]
                for idx in indices:
                    if 1 <= idx <= len(persons):
                        name = persons[idx - 1]
                        selected[name] = person_folders[name]
                    else:
                        print(f"  [跳过] 无效编号: {idx}")
            except ValueError:
                print("输入格式有误。")
                return

        if not selected:
            print("未选择任何人员。")
            return

        print(f"\n已选择 {len(selected)} 人: {', '.join(selected.keys())}")
        self._batch_run(selected)

    def _batch_run(self, person_folders: dict[str, dict[str, str]]):
        """批量运行分析"""
        print("\n--- 批量分析 ---")

        print("请选择导出格式：")
        print("  1. Excel + Word")
        print("  2. 仅Excel")
        print("  3. 仅Word")

        format_map = {'1': 'both', '2': 'excel', '3': 'word'}
        choice = input("请输入选项 (1-3): ").strip()
        export_format = format_map.get(choice, 'both')

        print(f"\n开始批量处理 {len(person_folders)} 人...")
        results = self.app.run_batch(person_folders, export_format=export_format)

        # 汇总结果
        print("\n" + "=" * 50)
        print("  批量处理完成 - 汇总")
        print("=" * 50)
        success_count = 0
        for name, paths in results.items():
            if 'error' in paths:
                print(f"  [失败] {name}: {paths['error']}")
            else:
                success_count += 1
                fmt_list = [f"{k}({os.path.basename(v)})" for k, v in paths.items()]
                print(f"  [完成] {name}: {', '.join(fmt_list)}")

        print(f"\n成功: {success_count}/{len(person_folders)}")

    def _combined_run(self, person_folders: dict[str, dict[str, str]]):
        """合并分析 - 所有人放一起分析，生成统一报告"""
        print("\n--- 合并分析 ---")
        print(f"将 {len(person_folders)} 人的数据合并在一起分析，生成统一报告。")
        print(f"涉及人员: {', '.join(person_folders.keys())}")

        confirm = input("\n确认执行合并分析？(Y/n): ").strip().lower()
        if confirm == 'n':
            print("已取消。")
            return

        print("请选择导出格式：")
        print("  1. Excel + Word")
        print("  2. 仅Excel")
        print("  3. 仅Word")

        format_map = {'1': 'both', '2': 'excel', '3': 'word'}
        choice = input("请输入选项 (1-3): ").strip()
        export_format = format_map.get(choice, 'both')

        try:
            print(f"\n开始合并分析 {len(person_folders)} 人...")
            paths = self.app.run_combined(person_folders, export_format=export_format)

            print("\n===== 合并分析完成 =====")
            for fmt, path in paths.items():
                size = os.path.getsize(path) if os.path.exists(path) else 0
                print(f"  {fmt}: {path} ({size:,} bytes)")
        except Exception as e:
            print(f"合并分析失败：{e}")

    def _analyze_menu(self):
        """执行分析菜单"""
        print("\n--- 执行分析 ---")
        if not self.app._state.get('data_loaded'):
            print("请先加载数据！")
            return

        print("请选择分析类型：")
        print("  1. 全部分析")
        print("  2. 频率分析")
        print("  3. 存取现识别")
        print("  4. 特殊分析")
        print("  5. 综合交叉分析")
        print("  6. 重点收支")
        print("  7. 大额资金追踪")
        print("  8. 高级分析")

        type_map = {
            '1': 'all', '2': 'frequency', '3': 'cash',
            '4': 'special', '5': 'cross', '6': 'key_transaction',
            '7': 'fund_tracking', '8': 'advanced'
        }

        choice = input("请输入选项 (1-8): ").strip()
        analysis_type = type_map.get(choice, 'all')

        try:
            result = self.app.analyze(analysis_type)
            print(f"分析完成！共分析 {len(result.persons)} 人")
        except Exception as e:
            print(f"分析失败：{e}")

    def _export_menu(self):
        """导出报告菜单"""
        print("\n--- 导出报告 ---")
        if not self.app._state.get('analysis_result'):
            print("请先执行分析！")
            return

        print("请选择导出格式：")
        print("  1. Excel + Word（同时导出）")
        print("  2. 仅Excel")
        print("  3. 仅Word")

        format_map = {'1': 'both', '2': 'excel', '3': 'word'}
        choice = input("请输入选项 (1-3): ").strip()
        export_format = format_map.get(choice, 'both')

        try:
            paths = self.app.export(export_format)
            print("报告导出完成！")
            for fmt, path in paths.items():
                print(f"  {fmt}: {path}")
        except Exception as e:
            print(f"导出失败：{e}")

    def _run_all_menu(self, auto_paths: dict = None):
        """一键运行菜单"""
        print("\n--- 一键运行 ---")

        # 自动识别数据
        data_paths = auto_paths or {}

        if data_paths:
            print(f"已自动识别 {len(data_paths)} 个数据文件：")
            for dtype, path in data_paths.items():
                print(f"  {dtype}: {os.path.basename(path)}")
            confirm = input("使用以上文件直接运行？(Y/n): ").strip().lower()
            if confirm == 'n':
                data_paths = self._manual_input_paths()
        else:
            data_paths = self._manual_input_paths()

        if not data_paths:
            print("未指定任何数据文件。")
            return

        print("\n请选择导出格式：")
        print("  1. Excel + Word")
        print("  2. 仅Excel")
        print("  3. 仅Word")

        format_map = {'1': 'both', '2': 'excel', '3': 'word'}
        choice = input("请输入选项 (1-3): ").strip()
        export_format = format_map.get(choice, 'both')

        try:
            paths = self.app.run(data_paths, export_format=export_format)
            print("\n===== 全部完成 =====")
            for fmt, path in paths.items():
                print(f"  {fmt}: {path}")
        except Exception as e:
            print(f"运行失败：{e}")

    def _manual_input_paths(self) -> dict[str, str]:
        """手动输入数据路径"""
        print("请输入数据文件路径（留空跳过）：")
        data_paths = {}

        bank_path = input("  银行数据文件路径: ").strip()
        if bank_path:
            data_paths['bank'] = bank_path

        wechat_path = input("  微信数据文件路径: ").strip()
        if wechat_path:
            data_paths['wechat'] = wechat_path

        alipay_path = input("  支付宝数据文件路径: ").strip()
        if alipay_path:
            data_paths['alipay'] = alipay_path

        call_path = input("  话单数据文件路径: ").strip()
        if call_path:
            data_paths['call'] = call_path

        return data_paths
