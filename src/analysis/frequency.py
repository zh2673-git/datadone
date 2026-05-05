"""频率分析器 - 账单类和话单类频率聚合"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional

from src.models.analysis_result import FrequencyItem, CallFrequencyItem


class FrequencyAnalyzer:
    """频率分析器"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 账单类频率分析
    # ------------------------------------------------------------------
    def analyze(self, all_data: dict[str, pd.DataFrame]) -> dict[str, list[FrequencyItem]]:
        """
        账单类频率分析
        Args:
            all_data: {'银行': {本方姓名: df}, '微信': {本方姓名: df}, '支付宝': {本方姓名: df}}
        Returns:
            {平台: [FrequencyItem]}
        """
        result: dict[str, list[FrequencyItem]] = {}

        platform_map = {
            "银行": "银行",
            "微信": "微信",
            "支付宝": "支付宝",
        }

        for platform_key, platform_name in platform_map.items():
            platform_data = all_data.get(platform_key)
            if platform_data is None:
                continue
            items: list[FrequencyItem] = []
            # platform_data 可能是 Dict[str, DataFrame] 或 单个 DataFrame
            if isinstance(platform_data, dict):
                for person, df in platform_data.items():
                    if df is None or df.empty:
                        continue
                    items.extend(self._analyze_bill_df(df, platform_name))
            elif isinstance(platform_data, pd.DataFrame):
                if not platform_data.empty:
                    items.extend(self._analyze_bill_df(platform_data, platform_name))
            if items:
                result[platform_name] = items

        return result

    def _analyze_bill_df(self, df: pd.DataFrame, platform: str) -> list[FrequencyItem]:
        """对单个 DataFrame 执行频率分析"""
        # 只统计转账记录
        if "存取现标识" in df.columns:
            df = df[df["存取现标识"] == "转账"].copy()
        if df.empty:
            return []

        # 确保有必要的列
        required = ["本方姓名"]
        for col in required:
            if col not in df.columns:
                self.logger.warning(f"频率分析缺少列 {col}，跳过平台 {platform}")
                return []

        # 计算收入/支出金额（如果列存在但全为0，也需要重新计算）
        need_calc = "收入金额" not in df.columns or df["收入金额"].sum() == 0 and df["支出金额"].sum() == 0
        if need_calc:
            df = self._calc_income_expense(df, platform)

        # 分组（对方姓名为NaN时填充为空字符串以便分组）
        if "对方姓名" not in df.columns:
            df["对方姓名"] = ""
        df["对方姓名"] = df["对方姓名"].fillna("")
        grouped = df.groupby(["本方姓名", "对方姓名"])

        items = []
        for (self_name, opp_name), group in grouped:
            income_total = group["收入金额"].sum()
            expense_total = group["支出金额"].sum()

            # 交易时间跨度
            date_col = group.get("交易日期")
            span = None
            if date_col is not None:
                try:
                    dates = pd.to_datetime(date_col, errors="coerce").dropna()
                    if len(dates) >= 2:
                        span = (dates.max() - dates.min()).days + 1
                    elif len(dates) == 1:
                        span = 1
                except Exception:
                    pass

            # 数据来源
            data_source = ""
            if "数据来源" in group.columns:
                ds_vals = group["数据来源"].dropna().unique()
                if len(ds_vals) > 0:
                    data_source = str(ds_vals[0])

            items.append(FrequencyItem(
                platform=platform,
                data_source=data_source,
                本方姓名=str(self_name),
                对方姓名=str(opp_name),
                收入总额=float(income_total),
                支出总额=float(expense_total),
                交易次数=int(len(group)),
                交易总金额=float(income_total + expense_total),
                交易时间跨度=span,
            ))

        # 排序：本方姓名 ASC, 交易总金额 DESC
        items.sort(key=lambda x: (x.本方姓名, -x.交易总金额))
        return items

    @staticmethod
    def _calc_income_expense(df: pd.DataFrame, platform: str) -> pd.DataFrame:
        """根据平台计算收入/支出金额"""
        df = df.copy()
        df["收入金额"] = 0.0
        df["支出金额"] = 0.0
        abs_amount = df["交易金额"].abs() if "交易金额" in df.columns else pd.Series([0.0] * len(df), index=df.index)

        if platform == "银行":
            if "借贷标识" in df.columns:
                credit = df["借贷标识"] == "贷"
                debit = df["借贷标识"] == "借"
                df.loc[credit, "收入金额"] = abs_amount[credit]
                df.loc[debit, "支出金额"] = abs_amount[debit]
            elif "收入金额" in df.columns and "支出金额" in df.columns:
                pass  # 已经有了
        elif platform == "微信":
            if "借贷标识" in df.columns:
                income = df["借贷标识"] == "入"
                expense = df["借贷标识"] == "出"
                df.loc[income, "收入金额"] = abs_amount[income]
                df.loc[expense, "支出金额"] = abs_amount[expense]
        elif platform == "支付宝":
            if "借贷标识" in df.columns:
                income = df["借贷标识"] == "收入"
                expense = df["借贷标识"] == "支出"
                df.loc[income, "收入金额"] = abs_amount[income]
                df.loc[expense, "支出金额"] = abs_amount[expense]

        return df

    # ------------------------------------------------------------------
    # 话单频率分析
    # ------------------------------------------------------------------
    def analyze_calls(self, call_data: pd.DataFrame) -> list[CallFrequencyItem]:
        """
        话单类频率分析
        Args:
            call_data: 标准化话单 DataFrame
        Returns:
            List[CallFrequencyItem]
        """
        if call_data is None or call_data.empty:
            return []

        df = call_data.copy()
        required = ["本方姓名", "对方姓名", "对方号码"]
        for col in required:
            if col not in df.columns:
                self.logger.warning(f"话单频率分析缺少列 {col}")
                return []

        # 通话时长列
        duration_col = "通话时长" if "通话时长" in df.columns else None
        # 呼叫类型列
        call_type_col = "呼叫类型" if "呼叫类型" in df.columns else None

        grouped = df.groupby(["本方姓名", "对方姓名", "对方号码"])

        # 全表通话次数
        total_calls = len(df)

        # 获取特殊日期映射用于计算特殊时间通话次数
        special_date_map = self._get_special_date_map()

        items = []
        for (self_name, opp_name, opp_number), group in grouped:
            call_count = len(group)

            # 主叫/被叫
            main_call = 0
            called = 0
            if call_type_col:
                main_call = int((group[call_type_col] == "主叫").sum())
                called = int((group[call_type_col] == "被叫").sum())

            # 通话时长
            total_seconds = 0.0
            if duration_col:
                total_seconds = float(group[duration_col].fillna(0).sum())
            total_minutes = total_seconds / 60.0 if total_seconds else 0.0

            # 通话占比
            call_pct = f"{call_count / total_calls * 100:.2f}%" if total_calls > 0 else ""

            # 对方单位/职务
            opp_unit = ""
            opp_title = ""
            if "对方单位名称" in group.columns:
                vals = group["对方单位名称"].dropna().unique()
                if len(vals) > 0:
                    opp_unit = str(vals[0])
            if "对方职务" in group.columns:
                vals = group["对方职务"].dropna().unique()
                if len(vals) > 0:
                    opp_title = str(vals[0])

            # 数据来源
            data_source = ""
            if "数据来源" in group.columns:
                ds_vals = group["数据来源"].dropna().unique()
                if len(ds_vals) > 0:
                    data_source = str(ds_vals[0])

            # 特殊时间次数
            special_count = 0
            if special_date_map and "呼叫日期" in group.columns:
                for _, row in group.iterrows():
                    try:
                        call_date = pd.to_datetime(row["呼叫日期"], errors="coerce").date()
                        if call_date and call_date in special_date_map:
                            special_count += 1
                    except Exception:
                        pass

            items.append(CallFrequencyItem(
                platform="话单",
                data_source=data_source,
                本方姓名=str(self_name),
                对方姓名=str(opp_name),
                对方号码=str(opp_number) if opp_number else "",
                对方单位名称=opp_unit or None,
                对方职务=opp_title or None,
                通话次数=call_count,
                主叫次数=main_call,
                被叫次数=called,
                通话总时长秒=total_seconds,
                通话总时长分钟=total_minutes,
                通话占比=call_pct or None,
                特殊时间次数=special_count,
            ))

        items.sort(key=lambda x: (x.本方姓名, -x.通话次数))
        return items

    # ------------------------------------------------------------------
    # 辅助：特殊日期映射
    # ------------------------------------------------------------------
    def _get_special_date_map(self) -> dict:
        """尝试使用 date_utils 构建特殊日期映射"""
        try:
            from src.utils.date_utils import build_special_date_mapping
            return build_special_date_mapping()
        except Exception:
            return {}
