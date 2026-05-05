"""大额资金追踪器 - 大额交易筛选+递归追踪+存取现话单匹配"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Set
from datetime import date, timedelta
from collections import defaultdict

from src.models.analysis_result import FundTrackingItem, FundTrackingOutput, CashRecognitionItem


class FundTracker:
    """大额资金追踪器"""

    def __init__(self, keywords, thresholds):
        self.keywords = keywords
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

        # 大额阈值
        raw_thresholds = self.thresholds.get_large_amount_thresholds()
        self.large_amount_thresholds = {}
        for i, item in enumerate(raw_thresholds, 1):
            self.large_amount_thresholds[f"level{i}"] = item
        self.min_large_amount = min(
            cfg["min"] for cfg in self.large_amount_thresholds.values()
        ) if self.large_amount_thresholds else 50000
        self.tracking_window_days = 30
        self.max_tracking_depth = 3

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(
        self,
        all_data: dict,
        cash_result: dict[str, list[CashRecognitionItem]],
        call_data: Optional[pd.DataFrame] = None,
    ) -> FundTrackingOutput:
        """
        执行大额资金追踪和存取现话单匹配
        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ...}
            cash_result: 存取现识别结果
            call_data: 话单 DataFrame
        Returns:
            FundTrackingOutput
        """
        # 收集所有交易
        all_transactions = self._collect_all_transactions(all_data)

        # 大额交易筛选
        large_transactions = self._filter_large_transactions(all_transactions)

        tracking_items: list[FundTrackingItem] = []

        if not large_transactions.empty:
            # 按月份和人员分组
            tracking_items = self._track_funds(large_transactions, all_transactions)

        # 存取现话单匹配
        cash_call_items = self._cash_call_match(cash_result, all_data.get("银行", {}), call_data)

        return FundTrackingOutput(
            tracking=tracking_items,
            cash_call_match=cash_call_items,
        )

    # ------------------------------------------------------------------
    # 收集所有交易
    # ------------------------------------------------------------------
    def _collect_all_transactions(self, all_data: dict) -> pd.DataFrame:
        dfs = []
        for platform in ["银行", "微信", "支付宝"]:
            platform_data = all_data.get(platform)
            if platform_data is None:
                continue
            if isinstance(platform_data, dict):
                for person, df in platform_data.items():
                    if df is not None and not df.empty:
                        d = df.copy()
                        d["平台"] = platform
                        dfs.append(d)
            elif isinstance(platform_data, pd.DataFrame) and not platform_data.empty:
                d = platform_data.copy()
                d["平台"] = platform
                dfs.append(d)

        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    # ------------------------------------------------------------------
    # 大额交易筛选
    # ------------------------------------------------------------------
    def _filter_large_transactions(self, all_transactions: pd.DataFrame) -> pd.DataFrame:
        if all_transactions.empty or "交易金额" not in all_transactions.columns:
            return pd.DataFrame()

        abs_amount = all_transactions["交易金额"].abs()
        mask = abs_amount >= self.min_large_amount
        return all_transactions[mask].copy()

    # ------------------------------------------------------------------
    # 追踪流程
    # ------------------------------------------------------------------
    def _track_funds(self, large_tx: pd.DataFrame, all_tx: pd.DataFrame) -> list[FundTrackingItem]:
        items: list[FundTrackingItem] = []

        # 确保 交易日期 列
        if "交易日期" not in large_tx.columns:
            return items

        large_tx["交易日期_parsed"] = pd.to_datetime(large_tx["交易日期"], errors="coerce")
        large_tx["月份"] = large_tx["交易日期_parsed"].dt.to_period("M")

        # 按人员+月份分组
        grouped = large_tx.groupby(["本方姓名", "月份"])

        for (person, month), group in grouped:
            visited: set[str] = set()
            # 月度汇总
            month_income = group[group["收入金额"] > 0]["收入金额"].sum() if "收入金额" in group.columns else 0
            month_expense = group[group["支出金额"] > 0]["支出金额"].sum() if "支出金额" in group.columns else 0

            # 来源/去向明细
            source_details = self._analyze_fund_sources(group)
            dest_details = self._analyze_fund_destinations(group)

            remark = self._generate_remark(str(person), month, month_income, month_expense, source_details, dest_details)
            level = self._get_amount_level(max(abs(month_income), abs(month_expense)))

            items.append(FundTrackingItem(
                分析类型="大额资金追踪",
                追踪层级=0,
                核心人员=str(person),
                关联人员="月度汇总",
                交易金额=month_income - month_expense,
                交易方向="汇总",
                大额级别=level,
                数据来源="月度统计",
                资金流向="月度汇总",
                追踪说明=remark,
                月份=str(month),
            ))

            # 单笔追踪
            for _, row in group.iterrows():
                direction = "收入" if row.get("收入金额", 0) > 0 else "支出"
                opp_name = str(row.get("对方姓名", "")) if pd.notna(row.get("对方姓名")) else ""
                amount = abs(float(row.get("交易金额", 0)))
                data_source = str(row.get("平台", ""))

                items.append(FundTrackingItem(
                    分析类型="大额资金追踪",
                    追踪层级=0,
                    核心人员=str(person),
                    关联人员=opp_name,
                    交易日期=row["交易日期_parsed"].date() if pd.notna(row["交易日期_parsed"]) else None,
                    交易金额=amount,
                    交易方向=direction,
                    大额级别=self._get_amount_level(amount),
                    数据来源=data_source,
                    资金流向="直接交易",
                    追踪说明=f"{person} {direction} {amount:,.0f}元给{opp_name}",
                    月份=str(month),
                ))

                # 间接追踪（支出方向）
                if direction == "支出" and opp_name:
                    indirect = self._track_indirect(
                        opp_name, row["交易日期_parsed"], all_tx, 1, visited | {str(person)}
                    )
                    items.extend(indirect)

        return items

    # ------------------------------------------------------------------
    # 间接递归追踪
    # ------------------------------------------------------------------
    def _track_indirect(
        self, person: str, base_date, all_tx: pd.DataFrame,
        depth: int, visited: set[str]
    ) -> list[FundTrackingItem]:
        if depth >= self.max_tracking_depth or person in visited:
            return []

        visited.add(person)

        if all_tx.empty or "本方姓名" not in all_tx.columns:
            return []

        # 时间窗口
        start_date = base_date - timedelta(days=self.tracking_window_days)
        end_date = base_date + timedelta(days=self.tracking_window_days)

        all_tx_copy = all_tx.copy()
        if "交易日期" not in all_tx_copy.columns:
            return []
        all_tx_copy["交易日期_parsed"] = pd.to_datetime(all_tx_copy["交易日期"], errors="coerce")

        # 找此人在时间窗口内的大额交易
        mask = (
            (all_tx_copy["本方姓名"] == person)
            & (all_tx_copy["交易日期_parsed"] >= start_date)
            & (all_tx_copy["交易日期_parsed"] <= end_date)
            & (all_tx_copy["交易金额"].abs() >= self.min_large_amount)
        )
        person_tx = all_tx_copy[mask]

        items: list[FundTrackingItem] = []
        for _, row in person_tx.iterrows():
            direction = "收入" if row.get("收入金额", 0) > 0 else "支出"
            opp_name = str(row.get("对方姓名", "")) if pd.notna(row.get("对方姓名")) else ""
            amount = abs(float(row.get("交易金额", 0)))
            data_source = str(row.get("平台", ""))

            items.append(FundTrackingItem(
                分析类型="大额资金追踪",
                追踪层级=depth,
                核心人员=str(person),
                关联人员=opp_name,
                交易日期=row["交易日期_parsed"].date() if pd.notna(row["交易日期_parsed"]) else None,
                交易金额=amount,
                交易方向=direction,
                大额级别=self._get_amount_level(amount),
                数据来源=data_source,
                资金流向="间接追踪",
                追踪说明=f"通过{person}间接追踪：{person} {direction} {amount:,.0f}元给{opp_name}",
            ))

            # 继续递归
            if direction == "支出" and opp_name:
                indirect = self._track_indirect(
                    opp_name, row["交易日期_parsed"], all_tx_copy, depth + 1, visited.copy()
                )
                items.extend(indirect)

        return items

    # ------------------------------------------------------------------
    # 大额级别判定
    # ------------------------------------------------------------------
    def _get_amount_level(self, amount: float) -> str:
        for cfg in self.large_amount_thresholds.values():
            if cfg["min"] <= abs(amount) < cfg["max"]:
                return cfg["name"]
        return ""

    # ------------------------------------------------------------------
    # 资金来源/去向分析
    # ------------------------------------------------------------------
    @staticmethod
    def _analyze_fund_sources(transactions: pd.DataFrame) -> dict:
        if "收入金额" not in transactions.columns:
            return {"现金收入": 0, "银行转账": {}, "微信转账": {}, "支付宝转账": {}, "其他收入": {}}

        income_tx = transactions[transactions["收入金额"] > 0]
        source_details = {
            "现金收入": 0,
            "银行转账": {},
            "微信转账": {},
            "支付宝转账": {},
            "其他收入": {},
        }

        for _, tx in income_tx.iterrows():
            amount = abs(float(tx.get("收入金额", 0)))
            opp = str(tx.get("对方姓名", "未知")) if pd.notna(tx.get("对方姓名")) else "未知"
            platform = str(tx.get("平台", ""))

            # 检查是否为存现
            if tx.get("存取现标识") == "存现":
                source_details["现金收入"] += amount
            elif "银行" in platform:
                source_details["银行转账"][opp] = source_details["银行转账"].get(opp, 0) + amount
            elif "微信" in platform:
                source_details["微信转账"][opp] = source_details["微信转账"].get(opp, 0) + amount
            elif "支付宝" in platform:
                source_details["支付宝转账"][opp] = source_details["支付宝转账"].get(opp, 0) + amount
            else:
                source_details["其他收入"][opp] = source_details["其他收入"].get(opp, 0) + amount

        return source_details

    @staticmethod
    def _analyze_fund_destinations(transactions: pd.DataFrame) -> dict:
        if "支出金额" not in transactions.columns:
            return {"现金支出": 0, "银行转账": {}, "微信转账": {}, "支付宝转账": {}, "其他支出": {}}

        expense_tx = transactions[transactions["支出金额"] > 0]
        dest_details = {
            "现金支出": 0,
            "银行转账": {},
            "微信转账": {},
            "支付宝转账": {},
            "其他支出": {},
        }

        for _, tx in expense_tx.iterrows():
            amount = abs(float(tx.get("支出金额", 0)))
            opp = str(tx.get("对方姓名", "未知")) if pd.notna(tx.get("对方姓名")) else "未知"
            platform = str(tx.get("平台", ""))

            if tx.get("存取现标识") == "取现":
                dest_details["现金支出"] += amount
            elif "银行" in platform:
                dest_details["银行转账"][opp] = dest_details["银行转账"].get(opp, 0) + amount
            elif "微信" in platform:
                dest_details["微信转账"][opp] = dest_details["微信转账"].get(opp, 0) + amount
            elif "支付宝" in platform:
                dest_details["支付宝转账"][opp] = dest_details["支付宝转账"].get(opp, 0) + amount
            else:
                dest_details["其他支出"][opp] = dest_details["其他支出"].get(opp, 0) + amount

        return dest_details

    # ------------------------------------------------------------------
    # 备注生成
    # ------------------------------------------------------------------
    @staticmethod
    def _generate_remark(person_name, month, income, expense, source_details, dest_details) -> str:
        parts = []
        net = income - expense

        if income > 0:
            income_parts = [f"总收入{income:,.0f}元"]
            if source_details["现金收入"] > 0:
                income_parts.append(f"现金收入{source_details['现金收入']:,.0f}元")
            for label, key in [("银行转账", "银行转账"), ("微信", "微信转账"), ("支付宝", "支付宝转账")]:
                if source_details[key]:
                    total = sum(source_details[key].values())
                    details = "、".join(f"{p}{label}{a:,.0f}元" for p, a in source_details[key].items())
                    income_parts.append(f"{label}收入{total:,.0f}元({details})")
            parts.append("资金来源：" + "；".join(income_parts))

        if expense > 0:
            expense_parts = [f"总支出{expense:,.0f}元"]
            if dest_details["现金支出"] > 0:
                expense_parts.append(f"现金支出{dest_details['现金支出']:,.0f}元")
            for label, key in [("银行转账", "银行转账"), ("微信", "微信转账"), ("支付宝", "支付宝转账")]:
                if dest_details[key]:
                    total = sum(dest_details[key].values())
                    details = "、".join(f"{p}{label}{a:,.0f}元" for p, a in dest_details[key].items())
                    expense_parts.append(f"{label}支出{total:,.0f}元({details})")
            parts.append("资金去向：" + "；".join(expense_parts))

        if net > 0:
            parts.append(f"净流入{net:,.0f}元")
        elif net < 0:
            parts.append(f"净流出{-net:,.0f}元")
        else:
            parts.append("收支平衡")

        month_str = str(month)
        return f"{person_name}{month_str}大额资金流向：" + "；".join(parts)

    # ------------------------------------------------------------------
    # 存取现话单匹配
    # ------------------------------------------------------------------
    def _cash_call_match(
        self,
        cash_result: dict[str, list[CashRecognitionItem]],
        bank_data: dict,
        call_data: Optional[pd.DataFrame],
    ) -> list[FundTrackingItem]:
        """存取现话单匹配"""
        if call_data is None or call_data.empty:
            return []

        items: list[FundTrackingItem] = []
        min_amount = 1000  # 存取现话单匹配最小金额

        # 构建话单按 日期+本方姓名 的索引
        if "呼叫日期" not in call_data.columns or "本方姓名" not in call_data.columns:
            return items

        call_data_copy = call_data.copy()
        call_data_copy["呼叫日期_parsed"] = pd.to_datetime(call_data_copy["呼叫日期"], errors="coerce")

        for person, cash_items in cash_result.items():
            for ci in cash_items:
                if ci.cash_type not in ("存现", "取现"):
                    continue

                # 获取银行数据中的行
                bank_df = bank_data.get(person) if isinstance(bank_data, dict) else None
                if bank_df is None:
                    continue

                # 找到对应的银行交易记录
                try:
                    row = bank_df.loc[ci.index]
                except (KeyError, IndexError):
                    continue

                # 检查金额
                amount = abs(float(row.get("交易金额", 0))) if "交易金额" in row.index else 0
                if amount < min_amount:
                    continue

                # 获取交易日期
                try:
                    tx_date = pd.to_datetime(row.get("交易日期"), errors="coerce")
                    if pd.isna(tx_date):
                        continue
                except Exception:
                    continue

                # 查找同一天的通话记录
                same_day_calls = call_data_copy[
                    (call_data_copy["本方姓名"] == person)
                    & (call_data_copy["呼叫日期_parsed"].dt.date == tx_date.date())
                ]

                if same_day_calls.empty:
                    continue

                # 合并通话信息
                call_opponents = []
                for _, call_row in same_day_calls.iterrows():
                    opp_name = str(call_row.get("对方姓名", "")) if pd.notna(call_row.get("对方姓名")) else ""
                    opp_unit = str(call_row.get("对方单位名称", "")) if pd.notna(call_row.get("对方单位名称")) else ""
                    call_opponents.append((opp_name, opp_unit))

                # 格式化
                opp_names_str = ",".join(set(name for name, _ in call_opponents if name))
                opp_units_str = ",".join(set(unit for _, unit in call_opponents if unit and unit != "None"))

                direction = "收入" if ci.cash_type == "存现" else "支出"

                items.append(FundTrackingItem(
                    分析类型="存取现话单匹配",
                    追踪层级=0,
                    核心人员=str(person),
                    关联人员=opp_names_str,
                    交易日期=tx_date.date(),
                    交易金额=amount,
                    交易方向=direction,
                    大额级别=self._get_amount_level(amount),
                    数据来源="银行",
                    资金流向="存取现话单匹配",
                    追踪说明=f"{person}{ci.cash_type}{amount:,.0f}元，当日与{opp_names_str}通话",
                    通话日期=tx_date.date(),
                    通话对方=opp_names_str or None,
                    通话对方单位=opp_units_str or None,
                ))

        return items
