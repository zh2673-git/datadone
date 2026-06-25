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
        """对单个 DataFrame 执行频率分析（向量化版本）"""
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
        need_calc = "收入金额" not in df.columns or (df["收入金额"].sum() == 0 and df["支出金额"].sum() == 0)
        if need_calc:
            df = self._calc_income_expense(df, platform)

        # 分组（对方姓名为NaN时填充为空字符串以便分组）
        if "对方姓名" not in df.columns:
            df["对方姓名"] = ""
        df["对方姓名"] = df["对方姓名"].fillna("")

        # 一次性把交易日期转 datetime（避免在 groupby 循环里反复转换）
        if "交易日期" in df.columns:
            df["_交易日期_dt"] = pd.to_datetime(df["交易日期"], errors="coerce")
        else:
            df["_交易日期_dt"] = pd.NaT

        # 数据来源列保证存在
        if "数据来源" not in df.columns:
            df["数据来源"] = ""

        # 一次性聚合：把循环里的多次 sum/unique 改为单次 agg
        grouped = df.groupby(["本方姓名", "对方姓名"], observed=True)
        agg_df = grouped.agg(
            收入总额=("收入金额", "sum"),
            支出总额=("支出金额", "sum"),
            交易次数=("收入金额", "size"),
            日期最小=("_交易日期_dt", "min"),
            日期最大=("_交易日期_dt", "max"),
            数据来源=("数据来源", "first"),
        ).reset_index()

        items = []
        for _, row in agg_df.iterrows():
            income_total = float(row["收入总额"])
            expense_total = float(row["支出总额"])

            # 交易时间跨度
            span = None
            d_min = row["日期最小"]
            d_max = row["日期最大"]
            if pd.notna(d_min) and pd.notna(d_max):
                span = (d_max - d_min).days + 1
            elif pd.notna(d_min):
                span = 1

            data_source = row["数据来源"]
            if pd.isna(data_source):
                data_source = ""

            items.append(FrequencyItem(
                platform=platform,
                data_source=str(data_source),
                本方姓名=str(row["本方姓名"]),
                对方姓名=str(row["对方姓名"]),
                收入总额=income_total,
                支出总额=expense_total,
                交易次数=int(row["交易次数"]),
                交易总金额=income_total + expense_total,
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
        话单类频率分析（向量化版本）
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

        duration_col = "通话时长" if "通话时长" in df.columns else None
        call_type_col = "呼叫类型" if "呼叫类型" in df.columns else None

        # 一次性把呼叫日期转 date，用于特殊时间检测
        special_date_map = self._get_special_date_map()
        special_dates_set = set(special_date_map.keys()) if special_date_map else set()
        if special_dates_set and "呼叫日期" in df.columns:
            call_dates = pd.to_datetime(df["呼叫日期"], errors="coerce").dt.date
            df["_特殊时间"] = call_dates.isin(special_dates_set).astype(int)
        else:
            df["_特殊时间"] = 0

        # 预计算主叫/被叫掩码列
        if call_type_col:
            df["_主叫"] = (df[call_type_col] == "主叫").astype(int)
            df["_被叫"] = (df[call_type_col] == "被叫").astype(int)
        else:
            df["_主叫"] = 0
            df["_被叫"] = 0

        # 通话时长数值化
        if duration_col:
            df["_通话时长"] = pd.to_numeric(df[duration_col], errors="coerce").fillna(0)
        else:
            df["_通话时长"] = 0.0

        # 数据来源列保证存在
        if "数据来源" not in df.columns:
            df["数据来源"] = ""

        total_calls = len(df)

        # 一次性聚合：避免 groupby 循环内多次 sum/unique
        grouped = df.groupby(["本方姓名", "对方姓名", "对方号码"], observed=True)
        agg_df = grouped.agg(
            通话次数=("_主叫", "size"),
            主叫次数=("_主叫", "sum"),
            被叫次数=("_被叫", "sum"),
            通话总时长秒=("_通话时长", "sum"),
            对方单位名称=("对方单位名称", "first"),
            对方职务=("对方职务", "first"),
            数据来源=("数据来源", "first"),
            特殊时间次数=("_特殊时间", "sum"),
        ).reset_index()

        items = []
        for _, row in agg_df.iterrows():
            call_count = int(row["通话次数"])
            total_seconds = float(row["通话总时长秒"])

            opp_unit = row["对方单位名称"]
            opp_title = row["对方职务"]
            data_source = row["数据来源"]

            items.append(CallFrequencyItem(
                platform="话单",
                data_source=str(data_source) if pd.notna(data_source) and data_source else "",
                本方姓名=str(row["本方姓名"]),
                对方姓名=str(row["对方姓名"]),
                对方号码=str(row["对方号码"]) if pd.notna(row["对方号码"]) else "",
                对方单位名称=str(opp_unit) if pd.notna(opp_unit) and opp_unit else None,
                对方职务=str(opp_title) if pd.notna(opp_title) and opp_title else None,
                通话次数=call_count,
                主叫次数=int(row["主叫次数"]),
                被叫次数=int(row["被叫次数"]),
                通话总时长秒=total_seconds,
                通话总时长分钟=total_seconds / 60.0 if total_seconds else 0.0,
                通话占比=f"{call_count / total_calls * 100:.2f}%" if total_calls > 0 else None,
                特殊时间次数=int(row["特殊时间次数"]),
            ))

        items.sort(key=lambda x: (x.本方姓名, -x.通话次数))
        return items

    # ------------------------------------------------------------------
    # 辅助：特殊日期映射
    # ------------------------------------------------------------------
    def _get_special_date_map(self) -> dict:
        """构建特殊日期→节日名映射（修复参数缺失 bug）"""
        try:
            import datetime
            from src.utils.date_utils import build_special_date_mapping
            current_year = datetime.datetime.now().year
            years = list(range(current_year - 5, current_year + 2))

            # 默认特殊日期配置（公历）
            default_special_dates = {
                "元旦": {"type": "solar", "month": 1, "day": 1},
                "情人节": {"type": "solar", "month": 2, "day": 14},
                "妇女节": {"type": "solar", "month": 3, "day": 8},
                "清明节": {"type": "solar", "month": 4, "day": 5},
                "劳动节": {"type": "solar", "month": 5, "day": 1},
                "儿童节": {"type": "solar", "month": 6, "day": 1},
                "建军节": {"type": "solar", "month": 8, "day": 1},
                "教师节": {"type": "solar", "month": 9, "day": 10},
                "国庆节": {"type": "solar", "month": 10, "day": 1},
                "冬至": {"type": "solar", "month": 12, "day": 22},
                "平安夜": {"type": "solar", "month": 12, "day": 24},
                "圣诞节": {"type": "solar", "month": 12, "day": 25},
            }
            # 农历节日（依赖 zhdate，失败则忽略）
            for name, m, d in [("春节", 1, 1), ("元宵节", 1, 15), ("端午节", 5, 5),
                               ("七夕", 7, 7), ("中秋节", 8, 15), ("重阳节", 9, 9)]:
                default_special_dates[name] = {"type": "lunar", "month": m, "day": d}

            return build_special_date_mapping(years, default_special_dates)
        except Exception as e:
            self.logger.debug(f"构建特殊日期映射失败: {e}")
            return {}
