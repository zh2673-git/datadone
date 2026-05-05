"""特殊分析器 - 特殊日期/金额/整数金额识别"""

import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import date

from src.models.analysis_result import SpecialDateItem, SpecialAmountItem, SpecialAnalysisOutput


class SpecialAnalyzer:
    """特殊分析器"""

    def __init__(self, keywords, thresholds):
        self.keywords = keywords
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(self, all_data: dict[str, pd.DataFrame]) -> SpecialAnalysisOutput:
        """
        执行特殊日期和特殊金额分析
        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ..., '话单': df}
        Returns:
            SpecialAnalysisOutput
        """
        dates_result: dict[str, list[SpecialDateItem]] = {}
        amounts_result: dict[str, list[SpecialAmountItem]] = {}

        # 收集所有人员的 DataFrame
        all_persons_data = self._collect_person_data(all_data)

        for person, df in all_persons_data.items():
            if df is None or df.empty:
                continue

            # 特殊日期
            date_items = self._analyze_special_dates(df, person)
            if date_items:
                dates_result[person] = date_items

            # 特殊金额
            amount_items = self._analyze_special_amounts(df, person)
            if amount_items:
                amounts_result[person] = amount_items

        return SpecialAnalysisOutput(dates=dates_result, amounts=amounts_result)

    # ------------------------------------------------------------------
    # 收集所有人员数据
    # ------------------------------------------------------------------
    def _collect_person_data(self, all_data: dict) -> dict[str, pd.DataFrame]:
        """将所有平台的按人数据合并为 {本方姓名: DataFrame}"""
        person_data: dict[str, list[pd.DataFrame]] = {}

        for platform in ["银行", "微信", "支付宝"]:
            platform_data = all_data.get(platform)
            if platform_data is None:
                continue
            if isinstance(platform_data, dict):
                for person, df in platform_data.items():
                    if df is not None and not df.empty:
                        if "平台" not in df.columns:
                            df = df.copy()
                            df["平台"] = platform
                        person_data.setdefault(person, []).append(df)
            elif isinstance(platform_data, pd.DataFrame) and not platform_data.empty:
                for person, group in platform_data.groupby("本方姓名"):
                    group = group.copy()
                    if "平台" not in group.columns:
                        group["平台"] = platform
                    person_data.setdefault(str(person), []).append(group)

        result = {}
        for person, dfs in person_data.items():
            result[person] = pd.concat(dfs, ignore_index=True)

        return result

    # ------------------------------------------------------------------
    # 特殊日期分析
    # ------------------------------------------------------------------
    def _analyze_special_dates(self, df: pd.DataFrame, person: str) -> list[SpecialDateItem]:
        """分析特殊日期"""
        date_mapping = self._build_date_mapping()
        if not date_mapping:
            return []

        if "交易日期" not in df.columns:
            return []

        items = []
        for idx, row in df.iterrows():
            try:
                tx_date = pd.to_datetime(row["交易日期"], errors="coerce")
                if pd.isna(tx_date):
                    continue
                tx_date_val = tx_date.date()
                if tx_date_val in date_mapping:
                    items.append(SpecialDateItem(
                        index=int(idx) if isinstance(idx, int) else len(items),
                        交易日期=tx_date_val,
                        特殊日期名称=date_mapping[tx_date_val],
                    ))
            except Exception:
                continue

        return items

    def _build_date_mapping(self) -> dict[date, str]:
        """构建 日期→节日名称 的映射"""
        import datetime
        current_year = datetime.datetime.now().year
        years = list(range(current_year - 25, current_year + 2))

        special_dates_config = self.thresholds.get("analysis.special_date.dates", {})
        if not special_dates_config:
            special_dates_config = self.thresholds.get_special_dates()

        try:
            from src.utils.date_utils import build_special_date_mapping
            return build_special_date_mapping(years, special_dates_config)
        except Exception:
            pass

        # fallback: 手动构建
        mapping: dict[date, str] = {}
        special_dates_config = self.thresholds.get("analysis.special_date.dates", {})

        if not special_dates_config:
            # 默认配置
            special_dates_config = {
                "元旦": {"type": "solar", "month": 1, "day": 1},
                "春节": {"type": "lunar", "month": 1, "day": 1},
                "元宵节": {"type": "lunar", "month": 1, "day": 15},
                "情人节": {"type": "solar", "month": 2, "day": 14},
                "妇女节": {"type": "solar", "month": 3, "day": 8},
                "清明节": {"type": "solar", "month": 4, "day": 5},
                "劳动节": {"type": "solar", "month": 5, "day": 1},
                "端午节": {"type": "lunar", "month": 5, "day": 5},
                "儿童节": {"type": "solar", "month": 6, "day": 1},
                "七夕": {"type": "lunar", "month": 7, "day": 7},
                "建军节": {"type": "solar", "month": 8, "day": 1},
                "中秋节": {"type": "lunar", "month": 8, "day": 15},
                "教师节": {"type": "solar", "month": 9, "day": 10},
                "国庆节": {"type": "solar", "month": 10, "day": 1},
                "重阳节": {"type": "lunar", "month": 9, "day": 9},
                "冬至": {"type": "solar", "month": 12, "day": 22},
                "平安夜": {"type": "solar", "month": 12, "day": 24},
                "圣诞节": {"type": "solar", "month": 12, "day": 25},
            }

        import datetime
        current_year = datetime.datetime.now().year
        years = list(range(current_year - 5, current_year + 2))

        for name, cfg in special_dates_config.items():
            dtype = cfg.get("type", "solar")
            month = cfg.get("month", 1)
            day = cfg.get("day", 1)

            for year in years:
                try:
                    if dtype == "solar":
                        d = date(year, month, day)
                        mapping[d] = name
                    elif dtype == "lunar":
                        try:
                            from zhdate import ZhDate
                            lunar = ZhDate(year, month, day)
                            solar = lunar.to_datetime()
                            mapping[solar.date()] = name
                        except Exception:
                            pass
                except Exception:
                    pass

        return mapping

    # ------------------------------------------------------------------
    # 特殊金额分析
    # ------------------------------------------------------------------
    def _analyze_special_amounts(self, df: pd.DataFrame, person: str) -> list[SpecialAmountItem]:
        """分析特殊金额"""
        if "交易金额" not in df.columns:
            return []

        # 加载特殊金额配置
        special_amounts = self.thresholds.get("analysis.special_amount.amounts", [])
        if not special_amounts:
            special_amounts = self._default_special_amounts()

        love_amounts = {520, 521, 1314, 13140, 131400, 5200, 52000, 520000, 52.0, 13.14}

        items = []
        for idx, row in df.iterrows():
            try:
                amount = abs(float(row["交易金额"]))
                if amount in special_amounts or amount in love_amounts:
                    # 判定类型
                    if amount in love_amounts:
                        special_type = "爱情数字"
                    else:
                        special_type = "其他特殊金额"

                    items.append(SpecialAmountItem(
                        index=int(idx) if isinstance(idx, int) else len(items),
                        交易金额=amount,
                        特殊类型=special_type,
                        特殊金额名=str(int(amount)) if amount == int(amount) else str(amount),
                    ))
            except (ValueError, TypeError):
                continue

        return items

    @staticmethod
    def _default_special_amounts() -> set:
        return {
            520, 521, 1314, 13140, 131400, 5200, 52000, 520000, 52.0, 13.14,
            6.66, 8.88, 66, 88, 99, 166, 188, 200, 288, 366, 388, 588,
            666, 688, 777, 888, 999, 1688, 1888, 2888, 3888, 5888, 6666,
            8888, 9999, 16888, 18888, 28888, 38888, 58888, 66666, 88888,
            99999, 168888, 188888, 288888, 388888, 588888, 666666, 888888,
            999999,
        }
