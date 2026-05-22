"""习惯兴趣分析器 - 关键词匹配+商户识别+频率统计+时间模式"""

import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import date

from src.models.analysis_result import HabitInterestItem


class HabitInterestAnalyzer:
    """习惯兴趣分析器 - 方案B：关键词+统计特征"""

    def __init__(self, keywords, thresholds):
        self.keywords = keywords
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    def analyze(self, all_data: dict[str, pd.DataFrame]) -> dict[str, list[HabitInterestItem]]:
        """
        分析习惯和兴趣

        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ...}
        Returns:
            {本方姓名: [HabitInterestItem]}
        """
        result: dict[str, list[HabitInterestItem]] = {}
        all_persons_data = self._collect_person_data(all_data)

        for person, df in all_persons_data.items():
            if df is None or df.empty:
                continue
            items = self._analyze_person(df, person)
            if items:
                result[person] = items

        return result

    def _collect_person_data(self, all_data: dict) -> dict[str, pd.DataFrame]:
        person_data: dict[str, list[pd.DataFrame]] = {}
        for platform in ["银行", "微信", "支付宝"]:
            platform_data = all_data.get(platform)
            if platform_data is None:
                continue
            if isinstance(platform_data, dict):
                for person, df in platform_data.items():
                    if df is not None and not df.empty:
                        person_data.setdefault(person, []).append(df)
            elif isinstance(platform_data, pd.DataFrame) and not platform_data.empty:
                for person, group in platform_data.groupby("本方姓名"):
                    person_data.setdefault(str(person), []).append(group)

        merged = {}
        for person, dfs in person_data.items():
            merged[person] = pd.concat(dfs, ignore_index=True)
        return merged

    _B2B_SUFFIXES = [
        "有限公司", "有限责任公司", "股份有限公司", "集团", "公司", "企业",
        "合伙", "工作室", "事务所", "研究所", "研究院", "协会", "基金会",
        "合作社", "经营部", "经销部", "服务部", "营业部", "分公司",
    ]

    def _analyze_person(self, df: pd.DataFrame, person: str) -> list[HabitInterestItem]:
        data = df.copy()

        text_parts = []
        for col in ["交易摘要", "交易备注", "交易类型", "对方姓名"]:
            if col in data.columns:
                text_parts.append(data[col].fillna("").astype(str))
            else:
                text_parts.append(pd.Series([""] * len(data), index=data.index))
        data["匹配文本"] = text_parts[0]
        for part in text_parts[1:]:
            data["匹配文本"] = data["匹配文本"] + " " + part

        if "交易日期" in data.columns:
            data["交易日期_dt"] = pd.to_datetime(data["交易日期"], errors="coerce")
        else:
            data["交易日期_dt"] = pd.NaT

        if "交易时间" in data.columns:
            data["交易小时"] = pd.to_numeric(
                data["交易时间"].astype(str).str[:2], errors="coerce"
            )
        else:
            data["交易小时"] = pd.NA

        if "对方姓名" in data.columns:
            data["_is_b2b"] = data["对方姓名"].fillna("").astype(str).apply(self._is_b2b_counterparty)
        else:
            data["_is_b2b"] = False

        categories = self._load_categories()

        category_hits: dict[str, dict] = {}
        for cat_key, cat_cfg in categories.items():
            label = cat_cfg.get("label", cat_key)
            keywords = cat_cfg.get("keywords", [])
            merchant_patterns = cat_cfg.get("merchant_patterns", [])

            kw_mask = pd.Series([False] * len(data), index=data.index)
            matched_kw_col = pd.Series([""] * len(data), index=data.index)

            if keywords:
                pattern = "|".join(keywords)
                mask = data["匹配文本"].str.contains(pattern, case=False, na=False)
                for idx in data.index[mask]:
                    text = str(data.loc[idx, "匹配文本"]).lower()
                    for kw in keywords:
                        if kw.lower() in text:
                            matched_kw_col[idx] = kw
                            break
                kw_mask = kw_mask | mask

            merchant_mask = pd.Series([False] * len(data), index=data.index)
            if merchant_patterns and "对方姓名" in data.columns:
                opp_name = data["对方姓名"].fillna("").astype(str)
                mp_pattern = "|".join(merchant_patterns)
                merchant_mask = opp_name.str.contains(mp_pattern, case=False, na=False)
                for idx in data.index[merchant_mask & ~kw_mask]:
                    text = str(data.loc[idx, "对方姓名"]).lower()
                    for mp in merchant_patterns:
                        if mp.lower() in text:
                            matched_kw_col[idx] = mp
                            break

            final_mask = kw_mask | merchant_mask
            if not final_mask.any():
                continue

            hit_data = data[final_mask].copy()
            hit_data["匹配关键词"] = matched_kw_col[final_mask]
            hit_data["证据类型"] = "关键词匹配"
            hit_data.loc[merchant_mask[final_mask] & ~kw_mask[final_mask], "证据类型"] = "商户识别"
            hit_data.loc[kw_mask[final_mask] & merchant_mask[final_mask], "证据类型"] = "关键词+商户"

            category_hits[cat_key] = {
                "label": label,
                "data": hit_data,
            }

        items = []
        for cat_key, hit_info in category_hits.items():
            label = hit_info["label"]
            hit_data = hit_info["data"]

            sub_groups = hit_data.groupby("匹配关键词")
            for kw, sub_df in sub_groups:
                item = self._build_item(label, kw, sub_df, data)
                if item is not None:
                    items.append(item)

        items.sort(key=lambda x: (-self._habit_priority(x.习惯等级), -x.月均频次, -x.总金额))
        return items

    def _build_item(self, label: str, kw: str, hit_data: pd.DataFrame,
                    full_data: pd.DataFrame) -> Optional[HabitInterestItem]:
        if "_is_b2b" in hit_data.columns:
            personal_data = hit_data[~hit_data["_is_b2b"]]
        else:
            personal_data = hit_data

        if personal_data.empty:
            b2b_count = len(hit_data)
            if b2b_count > 0 and b2b_count <= 3:
                personal_data = hit_data
            else:
                return None

        count = len(personal_data)

        amount_col = "交易金额"
        if amount_col in personal_data.columns:
            total_amount = personal_data[amount_col].abs().sum()
        else:
            total_amount = 0.0

        dates = personal_data["交易日期_dt"].dropna()
        if dates.empty:
            first_date = None
            last_date = None
            months_span = 1.0
        else:
            first_date = str(dates.min().date())
            last_date = str(dates.max().date())
            active_months = set()
            for dt in dates:
                active_months.add((dt.year, dt.month))
            months_span = max(len(active_months), 1)

        monthly_freq = count / months_span

        habit_level = self._determine_habit_level(monthly_freq)

        typical_period = self._determine_typical_period(personal_data)

        top3 = personal_data.nlargest(3, amount_col) if amount_col in personal_data.columns else personal_data.head(3)
        representative = []
        for _, row in top3.iterrows():
            rep = {
                "日期": str(row["交易日期_dt"].date()) if pd.notna(row.get("交易日期_dt")) else "",
                "金额": float(row[amount_col]) if amount_col in row.index else 0.0,
                "摘要": str(row.get("交易摘要", "")),
                "对方": str(row.get("对方姓名", "")),
            }
            representative.append(rep)

        evidence_type = personal_data["证据类型"].mode().iloc[0] if len(personal_data) > 0 else "关键词匹配"

        return HabitInterestItem(
            类别=label,
            子类=kw,
            证据类型=evidence_type,
            匹配关键词=kw,
            交易次数=count,
            总金额=round(total_amount, 2),
            首次交易日期=first_date,
            最近交易日期=last_date,
            月均频次=round(monthly_freq, 2),
            习惯等级=habit_level,
            典型时段=typical_period,
            代表性交易=representative[:3],
        )

    @staticmethod
    def _determine_habit_level(monthly_freq: float) -> str:
        if monthly_freq >= 4:
            return "高频习惯"
        elif monthly_freq >= 1:
            return "低频偏好"
        else:
            return "偶尔消费"

    @classmethod
    def _is_b2b_counterparty(cls, name: str) -> bool:
        if not name or len(name) < 4:
            return False
        for suffix in cls._B2B_SUFFIXES:
            if suffix in name:
                return True
        return False

    @staticmethod
    def _determine_typical_period(hit_data: pd.DataFrame) -> str:
        if "交易小时" not in hit_data.columns:
            return ""

        hours = hit_data["交易小时"].dropna()
        if hours.empty:
            return ""

        hour_counts = {}
        for h in hours:
            h_int = int(h) if pd.notna(h) else -1
            if h_int < 0:
                continue
            if 9 <= h_int < 17:
                period = "工作时段"
            elif 17 <= h_int < 22:
                period = "下班后"
            elif 22 <= h_int or h_int < 6:
                period = "深夜"
            else:
                period = "清晨"
            hour_counts[period] = hour_counts.get(period, 0) + 1

        if not hour_counts:
            return ""

        sorted_periods = sorted(hour_counts.items(), key=lambda x: -x[1])
        return sorted_periods[0][0]

    @staticmethod
    def _habit_priority(level: str) -> int:
        return {"高频习惯": 3, "低频偏好": 2, "偶尔消费": 1}.get(level, 0)

    def _load_categories(self) -> dict:
        if self.keywords is None:
            return {}
        return self.keywords.get_habits_interests_categories()
