"""重点收支识别器 - 工作收入/房产/租金/车辆/证券/大额识别"""

import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import date

from src.models.analysis_result import KeyTransactionItem


class KeyTransactionAnalyzer:
    """重点收支识别器"""

    def __init__(self, keywords, thresholds):
        self.keywords = keywords
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(self, all_data: dict[str, pd.DataFrame]) -> dict[str, list[KeyTransactionItem]]:
        """
        识别重点收支交易
        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ...}
        Returns:
            {本方姓名: [KeyTransactionItem]}
        """
        result: dict[str, list[KeyTransactionItem]] = {}

        # 收集所有人员的 DataFrame
        all_persons_data = self._collect_person_data(all_data)

        for person, df in all_persons_data.items():
            if df is None or df.empty:
                continue
            items = self._analyze_person(df, person)
            if items:
                result[person] = items

        return result

    # ------------------------------------------------------------------
    # 收集所有人员数据
    # ------------------------------------------------------------------
    def _collect_person_data(self, all_data: dict) -> dict[str, pd.DataFrame]:
        """将所有平台的按人数据合并"""
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

        result = {}
        for person, dfs in person_data.items():
            result[person] = pd.concat(dfs, ignore_index=True)
        return result

    # ------------------------------------------------------------------
    # 单人分析
    # ------------------------------------------------------------------
    def _analyze_person(self, df: pd.DataFrame, person: str) -> list[KeyTransactionItem]:
        data = df.copy()

        # 构建匹配文本
        text_parts = []
        for col in ["交易摘要", "交易备注", "交易类型", "对方姓名"]:
            if col in data.columns:
                text_parts.append(data[col].fillna("").astype(str))
            else:
                text_parts.append(pd.Series([""] * len(data), index=data.index))
        data["匹配文本"] = text_parts[0]
        for part in text_parts[1:]:
            data["匹配文本"] = data["匹配文本"] + " " + part

        # 收集所有标签
        labels: list[dict] = []  # [{index, 重点类型, 重点子类, 匹配关键词, 置信度, 优先级}]

        # 加载关键词
        work_income_kw = self.keywords.get_work_income_keywords()
        property_kw = self.keywords.get_property_keywords()
        rental_kw = self.keywords.get_rental_keywords()
        vehicle_kw = self.keywords.get_vehicle_keywords()
        securities_kw = self.keywords.get_securities_keywords()
        large_amount_thresholds = self.thresholds.get("analysis.key_transactions.large_amount_thresholds", {})

        # ---------- 工作收入 ----------
        self._match_category(data, labels, work_income_kw, "工作收入", "工资奖金",
                             direction="收入", priority=1, confidence=0.9)

        # ---------- 房产收入 ----------
        self._match_category(data, labels, property_kw, "资产收入", "房产",
                             direction="收入", priority=2, confidence=0.85)

        # ---------- 租金收入 ----------
        self._match_category(data, labels, rental_kw, "资产收入", "租金",
                             direction="收入", priority=3, confidence=0.85)

        # ---------- 车辆收入 ----------
        self._match_category(data, labels, vehicle_kw, "资产收入", "车辆收入",
                             direction="收入", priority=4, confidence=0.85)

        # ---------- 车辆支出 ----------
        self._match_category(data, labels, vehicle_kw, "资产支出", "车辆支出",
                             direction="支出", priority=5, confidence=0.85)

        # ---------- 房产支出 ----------
        self._match_category(data, labels, property_kw, "资产支出", "房产支出",
                             direction="支出", priority=6, confidence=0.85)

        # ---------- 证券收入 ----------
        self._match_category(data, labels, securities_kw, "资产收入", "证券收入",
                             direction="收入", priority=7, confidence=0.85)

        # ---------- 证券支出 ----------
        self._match_category(data, labels, securities_kw, "资产支出", "证券支出",
                             direction="支出", priority=8, confidence=0.85)

        # ---------- 租金支出 ----------
        self._match_category(data, labels, rental_kw, "资产支出", "租金支出",
                             direction="支出", priority=9, confidence=0.85)

        # ---------- 大额交易 ----------
        self._match_large_amount(data, labels, large_amount_thresholds)

        # ---------- 规避阈值交易 ----------
        self._match_evasion_threshold(data, labels)

        # 按优先级取主类
        return self._resolve_labels(data, labels, person)

    # ------------------------------------------------------------------
    # 关键词匹配
    # ------------------------------------------------------------------
    @staticmethod
    def _match_category(data, labels, keywords, main_type, sub_type,
                        direction: str, priority: int, confidence: float):
        if not keywords:
            return
        pattern = "|".join(keywords)
        match_mask = data["匹配文本"].str.contains(pattern, case=False, na=False)

        if direction == "收入":
            direction_mask = data["收入金额"] > 0 if "收入金额" in data.columns else pd.Series([False] * len(data), index=data.index)
        else:
            direction_mask = data["支出金额"] > 0 if "支出金额" in data.columns else pd.Series([False] * len(data), index=data.index)

        final_mask = match_mask & direction_mask

        for idx in data.index[final_mask]:
            # 找到匹配的关键词
            matched_kw = ""
            text = str(data.loc[idx, "匹配文本"])
            for kw in keywords:
                if kw.lower() in text.lower():
                    matched_kw = kw
                    break

            labels.append({
                "index": idx,
                "重点类型": main_type,
                "重点子类": sub_type,
                "匹配关键词": matched_kw,
                "置信度": confidence,
                "优先级": priority,
            })

    # ------------------------------------------------------------------
    # 大额交易识别
    # ------------------------------------------------------------------
    def _match_large_amount(self, data, labels, large_amount_thresholds):
        if not large_amount_thresholds:
            return

        if "交易金额" not in data.columns:
            return

        abs_amount = data["交易金额"].abs()

        for level_key, level_cfg in large_amount_thresholds.items():
            min_amt = level_cfg.get("min", 0)
            max_amt = level_cfg.get("max", float("inf"))
            name = level_cfg.get("name", level_key)

            level_mask = (abs_amount >= min_amt) & (abs_amount < max_amt)

            # 大额收入
            income_mask = level_mask & (data.get("收入金额", pd.Series([0.0] * len(data), index=data.index)) > 0)
            for idx in data.index[income_mask]:
                labels.append({
                    "index": idx,
                    "重点类型": "大额收入",
                    "重点子类": name,
                    "匹配关键词": f"金额>={min_amt}",
                    "置信度": 0.7,
                    "优先级": 10,
                })

            # 大额支出
            expense_mask = level_mask & (data.get("支出金额", pd.Series([0.0] * len(data), index=data.index)) > 0)
            for idx in data.index[expense_mask]:
                labels.append({
                    "index": idx,
                    "重点类型": "大额支出",
                    "重点子类": name,
                    "匹配关键词": f"金额>={min_amt}",
                    "置信度": 0.7,
                    "优先级": 11,
                })

    # ------------------------------------------------------------------
    # 规避阈值交易识别
    # ------------------------------------------------------------------
    def _match_evasion_threshold(self, data, labels):
        """识别接近监管阈值的交易，可能存在规避大额交易报告的意图
        
        监管阈值：
        - 现金交易：单笔5万元
        - 转账交易：单笔50万元
        规避区间：阈值的80%-100%，即4-5万、40-50万
        """
        if "交易金额" not in data.columns:
            return

        abs_amount = data["交易金额"].abs()

        # 现金规避阈值：4万-5万
        cash_evasion_mask = (abs_amount >= 40000) & (abs_amount < 50000)
        for idx in data.index[cash_evasion_mask]:
            row = data.loc[idx]
            direction = "收入" if row.get("收入金额", 0) > 0 else "支出"
            labels.append({
                "index": idx,
                "重点类型": "规避阈值",
                "重点子类": "4-5万区间",
                "匹配关键词": f"金额{abs(row.get('交易金额', 0)):,.0f}接近5万监管线",
                "置信度": 0.75,
                "优先级": 12,
            })

        # 转账规避阈值：40万-50万
        transfer_evasion_mask = (abs_amount >= 400000) & (abs_amount < 500000)
        for idx in data.index[transfer_evasion_mask]:
            row = data.loc[idx]
            direction = "收入" if row.get("收入金额", 0) > 0 else "支出"
            labels.append({
                "index": idx,
                "重点类型": "规避阈值",
                "重点子类": "40-50万区间",
                "匹配关键词": f"金额{abs(row.get('交易金额', 0)):,.0f}接近50万监管线",
                "置信度": 0.75,
                "优先级": 13,
            })

    # ------------------------------------------------------------------
    # 多标签优先级判定
    # ------------------------------------------------------------------
    def _resolve_labels(self, data, labels, person: str) -> list[KeyTransactionItem]:
        """按优先级取主类，同一 index 只保留最高优先级的标签"""
        # 按 index 分组，取优先级最高的
        from collections import defaultdict
        idx_labels = defaultdict(list)
        for label in labels:
            idx_labels[label["index"]].append(label)

        items = []
        for idx, label_list in idx_labels.items():
            # 按优先级排序，取第一个
            label_list.sort(key=lambda x: x["优先级"])
            primary = label_list[0]

            row = data.loc[idx]
            tx_date = None
            if "交易日期" in row.index:
                try:
                    tx_date = pd.to_datetime(row["交易日期"], errors="coerce").date()
                except Exception:
                    pass

            tx_amount = float(row.get("交易金额", 0)) if "交易金额" in row.index else 0.0
            tx_direction = "收入" if row.get("收入金额", 0) > 0 else "支出"

            items.append(KeyTransactionItem(
                index=int(idx) if isinstance(idx, int) else 0,
                本方姓名=str(person),
                交易日期=tx_date,
                交易金额=tx_amount,
                交易方向=tx_direction,
                重点类型=primary["重点类型"],
                重点子类=primary["重点子类"],
                匹配关键词=primary["匹配关键词"],
                置信度=primary["置信度"],
            ))

        return items
