"""存取现识别器 - 三级优先级递进识别存现/取现/转账"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional

from src.models.analysis_result import CashRecognitionItem


class CashRecognizer:
    """存取现识别器，仅对银行数据执行"""

    def __init__(self, keywords, thresholds):
        """
        Args:
            keywords: KeywordsManager 实例
            thresholds: ThresholdsManager 实例
        """
        self.keywords = keywords
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(self, bank_data: dict[str, pd.DataFrame]) -> dict[str, list[CashRecognitionItem]]:
        """对银行数据执行存取现识别，返回 {本方姓名: [CashRecognitionItem]}"""
        result: dict[str, list[CashRecognitionItem]] = {}
        for person, df in bank_data.items():
            if df is None or df.empty:
                result[person] = []
                continue
            result[person] = self._recognize_person(df)
        return result

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------
    def _recognize_person(self, df: pd.DataFrame) -> list[CashRecognitionItem]:
        data = df

        # 默认标识（直接在原DataFrame上修改）
        data["存取现标识"] = "转账"
        data["识别置信度"] = 0.0
        data["识别原因"] = ""

        # 收入/支出金额初始化
        data["收入金额"] = 0.0
        data["支出金额"] = 0.0

        # ---------- 构建辅助列 ----------
        opposite_name = data.get("对方姓名")
        empty_opposite = self._build_empty_opposite_mask(opposite_name)

        summary_col = data["交易摘要"].astype(str).fillna("") if "交易摘要" in data.columns else pd.Series([""] * len(data), index=data.index)
        remark_col = data["交易备注"].astype(str).fillna("") if "交易备注" in data.columns else pd.Series([""] * len(data), index=data.index)
        type_col = data["交易类型"].astype(str).fillna("") if "交易类型" in data.columns else pd.Series([""] * len(data), index=data.index)

        direction_col = data["借贷标识"] if "借贷标识" in data.columns else None
        amount_col = data["交易金额"] if "交易金额" in data.columns else None

        if amount_col is None:
            return self._build_items(data)

        income_flag = "贷"
        expense_flag = "借"

        # ---------- 1. 高优先级识别 (0.95) ----------
        self._high_priority_recognition(
            data, empty_opposite, summary_col, remark_col, type_col,
            direction_col, amount_col, income_flag, expense_flag
        )

        # ---------- 2. 中优先级识别 (0.8) ----------
        self._medium_priority_recognition(
            data, empty_opposite, summary_col, remark_col, type_col,
            direction_col, amount_col, income_flag, expense_flag
        )

        # ---------- 3. 低优先级识别 (0.6) ----------
        self._low_priority_recognition(
            data, empty_opposite, summary_col, remark_col, type_col,
            direction_col, amount_col, income_flag, expense_flag
        )

        # ---------- 4. 金额后处理 ----------
        self._amount_post_processing(data, amount_col)

        # ---------- 5. 计算收入/支出金额 ----------
        self._calc_income_expense(data, direction_col, amount_col)

        return self._build_items(data)

    # ------------------------------------------------------------------
    # 辅助：对方姓名为空掩码
    # ------------------------------------------------------------------
    @staticmethod
    def _build_empty_opposite_mask(series: Optional[pd.Series]) -> pd.Series:
        if series is None:
            return pd.Series([True] * len(series) if series is not None else [], dtype=bool)
        return (
            series.isna()
            | (series.astype(str).str.strip() == "")
            | (series.astype(str).str.strip() == "\\N")
        )

    # ------------------------------------------------------------------
    # 高优先级识别
    # ------------------------------------------------------------------
    def _high_priority_recognition(self, data, empty_opposite, summary_col, remark_col, type_col,
                                   direction_col, amount_col, income_flag, expense_flag):
        confidence = self.thresholds.get("analysis.cash.recognition.high_priority_confidence", 0.95)
        deposit_keywords = self.keywords.get_high_priority_deposit_keywords()
        withdraw_keywords = self.keywords.get_high_priority_withdraw_keywords()
        deposit_exclude = self.keywords.get_deposit_exclude_keywords()
        withdraw_exclude = self.keywords.get_withdraw_exclude_keywords()

        deposit_exclude_pattern = "|".join(deposit_exclude) if deposit_exclude else ""
        withdraw_exclude_pattern = "|".join(withdraw_exclude) if withdraw_exclude else ""

        # 存现
        for kw in deposit_keywords:
            base_mask = empty_opposite & self._text_match(summary_col, remark_col, type_col, kw) & (direction_col == income_flag) & (data["存取现标识"] == "转账")
            if deposit_exclude_pattern:
                mask = base_mask & ~self._text_match(summary_col, remark_col, type_col, deposit_exclude_pattern)
            else:
                mask = base_mask
            if mask.any():
                data.loc[mask, "存取现标识"] = "存现"
                data.loc[mask, "识别置信度"] = confidence
                data.loc[mask, "识别原因"] = f"高优先级关键词匹配: {kw}"

        # 取现
        for kw in withdraw_keywords:
            base_mask = empty_opposite & self._text_match(summary_col, remark_col, type_col, kw) & (direction_col == expense_flag) & (data["存取现标识"] == "转账")
            if withdraw_exclude_pattern:
                mask = base_mask & ~self._text_match(summary_col, remark_col, type_col, withdraw_exclude_pattern)
            else:
                mask = base_mask
            if mask.any():
                data.loc[mask, "存取现标识"] = "取现"
                data.loc[mask, "识别置信度"] = confidence
                data.loc[mask, "识别原因"] = f"高优先级关键词匹配: {kw}"

    # ------------------------------------------------------------------
    # 中优先级识别
    # ------------------------------------------------------------------
    def _medium_priority_recognition(self, data, empty_opposite, summary_col, remark_col, type_col,
                                     direction_col, amount_col, income_flag, expense_flag):
        confidence = self.thresholds.get("analysis.cash.recognition.medium_priority_confidence", 0.8)
        deposit_keywords = self.keywords.get_deposit_keywords()
        withdraw_keywords = self.keywords.get_withdraw_keywords()
        deposit_exclude = self.keywords.get_deposit_exclude_keywords()
        withdraw_exclude = self.keywords.get_withdraw_exclude_keywords()

        deposit_pattern = "|".join(deposit_keywords) if deposit_keywords else ""
        withdraw_pattern = "|".join(withdraw_keywords) if withdraw_keywords else ""
        deposit_exclude_pattern = "|".join(deposit_exclude) if deposit_exclude else ""
        withdraw_exclude_pattern = "|".join(withdraw_exclude) if withdraw_exclude else ""

        if not deposit_pattern and not withdraw_pattern:
            return

        # 存现
        if deposit_pattern:
            deposit_base = empty_opposite & (direction_col == income_flag) & (data["存取现标识"] == "转账")
            if deposit_exclude_pattern:
                deposit_candidate = deposit_base & ~self._text_match(summary_col, remark_col, type_col, deposit_exclude_pattern)
            else:
                deposit_candidate = deposit_base
            deposit_mask = deposit_candidate & self._text_match(summary_col, remark_col, type_col, deposit_pattern)
            if deposit_mask.any():
                data.loc[deposit_mask, "存取现标识"] = "存现"
                data.loc[deposit_mask, "识别置信度"] = confidence
                data.loc[deposit_mask, "识别原因"] = "中优先级关键词匹配"

        # 取现
        if withdraw_pattern:
            withdraw_base = empty_opposite & (direction_col == expense_flag) & (data["存取现标识"] == "转账")
            if withdraw_exclude_pattern:
                withdraw_candidate = withdraw_base & ~self._text_match(summary_col, remark_col, type_col, withdraw_exclude_pattern)
            else:
                withdraw_candidate = withdraw_base
            withdraw_mask = withdraw_candidate & self._text_match(summary_col, remark_col, type_col, withdraw_pattern)
            if withdraw_mask.any():
                data.loc[withdraw_mask, "存取现标识"] = "取现"
                data.loc[withdraw_mask, "识别置信度"] = confidence
                data.loc[withdraw_mask, "识别原因"] = "中优先级关键词匹配"

        # ATM 智能识别
        self._atm_smart_recognition(
            data, empty_opposite, summary_col, remark_col, type_col,
            direction_col, amount_col, income_flag, expense_flag,
            deposit_exclude_pattern, withdraw_exclude_pattern, confidence
        )

    # ------------------------------------------------------------------
    # ATM 智能识别
    # ------------------------------------------------------------------
    def _atm_smart_recognition(self, data, empty_opposite, summary_col, remark_col, type_col,
                               direction_col, amount_col, income_flag, expense_flag,
                               deposit_exclude_pattern, withdraw_exclude_pattern, confidence):
        # ATM 存现
        atm_deposit_base = empty_opposite & (direction_col == income_flag) & (data["存取现标识"] == "转账")
        if deposit_exclude_pattern:
            atm_deposit_candidate = atm_deposit_base & ~self._text_match(summary_col, remark_col, type_col, deposit_exclude_pattern)
        else:
            atm_deposit_candidate = atm_deposit_base
        atm_deposit_mask = atm_deposit_candidate & self._text_match(summary_col, remark_col, type_col, "ATM")
        if atm_deposit_mask.any():
            data.loc[atm_deposit_mask, "存取现标识"] = "存现"
            data.loc[atm_deposit_mask, "识别置信度"] = confidence
            data.loc[atm_deposit_mask, "识别原因"] = "ATM智能识别-存现"

        # ATM 取现
        atm_withdraw_base = empty_opposite & (direction_col == expense_flag) & (data["存取现标识"] == "转账")
        if withdraw_exclude_pattern:
            atm_withdraw_candidate = atm_withdraw_base & ~self._text_match(summary_col, remark_col, type_col, withdraw_exclude_pattern)
        else:
            atm_withdraw_candidate = atm_withdraw_base
        atm_withdraw_mask = atm_withdraw_candidate & self._text_match(summary_col, remark_col, type_col, "ATM")
        if atm_withdraw_mask.any():
            data.loc[atm_withdraw_mask, "存取现标识"] = "取现"
            data.loc[atm_withdraw_mask, "识别置信度"] = confidence
            data.loc[atm_withdraw_mask, "识别原因"] = "ATM智能识别-取现"

    # ------------------------------------------------------------------
    # 低优先级识别
    # ------------------------------------------------------------------
    def _low_priority_recognition(self, data, empty_opposite, summary_col, remark_col, type_col,
                                  direction_col, amount_col, income_flag, expense_flag):
        confidence = self.thresholds.get("analysis.cash.recognition.low_priority_confidence", 0.6)
        deposit_exclude = self.keywords.get_deposit_exclude_keywords()
        deposit_exclude_pattern = "|".join(deposit_exclude) if deposit_exclude else ""

        common_cash_amounts = self.thresholds.get("analysis.cash.recognition.common_cash_amounts",
                                                   [100, 200, 300, 500, 1000, 2000, 3000, 5000, 10000, 20000, 50000])
        round_modulos = self.thresholds.get("analysis.cash.recognition.round_amount_modulos", [50, 100])

        # 包含"现"字
        base_fuzzy = empty_opposite & self._text_match(summary_col, remark_col, type_col, "现") & (data["存取现标识"] == "转账")

        # 排除转账
        if deposit_exclude_pattern:
            fuzzy_mask = base_fuzzy & ~self._text_match(summary_col, remark_col, type_col, deposit_exclude_pattern)
        else:
            fuzzy_mask = base_fuzzy

        # 金额条件
        abs_amount = amount_col.abs()
        round_conds = [abs_amount % m == 0 for m in round_modulos]
        round_amount_mask = pd.concat(round_conds, axis=1).any(axis=1) if round_conds else pd.Series([False] * len(data), index=data.index)
        amount_mask = abs_amount.isin(common_cash_amounts) if common_cash_amounts else pd.Series([False] * len(data), index=data.index)

        # 存现
        fuzzy_deposit = fuzzy_mask & (direction_col == income_flag) & (round_amount_mask | amount_mask)
        if fuzzy_deposit.any():
            data.loc[fuzzy_deposit, "存取现标识"] = "存现"
            data.loc[fuzzy_deposit, "识别置信度"] = confidence
            data.loc[fuzzy_deposit, "识别原因"] = "低优先级上下文分析"

        # 取现
        fuzzy_withdraw = fuzzy_mask & (direction_col == expense_flag) & (round_amount_mask | amount_mask)
        if fuzzy_withdraw.any():
            data.loc[fuzzy_withdraw, "存取现标识"] = "取现"
            data.loc[fuzzy_withdraw, "识别置信度"] = confidence
            data.loc[fuzzy_withdraw, "识别原因"] = "低优先级上下文分析"

    # ------------------------------------------------------------------
    # 金额后处理
    # ------------------------------------------------------------------
    def _amount_post_processing(self, data, amount_col):
        large_threshold = self.thresholds.get("analysis.cash.recognition.large_amount_threshold", 100000)
        small_threshold = self.thresholds.get("analysis.cash.recognition.small_amount_threshold", 10)

        cash_mask = data["存取现标识"].isin(["存现", "取现"])
        abs_amount = amount_col.abs()

        large_mask = cash_mask & (abs_amount > large_threshold)
        if large_mask.any():
            data.loc[large_mask, "识别置信度"] *= 0.8
            data.loc[large_mask, "识别原因"] += " (大额交易置信度调整)"

        small_mask = cash_mask & (abs_amount < small_threshold)
        if small_mask.any():
            data.loc[small_mask, "识别置信度"] *= 0.7
            data.loc[small_mask, "识别原因"] += " (小额交易置信度调整)"

    # ------------------------------------------------------------------
    # 收入/支出金额计算
    # ------------------------------------------------------------------
    @staticmethod
    def _calc_income_expense(data, direction_col, amount_col):
        abs_amount = amount_col.abs()

        # 存现 → 收入
        deposit_mask = data["存取现标识"] == "存现"
        data.loc[deposit_mask, "收入金额"] = abs_amount[deposit_mask]

        # 取现 → 支出
        withdraw_mask = data["存取现标识"] == "取现"
        data.loc[withdraw_mask, "支出金额"] = abs_amount[withdraw_mask]

        # 转账 → 按借贷标识
        transfer_mask = data["存取现标识"] == "转账"
        if direction_col is not None:
            credit_mask = transfer_mask & (direction_col == "贷")
            debit_mask = transfer_mask & (direction_col == "借")
            data.loc[credit_mask, "收入金额"] = abs_amount[credit_mask]
            data.loc[debit_mask, "支出金额"] = abs_amount[debit_mask]

    # ------------------------------------------------------------------
    # 文本匹配辅助
    # ------------------------------------------------------------------
    @staticmethod
    def _text_match(summary, remark, type_col, pattern) -> pd.Series:
        """任意列包含 pattern 即为 True"""
        if not pattern:
            return pd.Series([False] * len(summary), index=summary.index)
        m = summary.str.contains(pattern, case=False, na=False)
        m = m | remark.str.contains(pattern, case=False, na=False)
        m = m | type_col.str.contains(pattern, case=False, na=False)
        return m

    # ------------------------------------------------------------------
    # 构建 CashRecognitionItem 列表
    # ------------------------------------------------------------------
    @staticmethod
    def _build_items(data: pd.DataFrame) -> list[CashRecognitionItem]:
        items = []
        for idx, row in data.iterrows():
            items.append(CashRecognitionItem(
                index=int(idx) if isinstance(idx, int) else 0,
                cash_type=str(row.get("存取现标识", "转账")),
                confidence=float(row.get("识别置信度", 0.0)),
                reason=str(row.get("识别原因", "")),
            ))
        return items
