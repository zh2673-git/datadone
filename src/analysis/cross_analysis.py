"""综合交叉分析器 - 多数据源交叉关联"""

import pandas as pd
import logging
from typing import Dict, List, Optional

from src.models.analysis_result import CrossAnalysisItem, FrequencyItem, CallFrequencyItem


class CrossAnalyzer:
    """综合交叉分析器"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(
        self,
        freq_result: dict[str, list[FrequencyItem]],
        call_freq_result: list[CallFrequencyItem],
    ) -> dict[str, list[CrossAnalysisItem]]:
        """
        综合交叉分析
        Args:
            freq_result: {平台: [FrequencyItem]} 来自频率分析
            call_freq_result: 话单频率结果
        Returns:
            {基准名: [CrossAnalysisItem]}
        """
        result: dict[str, list[CrossAnalysisItem]] = {}

        # 构建各平台 DataFrame
        freq_dfs: dict[str, pd.DataFrame] = {}
        for platform, items in freq_result.items():
            if items:
                freq_dfs[platform] = self._freq_items_to_df(items)

        # 话单 DataFrame
        call_df = self._call_items_to_df(call_freq_result) if call_freq_result else None

        # 全局联系人映射（对方姓名 → 对方单位名称/职务）
        unit_map, title_map = self._build_contact_map(call_freq_result)

        # 以各数据源为基准
        base_configs = [
            ("以话单为基准", "call", call_df),
            ("以银行为基准", "银行", freq_dfs.get("银行")),
            ("以微信为基准", "微信", freq_dfs.get("微信")),
            ("以支付宝为基准", "支付宝", freq_dfs.get("支付宝")),
        ]

        for base_name, base_key, base_df in base_configs:
            if base_df is None or base_df.empty:
                continue

            merged = self._merge_with_other_sources(
                base_name, base_df, base_key, freq_dfs, call_df, unit_map, title_map
            )
            if merged:
                result[base_name] = merged

        return result

    # ------------------------------------------------------------------
    # 合并逻辑
    # ------------------------------------------------------------------
    def _merge_with_other_sources(
        self,
        base_name: str,
        base_df: pd.DataFrame,
        base_key: str,
        freq_dfs: dict[str, pd.DataFrame],
        call_df: Optional[pd.DataFrame],
        unit_map: dict[str, str],
        title_map: dict[str, str],
    ) -> list[CrossAnalysisItem]:
        """以 base_df 为主表，左连接其他数据源"""
        # 主表列统一
        main = base_df[["本方姓名", "对方姓名"]].copy()
        main["分析基准"] = base_name

        # 话单信息
        if "对方号码" in base_df.columns:
            main["对方号码"] = base_df["对方号码"].values
        else:
            main["对方号码"] = None

        # 从 unit_map/title_map 补充
        main["对方单位名称"] = main["对方姓名"].map(unit_map)
        main["对方职务"] = main["对方姓名"].map(title_map)

        # 如果 base_df 是话单，补充通话信息
        if base_key == "call" and call_df is not None:
            call_subset = call_df[["本方姓名", "对方姓名", "通话次数", "通话总时长分钟"]].copy() if "通话次数" in call_df.columns else None
            if call_subset is not None:
                # 取每人每对方的汇总
                call_agg = call_subset.groupby(["本方姓名", "对方姓名"]).agg({
                    "通话次数": "sum",
                    "通话总时长分钟": "sum",
                }).reset_index()
                main = main.merge(call_agg, on=["本方姓名", "对方姓名"], how="left")
        else:
            main["通话次数"] = None
            main["通话总时长"] = None
            # 左连接话单
            if call_df is not None and "通话次数" in call_df.columns:
                call_agg = call_df.groupby(["本方姓名", "对方姓名"]).agg({
                    "通话次数": "sum",
                    "通话总时长分钟": "sum",
                }).reset_index()
                call_agg.rename(columns={"通话总时长分钟": "通话总时长_call"}, inplace=True)
                main = main.merge(call_agg, on=["本方姓名", "对方姓名"], how="left", suffixes=("", "_call"))
                main["通话次数"] = main["通话次数_call"].fillna(main.get("通话次数"))
                main["通话总时长"] = main.get("通话总时长_call")
                drop_cols = [c for c in main.columns if c.endswith("_call")]
                main.drop(columns=drop_cols, inplace=True, errors="ignore")

        # 各平台金额明细
        platform_names = ["银行", "微信", "支付宝"]
        for pname in platform_names:
            pf_df = freq_dfs.get(pname)
            if pf_df is not None and not pf_df.empty:
                pf_subset = pf_df[["本方姓名", "对方姓名", "收入总额", "支出总额", "交易次数"]].copy()
                pf_subset.rename(columns={
                    "收入总额": f"{pname}_收入总额",
                    "支出总额": f"{pname}_支出总额",
                    "交易次数": f"{pname}_交易次数",
                }, inplace=True)
                pf_agg = pf_subset.groupby(["本方姓名", "对方姓名"]).agg({
                    f"{pname}_收入总额": "sum",
                    f"{pname}_支出总额": "sum",
                    f"{pname}_交易次数": "sum",
                }).reset_index()
                main = main.merge(pf_agg, on=["本方姓名", "对方姓名"], how="left")
            else:
                main[f"{pname}_收入总额"] = None
                main[f"{pname}_支出总额"] = None
                main[f"{pname}_交易次数"] = None

        # 如果是账单为基准，从 base_df 填入收入/支出/次数
        if base_key in platform_names:
            base_income = base_df.groupby(["本方姓名", "对方姓名"])["收入总额"].sum().reset_index()
            base_expense = base_df.groupby(["本方姓名", "对方姓名"])["支出总额"].sum().reset_index()
            base_count = base_df.groupby(["本方姓名", "对方姓名"])["交易次数"].sum().reset_index()
            base_agg = base_income.merge(base_expense, on=["本方姓名", "对方姓名"]).merge(base_count, on=["本方姓名", "对方姓名"])
            main = main.merge(base_agg, on=["本方姓名", "对方姓名"], how="left", suffixes=("", "_base"))
            # 优先用 base 数据填充
            main["收入总额"] = main.get("收入总额_base", main.get("收入总额"))
            main["支出总额"] = main.get("支出总额_base", main.get("支出总额"))
            main["交易次数"] = main.get("交易次数_base", main.get("交易次数"))
            drop_cols = [c for c in main.columns if c.endswith("_base")]
            main.drop(columns=drop_cols, inplace=True, errors="ignore")

        # 计算 平台金额分布
        def _format_distribution(row) -> str:
            parts = []
            for pname in platform_names:
                inc = row.get(f"{pname}_收入总额")
                exp = row.get(f"{pname}_支出总额")
                if pd.notna(inc) or pd.notna(exp):
                    inc_str = f"{inc:,.0f}" if pd.notna(inc) else "0"
                    exp_str = f"{exp:,.0f}" if pd.notna(exp) else "0"
                    parts.append(f"{pname}(收入{inc_str}元,支出{exp_str}元)")
            return "; ".join(parts)

        main["平台金额分布"] = main.apply(_format_distribution, axis=1)

        # 判定涉及的平台
        def _detect_platform(row) -> str:
            platforms = []
            for pname in platform_names:
                if pd.notna(row.get(f"{pname}_交易次数")) and row.get(f"{pname}_交易次数", 0) > 0:
                    platforms.append(pname)
            if pd.notna(row.get("通话次数")) and row.get("通话次数", 0) > 0:
                platforms.append("话单")
            return "、".join(platforms)

        main["平台"] = main.apply(_detect_platform, axis=1)

        # 填充默认值
        for col in ["收入总额", "支出总额", "交易次数"]:
            if col not in main.columns:
                main[col] = None

        # 构建 CrossAnalysisItem 列表
        items = []
        for _, row in main.iterrows():
            items.append(CrossAnalysisItem(
                分析基准=str(row.get("分析基准", base_name)),
                本方姓名=str(row.get("本方姓名", "")),
                对方姓名=str(row.get("对方姓名", "")),
                对方号码=str(row.get("对方号码")) if pd.notna(row.get("对方号码")) else None,
                对方单位名称=str(row.get("对方单位名称")) if pd.notna(row.get("对方单位名称")) else None,
                对方职务=str(row.get("对方职务")) if pd.notna(row.get("对方职务")) else None,
                通话次数=int(row["通话次数"]) if pd.notna(row.get("通话次数")) else None,
                通话总时长=float(row["通话总时长"]) if pd.notna(row.get("通话总时长")) else None,
                收入总额=float(row["收入总额"]) if pd.notna(row.get("收入总额")) else None,
                支出总额=float(row["支出总额"]) if pd.notna(row.get("支出总额")) else None,
                交易次数=int(row["交易次数"]) if pd.notna(row.get("交易次数")) else None,
                平台=str(row.get("平台", "")) or None,
                平台金额分布=str(row.get("平台金额分布", "")) or None,
                银行_收入总额=float(row.get("银行_收入总额")) if pd.notna(row.get("银行_收入总额")) else None,
                银行_支出总额=float(row.get("银行_支出总额")) if pd.notna(row.get("银行_支出总额")) else None,
                银行_交易次数=int(row.get("银行_交易次数")) if pd.notna(row.get("银行_交易次数")) else None,
                微信_收入总额=float(row.get("微信_收入总额")) if pd.notna(row.get("微信_收入总额")) else None,
                微信_支出总额=float(row.get("微信_支出总额")) if pd.notna(row.get("微信_支出总额")) else None,
                微信_交易次数=int(row.get("微信_交易次数")) if pd.notna(row.get("微信_交易次数")) else None,
                支付宝_收入总额=float(row.get("支付宝_收入总额")) if pd.notna(row.get("支付宝_收入总额")) else None,
                支付宝_支出总额=float(row.get("支付宝_支出总额")) if pd.notna(row.get("支付宝_支出总额")) else None,
                支付宝_交易次数=int(row.get("支付宝_交易次数")) if pd.notna(row.get("支付宝_交易次数")) else None,
            ))

        return items

    # ------------------------------------------------------------------
    # 辅助方法
    # ------------------------------------------------------------------
    @staticmethod
    def _freq_items_to_df(items: list[FrequencyItem]) -> pd.DataFrame:
        records = []
        for item in items:
            records.append({
                "本方姓名": item.本方姓名,
                "对方姓名": item.对方姓名,
                "收入总额": item.收入总额,
                "支出总额": item.支出总额,
                "交易次数": item.交易次数,
                "交易总金额": item.交易总金额,
                "平台": item.platform,
                "数据来源": item.data_source,
            })
        return pd.DataFrame(records)

    @staticmethod
    def _call_items_to_df(items: list[CallFrequencyItem]) -> pd.DataFrame:
        records = []
        for item in items:
            records.append({
                "本方姓名": item.本方姓名,
                "对方姓名": item.对方姓名,
                "对方号码": item.对方号码,
                "对方单位名称": item.对方单位名称,
                "对方职务": item.对方职务,
                "通话次数": item.通话次数,
                "通话总时长分钟": item.通话总时长分钟,
            })
        return pd.DataFrame(records)

    @staticmethod
    def _build_contact_map(call_freq_result: list[CallFrequencyItem]):
        """从话单频率结果构建联系人映射"""
        unit_map: dict[str, str] = {}
        title_map: dict[str, str] = {}
        for item in call_freq_result:
            if item.对方单位名称 and item.对方姓名 not in unit_map:
                unit_map[item.对方姓名] = item.对方单位名称
            if item.对方职务 and item.对方姓名 not in title_map:
                title_map[item.对方姓名] = item.对方职务
        return unit_map, title_map
