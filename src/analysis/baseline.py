"""行为基线分析器 - 从自身历史数据建立参照系"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional

from src.models.analysis_result import PersonBaseline, MonthlyMetric


class BaselineAnalyzer:
    """行为基线分析器 - 为每个人计算行为基线，作为异常判定的参照"""

    def __init__(self, thresholds=None):
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(self, all_data: dict, call_data=None) -> dict[str, PersonBaseline]:
        """
        为每个人计算行为基线

        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ...}
            call_data: 话单DataFrame或{本方姓名: df}
        Returns:
            {本方姓名: PersonBaseline}
        """
        result: dict[str, PersonBaseline] = {}

        # 收集所有人的数据
        all_persons_data = self._collect_person_data(all_data)

        for person, df in all_persons_data.items():
            if df is None or df.empty:
                continue

            # 计算资金基线
            baseline = self._compute_baseline(person, df)

            # 计算通话基线
            call_df = self._get_person_call_data(person, call_data)
            if call_df is not None and not call_df.empty:
                self._compute_call_baseline(baseline, call_df)

            result[person] = baseline

        return result

    # ------------------------------------------------------------------
    # 收集人员数据
    # ------------------------------------------------------------------
    def _collect_person_data(self, all_data: dict) -> dict[str, pd.DataFrame]:
        """合并三平台数据"""
        person_data: dict[str, list[pd.DataFrame]] = {}
        for platform in ["银行", "微信", "支付宝"]:
            platform_data = all_data.get(platform)
            if platform_data is None:
                continue
            if isinstance(platform_data, dict):
                for person, df in platform_data.items():
                    if df is not None and not df.empty:
                        df_copy = df.copy()
                        df_copy['_平台'] = platform
                        person_data.setdefault(person, []).append(df_copy)
            elif isinstance(platform_data, pd.DataFrame) and not platform_data.empty:
                for person, group in platform_data.groupby("本方姓名"):
                    group_copy = group.copy()
                    group_copy['_平台'] = platform
                    person_data.setdefault(str(person), []).append(group_copy)

        result = {}
        for person, dfs in person_data.items():
            result[person] = pd.concat(dfs, ignore_index=True)
        return result

    def _get_person_call_data(self, person: str, call_data) -> Optional[pd.DataFrame]:
        """获取某人的话单数据"""
        if call_data is None:
            return None
        if isinstance(call_data, dict):
            return call_data.get(person)
        if isinstance(call_data, pd.DataFrame) and not call_data.empty:
            if '本方姓名' in call_data.columns:
                return call_data[call_data['本方姓名'] == person]
        return None

    # ------------------------------------------------------------------
    # 计算基线
    # ------------------------------------------------------------------
    def _compute_baseline(self, person: str, df: pd.DataFrame) -> PersonBaseline:
        """计算单人行为基线"""
        df = df.copy()

        # 确保有收入/支出金额
        if '收入金额' not in df.columns:
            df['收入金额'] = 0.0
        if '支出金额' not in df.columns:
            df['支出金额'] = 0.0

        # 解析日期
        if '交易日期' not in df.columns:
            return PersonBaseline(本方姓名=person, 数据充足度="不足")

        df['交易日期_dt'] = pd.to_datetime(df['交易日期'], errors='coerce')
        df = df.dropna(subset=['交易日期_dt'])

        if df.empty:
            return PersonBaseline(本方姓名=person, 数据充足度="不足")

        # 按月聚合
        df['年月'] = df['交易日期_dt'].dt.to_period('M')
        monthly = df.groupby('年月').agg(
            收入=('收入金额', 'sum'),
            支出=('支出金额', 'sum'),
            交易次数=('交易金额', 'count') if '交易金额' in df.columns else ('收入金额', 'count'),
            对手数=('对方姓名', 'nunique') if '对方姓名' in df.columns else ('收入金额', 'count'),
        ).reset_index()

        # 数据充足度
        num_months = len(monthly)
        if num_months >= 6:
            充足度 = "充足"
        elif num_months >= 3:
            充足度 = "有限"
        else:
            充足度 = "不足"

        # 月度基线统计
        monthly_metrics = []
        for _, row in monthly.iterrows():
            monthly_metrics.append(MonthlyMetric(
                年月=str(row['年月']),
                收入=float(row['收入']),
                支出=float(row['支出']),
                交易次数=int(row['交易次数']),
                对手数=int(row['对手数']),
            ))

        # 排除极端月(>3σ)后计算修正基线
        收入_mean, 收入_std = self._calc_mean_std(monthly['收入'])
        支出_mean, 支出_std = self._calc_mean_std(monthly['支出'])
        次数_mean, 次数_std = self._calc_mean_std(monthly['交易次数'].astype(float))
        对手_mean, 对手_std = self._calc_mean_std(monthly['对手数'].astype(float))

        # 单笔金额分布
        amounts = df['交易金额'].abs() if '交易金额' in df.columns else pd.Series([0.0])
        金额均值 = float(amounts.mean()) if len(amounts) > 0 else 0.0
        金额中位数 = float(amounts.median()) if len(amounts) > 0 else 0.0
        金额_P25 = float(amounts.quantile(0.25)) if len(amounts) > 0 else 0.0
        金额_P75 = float(amounts.quantile(0.75)) if len(amounts) > 0 else 0.0

        # 时间基线
        工作时间比, 深夜比, 周末比 = self._calc_time_baseline(df)

        # 现金基线
        现金月均, 现金占比 = self._calc_cash_baseline(df, monthly, num_months)

        # 趋势
        收入趋势 = self._calc_trend(monthly['收入'].values)
        支出趋势 = self._calc_trend(monthly['支出'].values)
        对手趋势 = self._calc_trend(monthly['对手数'].astype(float).values)

        return PersonBaseline(
            本方姓名=person,
            数据充足度=充足度,
            数据月数=num_months,
            月均收入=收入_mean,
            月均收入_std=收入_std,
            月均支出=支出_mean,
            月均支出_std=支出_std,
            月均交易次数=次数_mean,
            月均交易次数_std=次数_std,
            月均交易对手数=对手_mean,
            月均交易对手数_std=对手_std,
            单笔金额均值=金额均值,
            单笔金额中位数=金额中位数,
            单笔金额_P25=金额_P25,
            单笔金额_P75=金额_P75,
            工作时间交易占比=工作时间比,
            深夜交易占比=深夜比,
            周末交易占比=周末比,
            存取现月均金额=现金月均,
            存取现占收支比=现金占比,
            收入趋势=收入趋势,
            支出趋势=支出趋势,
            对手数趋势=对手趋势,
            月度明细=monthly_metrics,
        )

    # ------------------------------------------------------------------
    # 辅助计算
    # ------------------------------------------------------------------
    @staticmethod
    def _calc_mean_std(series: pd.Series) -> tuple[float, float]:
        """计算均值和标准差"""
        if len(series) == 0:
            return 0.0, 0.0
        mean = float(series.mean())
        std = float(series.std()) if len(series) > 1 else 0.0
        return round(mean, 2), round(std, 2)

    def _calc_time_baseline(self, df: pd.DataFrame) -> tuple[float, float, float]:
        """计算时间基线：工作时间比/深夜比/周末比"""
        dt_col = df['交易日期_dt']
        total = len(dt_col)
        if total == 0:
            return 0.0, 0.0, 0.0

        # 工作时间(9-17点) / 深夜(22-6点)
        hours = dt_col.dt.hour
        工作时间比 = float((hours.between(9, 17)).sum() / total * 100)

        深夜_mask = (hours >= 22) | (hours < 6)
        深夜比 = float(深夜_mask.sum() / total * 100)

        # 周末
        周末比 = float((dt_col.dt.weekday >= 5).sum() / total * 100)

        return round(工作时间比, 2), round(深夜比, 2), round(周末比, 2)

    def _calc_cash_baseline(self, df: pd.DataFrame, monthly: pd.DataFrame,
                            num_months: int) -> tuple[float, float]:
        """计算现金基线"""
        if '存取现标识' not in df.columns or num_months == 0:
            return 0.0, 0.0

        cash_mask = df['存取现标识'].isin(['存现', '取现'])
        cash_amount = df.loc[cash_mask, '交易金额'].abs().sum()
        total_amount = df['收入金额'].sum() + df['支出金额'].sum()

        现金月均 = round(cash_amount / num_months, 2) if num_months > 0 else 0.0
        现金占比 = round(cash_amount / total_amount * 100, 2) if total_amount > 0 else 0.0

        return 现金月均, 现金占比

    def _calc_trend(self, values: np.ndarray) -> str:
        """计算趋势方向"""
        if len(values) < 4:
            return "平稳"
        try:
            x = np.arange(len(values))
            coeffs = np.polyfit(x, values, 1)
            slope = coeffs[0]
            mean_val = np.mean(values)
            if mean_val == 0:
                return "平稳"
            # 斜率占均值的比例
            ratio = slope / mean_val
            if ratio > 0.05:
                return "上升"
            elif ratio < -0.05:
                return "下降"
            return "平稳"
        except Exception:
            return "平稳"

    def _compute_call_baseline(self, baseline: PersonBaseline, call_df: pd.DataFrame) -> None:
        """计算通话基线，更新baseline"""
        if call_df is None or call_df.empty:
            return

        if '呼叫日期' in call_df.columns:
            call_df = call_df.copy()
            call_df['呼叫日期_dt'] = pd.to_datetime(call_df['呼叫日期'], errors='coerce')
            call_df = call_df.dropna(subset=['呼叫日期_dt'])

            if not call_df.empty:
                call_df['年月'] = call_df['呼叫日期_dt'].dt.to_period('M')
                call_monthly = call_df.groupby('年月').agg(
                    通话次数=('呼叫日期', 'count'),
                    通话对手数=('对方姓名', 'nunique') if '对方姓名' in call_df.columns else ('呼叫日期', 'count'),
                ).reset_index()

                num_months = len(call_monthly)
                if num_months > 0:
                    baseline.月均通话次数 = round(float(call_monthly['通话次数'].mean()), 1)
                    baseline.月均通话对手数 = round(float(call_monthly['通话对手数'].mean()), 1)

                # 通话时长
                if '通话时长' in call_df.columns:
                    total_seconds = float(call_df['通话时长'].fillna(0).sum())
                    baseline.月均通话时长分钟 = round(total_seconds / 60 / max(num_months, 1), 1)

    # ------------------------------------------------------------------
    # 公共工具方法
    # ------------------------------------------------------------------
    @staticmethod
    def is_anomaly(value: float, baseline_mean: float, baseline_std: float,
                   threshold_sigma: float = 2.0) -> tuple[bool, float]:
        """
        判断某值是否偏离基线异常

        Args:
            value: 当前值
            baseline_mean: 基线均值
            baseline_std: 基线标准差
            threshold_sigma: 阈值σ倍数
        Returns:
            (是否异常, 偏离σ倍数)
        """
        if baseline_std == 0:
            if value != baseline_mean and baseline_mean == 0:
                return (True, float('inf') if value != 0 else 0)
            if baseline_mean > 0:
                ratio = abs(value - baseline_mean) / baseline_mean
                return (ratio > 0.5, round(ratio, 2))  # 均值非零但标准差为零时用比例判断
            return (value != baseline_mean, 0)
        sigma = abs(value - baseline_mean) / baseline_std
        return (sigma > threshold_sigma, round(sigma, 2))
