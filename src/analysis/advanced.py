"""高级分析器 - 时间模式/金额模式/异常检测/交易模式 + 偏离度+规避+突增+对手异常"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional
from datetime import datetime

from src.models.analysis_result import AdvancedAnalysisResult, AnomalyItem, PersonBaseline
from src.analysis.baseline import BaselineAnalyzer


class AdvancedAnalyzer:
    """高级分析器 - 升级版，新增偏离度+规避行为+突增异常+对手异常"""

    def __init__(self, thresholds):
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(self, all_data: dict[str, pd.DataFrame],
                baseline: Optional[dict[str, PersonBaseline]] = None) -> dict[str, AdvancedAnalysisResult]:
        """
        执行高级分析

        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ...}
            baseline: {本方姓名: PersonBaseline} 用于计算偏离度
        Returns:
            {本方姓名: AdvancedAnalysisResult}
        """
        result: dict[str, AdvancedAnalysisResult] = {}

        all_persons_data = self._collect_person_data(all_data)
        baseline = baseline or {}

        for person, df in all_persons_data.items():
            if df is None or df.empty:
                continue

            bl = baseline.get(person)

            time_patterns = self._analyze_time_patterns(df)
            amount_patterns = self._analyze_amount_patterns(df)
            anomalies = self._detect_anomalies(df)
            trade_patterns = self._analyze_trade_patterns(df)

            result[person] = AdvancedAnalysisResult(
                时间模式=time_patterns,
                金额模式=amount_patterns,
                异常交易=anomalies,
                交易模式=trade_patterns,
            )

        return result

    # ------------------------------------------------------------------
    # 新增：异常检测（带偏离度+规避+突增+对手异常）
    # ------------------------------------------------------------------
    def detect_anomalies_with_baseline(
        self, all_data: dict, baseline: dict[str, PersonBaseline]
    ) -> dict[str, list[AnomalyItem]]:
        """
        基于基线的异常检测（新方法，返回 AnomalyItem 列表）

        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ...}
            baseline: {本方姓名: PersonBaseline}
        Returns:
            {本方姓名: [AnomalyItem]}
        """
        result: dict[str, list[AnomalyItem]] = {}
        all_persons_data = self._collect_person_data(all_data)

        for person, df in all_persons_data.items():
            if df is None or df.empty:
                continue

            bl = baseline.get(person)
            anomalies = []

            # 1. 资金异常（月度偏离）
            anomalies.extend(self._detect_fund_anomaly(person, df, bl))

            # 2. 时间异常（深夜/节假日交易）
            anomalies.extend(self._detect_time_anomaly(person, df, bl))

            # 3. 频率异常（突增突减）
            anomalies.extend(self._detect_frequency_anomaly(person, df, bl))

            # 4. 行为异常（规避/拆分/集中）
            anomalies.extend(self._detect_behavior_anomaly(person, df))

            # 5. 对手异常（新对手/消失对手）
            anomalies.extend(self._detect_opponent_anomaly(person, df, bl))

            # 按严重程度排序
            severity_order = {'高': 0, '中': 1, '低': 2}
            anomalies.sort(key=lambda a: severity_order.get(a.严重程度, 3))

            result[person] = anomalies

        return result

    # ------------------------------------------------------------------
    # 收集所有人员数据
    # ------------------------------------------------------------------
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

        result = {}
        for person, dfs in person_data.items():
            result[person] = pd.concat(dfs, ignore_index=True)
        return result

    # ------------------------------------------------------------------
    # 资金异常检测（月度偏离）
    # ------------------------------------------------------------------
    def _detect_fund_anomaly(self, person: str, df: pd.DataFrame,
                              bl: Optional[PersonBaseline]) -> list[AnomalyItem]:
        anomalies = []
        if bl is None or not bl.月度明细:
            return anomalies
        if '交易日期' not in df.columns:
            return anomalies

        df = df.copy()
        df['交易日期_dt'] = pd.to_datetime(df['交易日期'], errors='coerce')
        df['年月'] = df['交易日期_dt'].dt.to_period('M')

        # 逐月对比基线
        sigma_threshold = self.thresholds.get("analysis.anomaly.sigma_threshold", 2.0) if self.thresholds else 2.0

        for metric in bl.月度明细:
            # 收入偏离
            if bl.月均收入_std > 0:
                income_sigma = abs(metric.收入 - bl.月均收入) / bl.月均收入_std
                if income_sigma > sigma_threshold:
                    severity = '高' if income_sigma > 3 else '中'
                    anomalies.append(AnomalyItem(
                        异常类型="资金异常",
                        异常子类="月度收入突增" if metric.收入 > bl.月均收入 else "月度收入骤降",
                        偏离度=round(income_sigma, 2),
                        严重程度=severity,
                        本方姓名=person,
                        对方姓名="",
                        交易日期=metric.年月,
                        交易金额=metric.收入,
                        基线均值=bl.月均收入,
                        说明=f"{metric.年月}收入{metric.收入:,.0f}元，偏离基线均值{bl.月均收入:,.0f}元{income_sigma:.1f}σ",
                    ))

            # 支出偏离
            if bl.月均支出_std > 0:
                expense_sigma = abs(metric.支出 - bl.月均支出) / bl.月均支出_std
                if expense_sigma > sigma_threshold:
                    severity = '高' if expense_sigma > 3 else '中'
                    anomalies.append(AnomalyItem(
                        异常类型="资金异常",
                        异常子类="月度支出突增" if metric.支出 > bl.月均支出 else "月度支出骤降",
                        偏离度=round(expense_sigma, 2),
                        严重程度=severity,
                        本方姓名=person,
                        对方姓名="",
                        交易日期=metric.年月,
                        交易金额=metric.支出,
                        基线均值=bl.月均支出,
                        说明=f"{metric.年月}支出{metric.支出:,.0f}元，偏离基线均值{bl.月均支出:,.0f}元{expense_sigma:.1f}σ",
                    ))

        return anomalies

    # ------------------------------------------------------------------
    # 时间异常检测
    # ------------------------------------------------------------------
    def _detect_time_anomaly(self, person: str, df: pd.DataFrame,
                              bl: Optional[PersonBaseline]) -> list[AnomalyItem]:
        anomalies = []
        if '交易日期' not in df.columns:
            return anomalies

        df = df.copy()
        df['交易日期_dt'] = pd.to_datetime(df['交易日期'], errors='coerce')
        df = df.dropna(subset=['交易日期_dt'])

        if df.empty:
            return anomalies

        # 深夜交易(22-6点)
        hours = df['交易日期_dt'].dt.hour
        late_mask = (hours >= 22) | (hours < 6)
        late_trades = df[late_mask]

        if len(late_trades) > 0:
            late_pct = len(late_trades) / len(df) * 100
            # 如果深夜比例超过基线2倍
            is_anomaly = bl is not None and bl.深夜交易占比 > 0 and late_pct > bl.深夜交易占比 * 2
            is_anomaly = is_anomaly or (late_pct > 10)  # 绝对值>10%也标记

            if is_anomaly:
                severity = '中' if late_pct < 20 else '高'
                anomalies.append(AnomalyItem(
                    异常类型="时间异常",
                    异常子类="深夜交易",
                    偏离度=-1,
                    严重程度=severity,
                    本方姓名=person,
                    对方姓名="",
                    交易日期="",
                    交易金额=float(late_trades['交易金额'].abs().sum()) if '交易金额' in late_trades.columns else 0,
                    基线均值=bl.深夜交易占比 if bl else 0,
                    说明=f"深夜(22-6点)交易{len(late_trades)}笔，占比{late_pct:.1f}%{(f'，基线{bl.深夜交易占比:.1f}%' if bl and bl.深夜交易占比 > 0 else '')}",
                ))

        return anomalies

    # ------------------------------------------------------------------
    # 频率异常检测
    # ------------------------------------------------------------------
    def _detect_frequency_anomaly(self, person: str, df: pd.DataFrame,
                                    bl: Optional[PersonBaseline]) -> list[AnomalyItem]:
        anomalies = []
        if bl is None or not bl.月度明细:
            return anomalies
        if '交易日期' not in df.columns:
            return anomalies

        # 检测交易对手数突增
        sigma_threshold = 2.0

        for metric in bl.月度明细:
            if bl.月均交易对手数_std > 0:
                opponent_sigma = abs(metric.对手数 - bl.月均交易对手数) / bl.月均交易对手数_std
                if opponent_sigma > sigma_threshold and metric.对手数 > bl.月均交易对手数:
                    anomalies.append(AnomalyItem(
                        异常类型="频率异常",
                        异常子类="交易对手数突增",
                        偏离度=round(opponent_sigma, 2),
                        严重程度='中',
                        本方姓名=person,
                        对方姓名="",
                        交易日期=metric.年月,
                        交易金额=0,
                        基线均值=bl.月均交易对手数,
                        说明=f"{metric.年月}交易对手{metric.对手数}人，偏离基线均值{bl.月均交易对手数:.0f}人{opponent_sigma:.1f}σ",
                    ))

        return anomalies

    # ------------------------------------------------------------------
    # 行为异常检测（规避/拆分/集中）
    # ------------------------------------------------------------------
    def _detect_behavior_anomaly(self, person: str, df: pd.DataFrame) -> list[AnomalyItem]:
        anomalies = []
        if '交易金额' not in df.columns:
            return anomalies

        amounts = df['交易金额'].abs()

        # 规避行为：4-5万区间密集交易
        threshold = 50000
        window = 0.8
        min_count = 3
        near_threshold = df[(amounts >= threshold * window) & (amounts < threshold)]

        if len(near_threshold) >= min_count:
            anomalies.append(AnomalyItem(
                异常类型="行为异常",
                异常子类="阈值附近密集交易",
                偏离度=-1,
                严重程度='中',
                本方姓名=person,
                对方姓名="",
                交易日期="",
                交易金额=float(near_threshold['交易金额'].abs().sum()),
                基线均值=0,
                说明=f"在{threshold * window / 10000:.0f}-{threshold / 10000:.0f}万区间有{len(near_threshold)}笔交易，疑似规避大额报告",
            ))

        # 等额拆分：同一对手7天内≥2笔金额差异<5%的交易
        if '对方姓名' in df.columns:
            for opponent, group in df.groupby('对方姓名'):
                if not opponent or len(group) < 2:
                    continue
                sorted_group = group.sort_values('交易日期' if '交易日期' in group.columns else group.columns[0])
                dates_col = sorted_group.get('交易日期')
                if dates_col is None:
                    continue
                dates = pd.to_datetime(dates_col, errors='coerce')
                amts = sorted_group['交易金额'].abs().values

                for i in range(len(sorted_group) - 1):
                    if pd.isna(dates.iloc[i]) or pd.isna(dates.iloc[i + 1]):
                        continue
                    time_gap = (dates.iloc[i + 1] - dates.iloc[i]).days
                    if time_gap <= 7 and amts[i] > 0 and amts[i + 1] > 0:
                        if abs(amts[i] - amts[i + 1]) / max(amts[i], amts[i + 1]) < 0.05:
                            anomalies.append(AnomalyItem(
                                异常类型="行为异常",
                                异常子类="等额拆分交易",
                                偏离度=-1,
                                严重程度='中',
                                本方姓名=person,
                                对方姓名=str(opponent),
                                交易日期=str(dates.iloc[i].date()) if pd.notna(dates.iloc[i]) else "",
                                交易金额=float(amts[i] + amts[i + 1]),
                                基线均值=0,
                                说明=f"与{opponent}在{time_gap}天内有两笔金额相近交易({amts[i]:,.0f}/{amts[i+1]:,.0f}元)",
                            ))
                            break  # 每个对手只报告一次

        return anomalies

    # ------------------------------------------------------------------
    # 对手异常检测（新对手）
    # ------------------------------------------------------------------
    def _detect_opponent_anomaly(self, person: str, df: pd.DataFrame,
                                  bl: Optional[PersonBaseline]) -> list[AnomalyItem]:
        anomalies = []
        if bl is None or '对方姓名' not in df.columns:
            return anomalies
        if '交易金额' not in df.columns:
            return anomalies

        # 获取基线月度对手列表
        baseline_opponents = set()
        if bl.月度明细:
            # 基线期内的对手（前半部分月份）
            mid = len(bl.月度明细) // 2
            early_months = [m.年月 for m in bl.月度明细[:max(mid, 1)]]

        df = df.copy()
        if '交易日期' not in df.columns:
            return anomalies

        df['交易日期_dt'] = pd.to_datetime(df['交易日期'], errors='coerce')
        df['年月'] = df['交易日期_dt'].dt.to_period('M').astype(str)

        # 计算每个对手首次出现的月份
        if df['对方姓名'].isna().all():
            return anomalies

        df_valid = df[df['对方姓名'].notna() & (df['对方姓名'] != '')]
        if df_valid.empty:
            return anomalies

        first_appearance = df_valid.groupby('对方姓名')['年月'].min()

        # 基线的后半段月份
        if bl.月度明细 and len(bl.月度明细) >= 3:
            mid = len(bl.月度明细) // 2
            recent_months = set(str(m.年月) for m in bl.月度明细[mid:])
            early_months = set(str(m.年月) for m in bl.月度明细[:mid])

            for opp, first_month in first_appearance.items():
                if first_month in recent_months and first_month not in early_months:
                    # 新出现的对手
                    opp_data = df_valid[df_valid['对方姓名'] == opp]
                    max_amount = opp_data['交易金额'].abs().max()
                    total_amount = opp_data['交易金额'].abs().sum()

                    # 新对手+大额=异常
                    if max_amount > bl.单笔金额_P75:
                        anomalies.append(AnomalyItem(
                            异常类型="对手异常",
                            异常子类="大额新对手",
                            偏离度=-1,
                            严重程度='中',
                            本方姓名=person,
                            对方姓名=str(opp),
                            交易日期=first_month,
                            交易金额=float(total_amount),
                            基线均值=bl.单笔金额_P75,
                            说明=f"{first_month}新增对手{opp}，单笔最大{max_amount:,.0f}元，总金额{total_amount:,.0f}元，超过基线P75({bl.单笔金额_P75:,.0f}元)",
                        ))

        return anomalies[:10]  # 限制数量

    # ------------------------------------------------------------------
    # 原有方法（保持不变）
    # ------------------------------------------------------------------
    def _analyze_time_patterns(self, df: pd.DataFrame) -> list[dict]:
        patterns = []
        if "交易日期" not in df.columns or df.empty:
            return patterns

        dates = pd.to_datetime(df["交易日期"], errors="coerce").dropna()
        if dates.empty:
            return patterns

        # 工作日 vs 周末
        weekdays = dates[dates.dt.weekday < 5]
        weekends = dates[dates.dt.weekday >= 5]
        patterns.append({
            "类型": "工作日vs周末",
            "工作日交易数": len(weekdays),
            "周末交易数": len(weekends),
            "工作日占比": round(len(weekdays) / len(dates) * 100, 2) if len(dates) > 0 else 0,
        })

        # 工作时间 vs 非工作时间
        working_hours_start = self.thresholds.get("analysis.advanced.time_analysis.working_hours_start", 9)
        working_hours_end = self.thresholds.get("analysis.advanced.time_analysis.working_hours_end", 17)

        time_col = df.get("交易时间")
        if time_col is not None:
            try:
                times = pd.to_datetime(time_col, errors="coerce").dropna()
                if not times.empty:
                    hours = times.dt.hour
                    working = hours[(hours >= working_hours_start) & (hours <= working_hours_end)]
                    non_working = hours[(hours < working_hours_start) | (hours > working_hours_end)]
                    patterns.append({
                        "类型": "工作时间vs非工作时间",
                        "工作时间交易数": len(working),
                        "非工作时间交易数": len(non_working),
                        "工作时间占比": round(len(working) / len(hours) * 100, 2) if len(hours) > 0 else 0,
                    })
            except Exception:
                pass

        # 活跃时段 TOP5
        hourly = dates.dt.hour.value_counts().head(5)
        patterns.append({
            "类型": "活跃时段",
            "数据": {str(h): int(c) for h, c in hourly.items()},
        })

        # 活跃月份
        monthly = dates.dt.month.value_counts().sort_index()
        patterns.append({
            "类型": "活跃月份",
            "数据": {str(m): int(c) for m, c in monthly.items()},
        })

        return patterns

    def _analyze_amount_patterns(self, df: pd.DataFrame) -> list[dict]:
        patterns = []
        if "交易金额" not in df.columns or df.empty:
            return patterns

        amounts = df["交易金额"].abs()

        # 金额区间
        ranges = self.thresholds.get("analysis.advanced.amount_analysis.ranges", [
            {"name": "小额", "min": 0, "max": 1000},
            {"name": "中额", "min": 1000, "max": 10000},
            {"name": "大额", "min": 10000, "max": 100000},
            {"name": "巨额", "min": 100000, "max": 999999999},
        ])

        range_data = {}
        for r in ranges:
            mask = (amounts >= r["min"]) & (amounts < r["max"])
            count = int(mask.sum())
            total = float(amounts[mask].sum())
            avg = float(amounts[mask].mean()) if count > 0 else 0
            pct = round(count / len(amounts) * 100, 2) if len(amounts) > 0 else 0
            range_data[r["name"]] = {
                "交易数": count,
                "总金额": round(total, 2),
                "平均金额": round(avg, 2),
                "占比": pct,
            }

        patterns.append({"类型": "金额区间分布", "数据": range_data})

        # 整数金额偏好
        total_count = len(amounts)
        round_100 = int((amounts % 100 == 0).sum())
        round_1000 = int((amounts % 1000 == 0).sum())
        round_10000 = int((amounts % 10000 == 0).sum())

        patterns.append({
            "类型": "整数金额偏好",
            "整百金额占比": round(round_100 / total_count * 100, 2) if total_count > 0 else 0,
            "整千金额占比": round(round_1000 / total_count * 100, 2) if total_count > 0 else 0,
            "整万金额占比": round(round_10000 / total_count * 100, 2) if total_count > 0 else 0,
        })

        # 金额统计
        patterns.append({
            "类型": "金额统计",
            "最大金额": float(amounts.max()),
            "最小金额": float(amounts.min()),
            "平均金额": round(float(amounts.mean()), 2),
            "中位数金额": round(float(amounts.median()), 2),
            "标准差": round(float(amounts.std()), 2),
            "总金额": round(float(amounts.sum()), 2),
        })

        return patterns

    def _detect_anomalies(self, df: pd.DataFrame) -> list[dict]:
        anomalies = []
        if df.empty:
            return anomalies

        freq_threshold = self.thresholds.get("analysis.advanced.anomaly_detection.frequency_threshold", 10)
        amount_std_threshold = self.thresholds.get("analysis.advanced.anomaly_detection.amount_std_threshold", 3)
        time_gap_threshold = self.thresholds.get("analysis.advanced.anomaly_detection.time_gap_threshold_hours", 1)

        if "交易金额" in df.columns:
            amounts = df["交易金额"].abs()
            mean_amt = amounts.mean()
            std_amt = amounts.std()

            if std_amt and std_amt > 0:
                z_scores = (amounts - mean_amt) / std_amt
                outlier_mask = z_scores.abs() > amount_std_threshold
                outlier_count = int(outlier_mask.sum())
                if outlier_count > 0:
                    anomalies.append({
                        "类型": "金额异常",
                        "异常交易数": outlier_count,
                        "风险等级": "高",
                        "说明": f"有{outlier_count}笔交易金额偏离平均值超过{amount_std_threshold}个标准差",
                    })

        # 高频交易检测
        if "对方姓名" in df.columns and "本方姓名" in df.columns:
            freq = df.groupby(["本方姓名", "对方姓名"]).size()
            high_freq = freq[freq > freq_threshold]
            if not high_freq.empty:
                risk = "高" if high_freq.max() > 20 else "中" if high_freq.max() > 15 else "低"
                anomalies.append({
                    "类型": "高频交易",
                    "高频交易对数": len(high_freq),
                    "风险等级": risk,
                    "说明": f"有{len(high_freq)}对交易频率超过{freq_threshold}次",
                })

        # 时间间隔异常
        if "交易日期" in df.columns:
            dates = pd.to_datetime(df["交易日期"], errors="coerce").dropna().sort_values()
            if len(dates) >= 2:
                gaps = dates.diff().dropna()
                short_gaps = gaps[gaps.dt.total_seconds() < time_gap_threshold * 3600]
                if len(short_gaps) > 0:
                    anomalies.append({
                        "类型": "时间间隔异常",
                        "短间隔交易数": len(short_gaps),
                        "风险等级": "中",
                        "说明": f"有{len(short_gaps)}笔交易间隔小于{time_gap_threshold}小时",
                    })

        return anomalies

    def _analyze_trade_patterns(self, df: pd.DataFrame) -> list[dict]:
        patterns = []
        if df.empty:
            return patterns

        if "交易金额" not in df.columns:
            return patterns

        amounts = df["交易金额"].abs()
        total_count = len(amounts)

        # 整数金额偏好
        round_100_pct = (amounts % 100 == 0).sum() / total_count if total_count > 0 else 0
        if round_100_pct > 0.8:
            round_pref = "强"
        elif round_100_pct > 0.5:
            round_pref = "中"
        else:
            round_pref = "弱"

        patterns.append({
            "指标": "整数金额偏好",
            "值": f"{round_100_pct * 100:.1f}%",
            "分级": round_pref,
        })

        # 金额稳定性
        mean_amt = amounts.mean() if total_count > 0 else 0
        std_amt = amounts.std() if total_count > 1 else 0
        cv = std_amt / mean_amt if mean_amt > 0 else 0

        if cv < 0.5:
            stability = "稳定"
        elif cv < 1.0:
            stability = "一般"
        else:
            stability = "波动大"

        patterns.append({
            "指标": "金额稳定性",
            "值": f"变异系数={cv:.2f}",
            "分级": stability,
        })

        # 活跃度
        if total_count > 50:
            activity = "高"
        elif total_count > 20:
            activity = "中"
        else:
            activity = "低"

        patterns.append({
            "指标": "活跃度",
            "值": f"交易{total_count}次",
            "分级": activity,
        })

        # 行为特征描述
        features = []
        if round_100_pct > 0.8:
            features.append("偏好整数金额")
        if mean_amt > 50000:
            features.append("大额交易为主")
        elif mean_amt < 1000:
            features.append("小额交易为主")
        if cv < 0.3:
            features.append("金额稳定")
        elif cv > 2.0:
            features.append("金额波动大")

        if features:
            patterns.append({
                "指标": "行为特征",
                "值": "；".join(features),
                "分级": "",
            })

        return patterns
