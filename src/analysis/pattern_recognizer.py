"""行为模式识别器 - 基于四类数据的5种模式匹配"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from src.models.analysis_result import PersonBaseline, AnomalyItem, PatternMatch


class PatternRecognizer:
    """行为模式识别器 - 5种行为模式匹配"""

    def __init__(self, thresholds=None):
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(
        self,
        all_data: dict,
        call_data=None,
        baseline: Optional[dict[str, PersonBaseline]] = None,
        anomalies: Optional[dict[str, list[AnomalyItem]]] = None,
    ) -> dict[str, list[PatternMatch]]:
        """
        对每个人匹配行为模式

        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ...}
            call_data: 话单DataFrame
            baseline: {本方姓名: PersonBaseline}
            anomalies: {本方姓名: [AnomalyItem]}
        Returns:
            {本方姓名: [PatternMatch]}
        """
        import gc
        result: dict[str, list[PatternMatch]] = {}

        # 收集人员数据
        all_persons_data = self._collect_person_data(all_data)
        baseline = baseline or {}
        anomalies = anomalies or {}

        # 预处理话单：按人员分组，避免每人复制整个话单导致内存爆炸
        call_by_person: dict[str, pd.DataFrame] = {}
        if call_data is not None and isinstance(call_data, pd.DataFrame) and not call_data.empty:
            if '本方姓名' in call_data.columns:
                # 预转换日期列，避免在循环里反复转换
                call_prep = call_data.copy()
                if '呼叫日期' in call_prep.columns:
                    call_prep['呼叫日期_dt'] = pd.to_datetime(call_prep['呼叫日期'], errors='coerce')
                    call_prep = call_prep.dropna(subset=['呼叫日期_dt'])
                for person_name, group in call_prep.groupby('本方姓名'):
                    call_by_person[str(person_name)] = group
                del call_prep
            else:
                # 无本方姓名列，作为公共数据保留
                call_by_person['__all__'] = call_data

        total = len(all_persons_data)
        for idx, (person, df) in enumerate(all_persons_data.items(), 1):
            patterns = []
            bl = baseline.get(person)

            # 取该人员自己的话单（不再复制整个话单）
            person_call = call_by_person.get(person)
            if person_call is None:
                person_call = call_by_person.get('__all__')

            try:
                # P1: 周期性收入
                patterns.extend(self._check_periodic_income(person, df, bl))

                # P2: 资金中转
                patterns.extend(self._check_fund_transfer(person, df, bl))

                # P3: 通讯-资金关联
                patterns.extend(self._check_comm_fund_link(person, df, person_call, bl))

                # P4: 规避检测
                patterns.extend(self._check_threshold_avoidance(person, df))

                # P5: 特殊关系
                patterns.extend(self._check_special_relation(person, df, person_call))
            except Exception as e:
                self.logger.warning(f"人员 {person} 行为模式识别失败，已跳过: {e}")

            result[person] = patterns

            # 每20人强制回收内存，避免大循环内存累积
            if idx % 20 == 0:
                gc.collect()
                self.logger.info(f"  行为模式识别进度: {idx}/{total}")

        return result

    # ------------------------------------------------------------------
    # 收集人员数据
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

    # ------------------------------------------------------------------
    # P1: 周期性收入模式
    # ------------------------------------------------------------------
    def _check_periodic_income(self, person: str, df: pd.DataFrame,
                                bl: Optional[PersonBaseline]) -> list[PatternMatch]:
        """P1: 与特定对手存在≥3个月的定期收入"""
        matches = []
        if '对方姓名' not in df.columns or '收入金额' not in df.columns:
            return matches

        # 只看转账收入
        income_df = df[(df['收入金额'] > 0) & (df['对方姓名'].notna()) & (df['对方姓名'] != '')]
        if income_df.empty:
            return matches

        # 按对手分组检查周期性
        for opponent, group in income_df.groupby('对方姓名'):
            if not opponent or len(group) < 3:
                continue

            # 按月统计收入
            group = group.copy()
            if '交易日期' not in group.columns:
                continue
            group['年月'] = pd.to_datetime(group['交易日期'], errors='coerce').dt.to_period('M')
            monthly = group.groupby('年月')['收入金额'].sum().reset_index()

            if len(monthly) < 3:  # C1: ≥3个月
                continue

            amounts = monthly['收入金额'].values
            mean_amt = np.mean(amounts)
            std_amt = np.std(amounts)

            # C2: 金额稳定性（变异系数<30%）
            c2 = (std_amt / mean_amt < 0.3) if mean_amt > 0 else False

            # C3: 时间稳定性
            months = monthly['年月'].astype(str).tolist()
            c3 = False
            if len(months) >= 3:
                intervals = []
                for i in range(1, len(months)):
                    try:
                        m1 = pd.Period(months[i - 1])
                        m2 = pd.Period(months[i])
                        intervals.append((m2 - m1).n)
                    except Exception:
                        pass
                if intervals:
                    interval_std = np.std(intervals)
                    c3 = interval_std < 2  # 月份间隔标准差<2个月

            # C4: 偏离基线
            c4 = False
            if bl and bl.月均收入 > 0:
                c4 = mean_amt > bl.月均收入 * 0.2

            # 匹配度
            conditions_met = sum([c2, c3, c4])
            match_score = min(0.6 + 0.15 * conditions_met, 1.0)  # 60%-100%

            satisfied = []
            unsatisfied = []
            if c2:
                satisfied.append("C2:金额稳定")
            else:
                unsatisfied.append("C2:金额稳定")
            if c3:
                satisfied.append("C3:时间稳定")
            else:
                unsatisfied.append("C3:时间稳定")
            if c4:
                satisfied.append("C4:偏离基线")
            else:
                unsatisfied.append("C4:偏离基线")

            matches.append(PatternMatch(
                模式编号="P1",
                模式名称="周期性收入模式",
                匹配度=round(match_score, 2),
                涉及对手=str(opponent),
                关键证据=f'与{opponent}有{len(monthly)}个月定期收入，月均{mean_amt:,.0f}元',
                满足条件=satisfied,
                未满足条件=unsatisfied,
                报告用语="存在周期性收入特征",
            ))

        return matches

    # ------------------------------------------------------------------
    # P2: 资金中转模式
    # ------------------------------------------------------------------
    def _check_fund_transfer(self, person: str, df: pd.DataFrame,
                              bl: Optional[PersonBaseline]) -> list[PatternMatch]:
        """P2: 大额转入后48小时内取现或转出≥转入金额的60%"""
        matches = []
        if '交易日期' not in df.columns or '交易金额' not in df.columns:
            return matches

        df = df.copy()
        df['交易日期_dt'] = pd.to_datetime(df['交易日期'], errors='coerce')
        df = df.dropna(subset=['交易日期_dt'])

        if df.empty:
            return matches

        # 大额转入（收入>5万）
        threshold = self.thresholds.get("analysis.pattern.fund_transfer_threshold", 50000) if self.thresholds else 50000
        window_hours = self.thresholds.get("analysis.pattern.fund_transfer_window_hours", 48) if self.thresholds else 48
        flow_ratio = self.thresholds.get("analysis.pattern.fund_transfer_flow_ratio", 0.6) if self.thresholds else 0.6

        large_income = df[(df['收入金额'] >= threshold)]
        if large_income.empty:
            return matches

        for idx, row in large_income.iterrows():
            income_time = row['交易日期_dt']
            income_amount = row['收入金额']
            window_end = income_time + timedelta(hours=window_hours)

            # 查找48小时内的取现/转出
            after = df[(df['交易日期_dt'] > income_time) & (df['交易日期_dt'] <= window_end)]
            outflow = after['支出金额'].sum()

            if outflow >= income_amount * flow_ratio:
                # 判断流出方式
                cash_out = 0.0
                transfer_out = 0.0
                if '存取现标识' in after.columns:
                    cash_rows = after[after['存取现标识'] == '取现']
                    cash_out = cash_rows['支出金额'].sum()

                matches.append(PatternMatch(
                    模式编号="P2",
                    模式名称="资金中转模式",
                    匹配度=0.8 if cash_out > 0 else 0.6,
                    涉及对手="",
                    关键证据=f'收入{income_amount:,.0f}元后{window_hours}小时内流出{outflow:,.0f}元(取现{cash_out:,.0f}元/转账{transfer_out:,.0f}元)',
                    满足条件=["C1:大额转入", "C2:快速流出"],
                    未满足条件=[],
                    报告用语="存在资金快速中转特征",
                ))

        # 限制输出数量
        return matches[:10]

    # ------------------------------------------------------------------
    # P3: 通讯-资金关联模式
    # ------------------------------------------------------------------
    def _check_comm_fund_link(self, person: str, df: pd.DataFrame,
                               call_data, bl: Optional[PersonBaseline]) -> list[PatternMatch]:
        """P3: 通话后24小时内与通话对手资金往来≥3次"""
        matches = []
        if call_data is None or (hasattr(call_data, 'empty') and call_data.empty):
            return matches
        if '对方姓名' not in df.columns:
            return matches

        window_hours = self.thresholds.get("analysis.pattern.comm_fund_window_hours", 24) if self.thresholds else 24

        # call_data 已在 analyze() 中按人员预过滤并预转换日期列，这里直接用
        call_df = call_data if isinstance(call_data, pd.DataFrame) else None
        if call_df is None or call_df.empty:
            return matches

        # 兼容：若未预过滤则按人员过滤
        if '本方姓名' in call_df.columns and person is not None:
            # 检查是否已只含该人员
            if call_df['本方姓名'].nunique() > 1:
                call_df = call_df[call_df['本方姓名'] == person]
        if call_df.empty:
            return matches

        if '呼叫日期' not in call_df.columns or '对方姓名' not in call_df.columns:
            return matches

        # 日期列可能已预转换（_呼叫日期_dt），避免重复转换
        if '呼叫日期_dt' not in call_df.columns:
            call_df = call_df.copy()
            call_df['呼叫日期_dt'] = pd.to_datetime(call_df['呼叫日期'], errors='coerce')
            call_df = call_df.dropna(subset=['呼叫日期_dt'])

        # 准备资金数据（复用，不复制整个df）
        if '交易日期_dt' not in df.columns:
            df = df.copy()
            df['交易日期_dt'] = pd.to_datetime(df['交易日期'], errors='coerce')
            df = df.dropna(subset=['交易日期_dt'])

        # 找出在话单和账单中共同出现的对手
        bill_opponents = set(df[df['对方姓名'].notna() & (df['对方姓名'] != '')]['对方姓名'].unique())
        call_opponents = set(call_df[call_df['对方姓名'].notna() & (call_df['对方姓名'] != '')]['对方姓名'].unique())
        common_opponents = bill_opponents & call_opponents

        for opponent in common_opponents:
            # 获取该对手的通话记录
            opp_calls = call_df[call_df['对方姓名'] == opponent].sort_values('呼叫日期_dt')
            # 获取该对手的资金记录
            opp_bills = df[df['对方姓名'] == opponent].sort_values('交易日期_dt')

            if opp_calls.empty or opp_bills.empty:
                continue

            # 检测"通话→N小时内→资金"模式
            link_count = 0
            comm_first_count = 0
            link_amounts = []

            for _, call_row in opp_calls.iterrows():
                call_time = call_row['呼叫日期_dt']
                # 查找通话后window_hours小时内的资金事件
                after_window = opp_bills[
                    (opp_bills['交易日期_dt'] > call_time) &
                    (opp_bills['交易日期_dt'] <= call_time + timedelta(hours=window_hours))
                ]
                if not after_window.empty:
                    link_count += 1
                    comm_first_count += 1
                    link_amounts.append(after_window['交易金额'].abs().sum())

                    # 检查资金后是否有通话（三段链）
                    for _, bill_row in after_window.iterrows():
                        bill_time = bill_row['交易日期_dt']
                        after_call = opp_calls[
                            (opp_calls['呼叫日期_dt'] > bill_time) &
                            (opp_calls['呼叫日期_dt'] <= bill_time + timedelta(hours=window_hours))
                        ]
                        if not after_call.empty:
                            # 找到三段链
                            break

            if link_count < 3:  # C1: ≥3次
                continue

            # C2: 方向判断（通话在先）
            c2 = comm_first_count / link_count > 0.5 if link_count > 0 else False

            # C3: 金额判断
            avg_amount = np.mean(link_amounts) if link_amounts else 0
            c3 = bl and bl.单笔金额均值 > 0 and avg_amount > bl.单笔金额均值

            conditions_met = sum([c2, c3])
            match_score = 0.6 + 0.2 * conditions_met

            satisfied = ["C1:通话后资金≥3次"]
            unsatisfied = []
            if c2:
                satisfied.append("C2:通话在先")
            else:
                unsatisfied.append("C2:通话在先")
            if c3:
                satisfied.append("C3:金额偏离基线")
            else:
                unsatisfied.append("C3:金额偏离基线")

            matches.append(PatternMatch(
                模式编号="P3",
                模式名称="通讯-资金关联模式",
                匹配度=round(match_score, 2),
                涉及对手=str(opponent),
                关键证据=f'与{opponent}有{link_count}次"通话→资金"时序关联',
                满足条件=satisfied,
                未满足条件=unsatisfied,
                报告用语="存在通讯后资金往来关联特征",
            ))

        return matches

    # ------------------------------------------------------------------
    # P4: 规避检测模式
    # ------------------------------------------------------------------
    def _check_threshold_avoidance(self, person: str, df: pd.DataFrame) -> list[PatternMatch]:
        """P4: 阈值附近密集交易/等额拆分"""
        matches = []
        if '交易金额' not in df.columns:
            return matches

        amounts = df['交易金额'].abs()
        threshold = 50000

        # 条件1: 4-5万区间密集交易≥3笔
        near_threshold = df[(amounts >= threshold * 0.8) & (amounts < threshold)]
        c1 = len(near_threshold) >= 3

        # 条件2: 同对手7天内≥2笔金额差异<5%的交易（等额拆分）
        c2 = False
        split_details = []
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
                            c2 = True
                            split_details.append(f'与{opponent}在{time_gap}天内有两笔金额相近交易({amts[i]:,.0f}/{amts[i+1]:,.0f}元)')
                            if len(split_details) >= 3:
                                break
                if len(split_details) >= 3:
                    break

        # 条件3: 单日同对手多笔合计>5万但单笔均<5万
        c3 = False
        if '对方姓名' in df.columns and '交易日期' in df.columns:
            df_copy = df.copy()
            df_copy['交易日期_only'] = pd.to_datetime(df_copy['交易日期'], errors='coerce').dt.date
            for (date_val, opponent), group in df_copy.groupby(['交易日期_only', '对方姓名']):
                if not opponent:
                    continue
                total = group['交易金额'].abs().sum()
                max_single = group['交易金额'].abs().max()
                if total > 50000 and max_single < 50000 and len(group) >= 2:
                    c3 = True
                    break

        # 匹配度
        conditions_met = sum([c1, c2, c3])
        if conditions_met == 0:
            return matches

        match_score = 0.6 + 0.12 * conditions_met  # 60%-96%

        satisfied = []
        unsatisfied = []
        if c1:
            satisfied.append(f"C1:阈值附近密集交易({len(near_threshold)}笔)")
        else:
            unsatisfied.append("C1:阈值附近密集交易")
        if c2:
            satisfied.append("C2:等额拆分交易")
        else:
            unsatisfied.append("C2:等额拆分交易")
        if c3:
            satisfied.append("C3:单日拆分交易")
        else:
            unsatisfied.append("C3:单日拆分交易")

        evidence_parts = []
        if c1:
            evidence_parts.append(f'4-5万区间{len(near_threshold)}笔交易')
        if c2 and split_details:
            evidence_parts.append(split_details[0])
        if c3:
            evidence_parts.append('存在单日拆分')

        matches.append(PatternMatch(
            模式编号="P4",
            模式名称="规避检测模式",
            匹配度=round(match_score, 2),
            涉及对手="",
            关键证据='；'.join(evidence_parts),
            满足条件=satisfied,
            未满足条件=unsatisfied,
            报告用语="存在疑似规避大额交易报告特征",
        ))

        return matches

    # ------------------------------------------------------------------
    # P5: 特殊关系模式
    # ------------------------------------------------------------------
    def _check_special_relation(self, person: str, df: pd.DataFrame,
                                 call_data) -> list[PatternMatch]:
        """P5: 特殊金额+特殊日期+深夜通话的组合"""
        matches = []

        # 条件1: 特殊金额(520/1314/3344等)交易≥2笔
        special_amounts = [520, 521, 1314, 3344, 5200, 13140, 52000, 131400]
        c1 = False
        special_opp = ""
        if '交易金额' in df.columns:
            amounts = df['交易金额'].abs()
            special_tx = df[amounts.isin(special_amounts)]
            if len(special_tx) >= 2:
                c1 = True
                if '对方姓名' in special_tx.columns:
                    opp_counts = special_tx['对方姓名'].value_counts()
                    if not opp_counts.empty:
                        special_opp = str(opp_counts.index[0])

        # 条件2: 特殊日期(情人节/七夕等)交易（依赖SpecialAnalyzer的结果，此处简化检测）
        c2 = False

        # 条件3: 深夜(22-6点)通话≥3次
        c3 = False
        if call_data is not None and isinstance(call_data, pd.DataFrame) and not call_data.empty:
            call_df = call_data
            # 兼容：若未预过滤则按人员过滤
            if '本方姓名' in call_df.columns and person is not None:
                if call_df['本方姓名'].nunique() > 1:
                    call_df = call_df[call_df['本方姓名'] == person]
            if '呼叫日期' in call_df.columns:
                times = pd.to_datetime(call_df['呼叫日期'], errors='coerce')
                if not times.empty:
                    late_calls = times[(times.dt.hour >= 22) | (times.dt.hour < 6)]
                    if len(late_calls) >= 3:
                        c3 = True

        # 匹配度：满足2个条件=60%，3个=85%
        conditions_met = sum([c1, c2, c3])
        if conditions_met < 2:
            return matches

        match_score = 0.6 if conditions_met == 2 else 0.85

        satisfied = []
        unsatisfied = []
        if c1:
            satisfied.append("C1:特殊金额交易≥2笔")
        else:
            unsatisfied.append("C1:特殊金额交易≥2笔")
        if c2:
            satisfied.append("C2:特殊日期交易")
        else:
            unsatisfied.append("C2:特殊日期交易")
        if c3:
            satisfied.append("C3:深夜通话≥3次")
        else:
            unsatisfied.append("C3:深夜通话≥3次")

        evidence_parts = []
        if c1:
            evidence_parts.append(f'特殊金额交易对方含{special_opp}' if special_opp else '特殊金额交易≥2笔')
        if c3:
            evidence_parts.append('深夜通话≥3次')

        matches.append(PatternMatch(
            模式编号="P5",
            模式名称="特殊关系模式",
            匹配度=round(match_score, 2),
            涉及对手=special_opp,
            关键证据='；'.join(evidence_parts),
            满足条件=satisfied,
            未满足条件=unsatisfied,
            报告用语="存在特殊关系资金往来特征",
        ))

        return matches
