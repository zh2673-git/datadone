"""时序分析器 - 构建通话与资金的时序关联"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from src.models.analysis_result import TimelineEvent, TimelineChain


class TimelineAnalyzer:
    """时序分析器 - 构建通讯-资金时序链"""

    def __init__(self, thresholds=None):
        self.thresholds = thresholds
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def analyze(self, all_data: dict, call_data=None) -> dict[str, list[TimelineChain]]:
        """
        为每个人构建时序链

        Args:
            all_data: {'银行': {本方姓名: df}, '微信': ..., '支付宝': ...}
            call_data: 话单DataFrame
        Returns:
            {本方姓名: [TimelineChain]}
        """
        result: dict[str, list[TimelineChain]] = {}

        if call_data is None or (isinstance(call_data, pd.DataFrame) and call_data.empty):
            return result

        # 收集人员数据
        all_persons_data = self._collect_person_data(all_data)

        # 获取所有人名
        persons = set(all_persons_data.keys())
        if isinstance(call_data, pd.DataFrame) and '本方姓名' in call_data.columns:
            persons.update(call_data['本方姓名'].dropna().unique())

        window_hours = self.thresholds.get("analysis.timeline.window_hours", 24) if self.thresholds else 24
        min_repeats = self.thresholds.get("analysis.timeline.min_repeats", 2) if self.thresholds else 2

        for person in persons:
            df = all_persons_data.get(person)
            person_call = self._get_person_call_data(person, call_data)

            if df is None or df.empty or person_call is None or person_call.empty:
                continue

            chains = self._detect_chains(person, df, person_call, window_hours, min_repeats)
            if chains:
                result[person] = chains

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

    def _get_person_call_data(self, person: str, call_data) -> Optional[pd.DataFrame]:
        if call_data is None:
            return None
        if isinstance(call_data, dict):
            return call_data.get(person)
        if isinstance(call_data, pd.DataFrame) and not call_data.empty:
            if '本方姓名' in call_data.columns:
                return call_data[call_data['本方姓名'] == person].copy()
        return None

    # ------------------------------------------------------------------
    # 时序链检测
    # ------------------------------------------------------------------
    def _detect_chains(self, person: str, df: pd.DataFrame,
                       call_df: pd.DataFrame, window_hours: int,
                       min_repeats: int) -> list[TimelineChain]:
        """检测通讯-资金时序链"""
        chains = []

        # 准备数据
        df = df.copy()
        if '交易日期' not in df.columns or '对方姓名' not in df.columns:
            return chains
        df['交易日期_dt'] = pd.to_datetime(df['交易日期'], errors='coerce')
        df = df.dropna(subset=['交易日期_dt', '对方姓名'])
        df = df[df['对方姓名'] != '']

        call_df = call_df.copy()
        if '呼叫日期' not in call_df.columns or '对方姓名' not in call_df.columns:
            return chains
        call_df['呼叫日期_dt'] = pd.to_datetime(call_df['呼叫日期'], errors='coerce')
        call_df = call_df.dropna(subset=['呼叫日期_dt', '对方姓名'])
        call_df = call_df[call_df['对方姓名'] != '']

        if df.empty or call_df.empty:
            return chains

        # 找出在话单和账单中共同出现的对手
        bill_opponents = set(df['对方姓名'].unique())
        call_opponents = set(call_df['对方姓名'].unique())
        common_opponents = bill_opponents & call_opponents

        for opponent in common_opponents:
            opp_chains = self._detect_chains_for_opponent(
                person, opponent, df, call_df, window_hours
            )
            chains.extend(opp_chains)

        # 按重复次数排序
        chains.sort(key=lambda c: c.重复次数, reverse=True)

        return chains[:30]  # 限制输出数量

    def _detect_chains_for_opponent(
        self, person: str, opponent: str,
        df: pd.DataFrame, call_df: pd.DataFrame,
        window_hours: int
    ) -> list[TimelineChain]:
        """检测与特定对手的时序链"""
        chains = []

        # 获取该对手的通话和资金事件
        opp_calls = call_df[call_df['对方姓名'] == opponent].sort_values('呼叫日期_dt')
        opp_bills = df[df['对方姓名'] == opponent].sort_values('交易日期_dt')

        if opp_calls.empty or opp_bills.empty:
            return chains

        # 构建统一事件列表
        events = []
        for _, row in opp_calls.iterrows():
            call_type = row.get('呼叫类型', '')
            duration = row.get('通话时长', 0)
            data_source = row.get('数据来源', '')
            events.append({
                '时间': row['呼叫日期_dt'],
                '事件类型': '通话',
                '方向': str(call_type),
                '金额': float(duration) if pd.notna(duration) else 0,
                '平台': '话单',
                '备注': data_source,
            })

        for _, row in opp_bills.iterrows():
            direction = row.get('借贷标识', '')
            amount = abs(row['交易金额']) if '交易金额' in row and pd.notna(row['交易金额']) else 0
            income = row.get('收入金额', 0)
            expense = row.get('支出金额', 0)
            platform = row.get('_平台', '')

            if income and float(income) > 0:
                direction_str = '收入'
            elif expense and float(expense) > 0:
                direction_str = '支出'
            else:
                direction_str = str(direction)

            events.append({
                '时间': row['交易日期_dt'],
                '事件类型': '资金',
                '方向': direction_str,
                '金额': float(amount),
                '平台': platform,
                '备注': '',
            })

        # 按时间排序
        events.sort(key=lambda e: e['时间'])

        # 滑动窗口检测"通话→资金"模式
        raw_chains = []
        for i in range(len(events)):
            if events[i]['事件类型'] != '通话':
                continue

            call_time = events[i]['时间']
            # 查找后续window_hours小时内的资金事件
            for j in range(i + 1, len(events)):
                time_diff = (events[j]['时间'] - call_time).total_seconds() / 3600
                if time_diff > window_hours:
                    break
                if events[j]['事件类型'] == '资金':
                    # 找到"通话→资金"
                    chain_events = [events[i], events[j]]

                    # 查找资金后window_hours内的通话（三段链）
                    for k in range(j + 1, len(events)):
                        time_diff2 = (events[k]['时间'] - events[j]['时间']).total_seconds() / 3600
                        if time_diff2 > window_hours:
                            break
                        if events[k]['事件类型'] == '通话':
                            chain_events.append(events[k])
                            break

                    chain_mode = "通话→资金→通话" if len(chain_events) == 3 else "通话→资金"
                    raw_chains.append({
                        'mode': chain_mode,
                        'events': chain_events,
                        'call_time': call_time,
                    })

        if not raw_chains:
            return chains

        # 统计重复次数
        two_seg = [c for c in raw_chains if c['mode'] == "通话→资金"]
        three_seg = [c for c in raw_chains if c['mode'] == "通话→资金→通话"]

        # 评估链强度
        for chain_info in raw_chains:
            mode = chain_info['mode']
            repeat_count = len(three_seg) if mode == "通话→资金→通话" else len(two_seg)

            if mode == "通话→资金→通话" and repeat_count >= 3:
                strength = "很强"
            elif mode == "通话→资金→通话" and repeat_count >= 2:
                strength = "强"
            elif mode == "通话→资金→通话":
                strength = "中"
            elif repeat_count >= 3:
                strength = "中"
            else:
                strength = "弱"

            # 只保留中及以上强度的链
            if strength in ("弱",) and len(chains) >= 5:
                continue

            timeline_events = []
            for evt in chain_info['events']:
                timeline_events.append(TimelineEvent(
                    时间=evt['时间'].strftime('%Y-%m-%d %H:%M'),
                    事件类型=evt['事件类型'],
                    方向=evt['方向'],
                    金额=evt['金额'],
                    对手=opponent,
                    平台=evt['平台'],
                    备注=evt.get('备注', ''),
                ))

            # 证据描述
            evt_desc_parts = []
            for evt in chain_info['events']:
                if evt['事件类型'] == '通话':
                    evt_desc_parts.append(
                        f"{evt['时间'].strftime('%Y-%m-%d %H:%M')}通话({evt['方向']})"
                    )
                else:
                    evt_desc_parts.append(
                        f"{evt['时间'].strftime('%Y-%m-%d %H:%M')}{evt['平台']}{evt['方向']}{evt['金额']:,.0f}元"
                    )

            chains.append(TimelineChain(
                本方姓名=person,
                对方姓名=opponent,
                事件序列=timeline_events,
                链模式=mode,
                链强度=strength,
                重复次数=repeat_count,
                时间窗口=window_hours,
                关键证据描述=' → '.join(evt_desc_parts),
            ))

        return chains
