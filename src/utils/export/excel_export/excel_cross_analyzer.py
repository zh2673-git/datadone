"""
Excel交叉分析工具模块

提供Excel交叉分析的相关功能，包括话单与账单数据的交叉分析、平台基准分析等。
"""

import pandas as pd
from typing import Dict, Any, List


class ExcelCrossAnalyzer:
    """Excel交叉分析工具类"""
    
    def __init__(self):
        """初始化Excel交叉分析工具"""
        pass
    
    def cross_analyze_with_call_base(self, call_df: pd.DataFrame, bill_df: pd.DataFrame) -> pd.DataFrame:
        """
        以话单为基准进行交叉分析，支持跨数据源对手信息显示
        
        Parameters:
        -----------
        call_df : pd.DataFrame
            话单数据
        bill_df : pd.DataFrame
            账单数据
            
        Returns:
        --------
        pd.DataFrame
            交叉分析结果
        """
        # 以话单数据为基础，不创建额外组合

        # 基于对方姓名进行匹配，并计算各平台的金额分布
        agg_dict = {}
        
        # 安全地添加存在的列
        if '收入总额' in bill_df.columns:
            agg_dict['收入总额'] = 'sum'
        if '支出总额' in bill_df.columns:
            agg_dict['支出总额'] = 'sum'
        if '交易次数' in bill_df.columns:
            agg_dict['交易次数'] = 'sum'
            
        if agg_dict:
            bill_platform_summary = bill_df.groupby(['本方姓名', '对方姓名', '平台']).agg(agg_dict).reset_index()
        else:
            bill_platform_summary = pd.DataFrame()

        # 计算总金额
        total_agg_dict = {}
        if '收入总额' in bill_df.columns:
            total_agg_dict['收入总额'] = 'sum'
        if '支出总额' in bill_df.columns:
            total_agg_dict['支出总额'] = 'sum'
        if '交易次数' in bill_df.columns:
            total_agg_dict['交易次数'] = 'sum'
            
        if total_agg_dict:
            total_agg_dict['平台'] = lambda x: '、'.join(x.unique())
            bill_total_summary = bill_df.groupby(['本方姓名', '对方姓名']).agg(total_agg_dict).reset_index()
        else:
            bill_total_summary = pd.DataFrame()

        # 计算各平台的金额分布
        platform_details = bill_platform_summary.groupby(['本方姓名', '对方姓名']).apply(
            lambda group: self._format_platform_details(group)
        ).reset_index(name='平台金额分布')

        # 为每个平台创建独立字段
        platform_individual_data = {}
        if not bill_platform_summary.empty:
            platforms = bill_platform_summary['平台'].unique()
            for platform in platforms:
                platform_data = bill_platform_summary[bill_platform_summary['平台'] == platform]
                
                # 动态构建聚合字典，只包含存在的列
                agg_dict = {}
                if '收入总额' in platform_data.columns:
                    agg_dict['收入总额'] = 'sum'
                if '支出总额' in platform_data.columns:
                    agg_dict['支出总额'] = 'sum'
                if '交易次数' in platform_data.columns:
                    agg_dict['交易次数'] = 'sum'
                    
                if agg_dict:
                    platform_summary = platform_data.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()

                    # 重命名列以区分不同平台
                    rename_dict = {}
                    if '收入总额' in platform_summary.columns:
                        rename_dict['收入总额'] = f'{platform}_收入总额'
                    if '支出总额' in platform_summary.columns:
                        rename_dict['支出总额'] = f'{platform}_支出总额'
                    if '交易次数' in platform_summary.columns:
                        rename_dict['交易次数'] = f'{platform}_交易次数'
                        
                    if rename_dict:
                        platform_summary = platform_summary.rename(columns=rename_dict)
                        platform_individual_data[platform] = platform_summary

        # 合并总金额和平台详情
        if not bill_total_summary.empty and not platform_details.empty:
            bill_summary_with_details = pd.merge(bill_total_summary, platform_details, on=['本方姓名', '对方姓名'])
        else:
            bill_summary_with_details = bill_total_summary.copy() if not bill_total_summary.empty else pd.DataFrame()

        # 获取话单中的对方详细信息
        agg_dict = {
            '通话次数': 'sum',
            '数据来源': 'first'
        }

        # 检查通话时长列名
        if '通话总时长(分钟)' in call_df.columns:
            agg_dict['通话总时长(分钟)'] = 'sum'
        elif '通话时长' in call_df.columns:
            agg_dict['通话时长'] = 'sum'

        # 安全地添加可选字段
        if '对方号码' in call_df.columns:
            agg_dict['对方号码'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        # 检查带lambda后缀的字段名（来自话单频率分析）
        if '对方单位名称_<lambda>' in call_df.columns:
            agg_dict['对方单位名称_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        elif '对方单位名称' in call_df.columns:
            agg_dict['对方单位名称'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        if '对方职务_<lambda>' in call_df.columns:
            agg_dict['对方职务_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        elif '对方职务' in call_df.columns:
            agg_dict['对方职务'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''

        call_details = call_df.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()

        # 以话单数据为基础进行合并
        merged_df = call_details.copy()

        # 与账单数据合并 - 严格匹配，禁止跨人员关联
        if not bill_summary_with_details.empty:
            # 只进行完全匹配（本方姓名+对方姓名），禁止跨人员匹配
            merged_df = pd.merge(
                merged_df,
                bill_summary_with_details,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

        # 与各平台独立数据合并 - 严格匹配，禁止跨人员关联
        for platform, platform_data in platform_individual_data.items():
            # 只进行完全匹配（本方姓名+对方姓名），禁止跨人员匹配
            merged_df = pd.merge(
                merged_df,
                platform_data,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

        # 填充空值
        if '收入总额' in merged_df.columns:
            merged_df['收入总额'] = merged_df['收入总额'].fillna(0)
        if '支出总额' in merged_df.columns:
            merged_df['支出总额'] = merged_df['支出总额'].fillna(0)
        if '交易次数' in merged_df.columns:
            merged_df['交易次数'] = merged_df['交易次数'].fillna(0)
        if '平台' in merged_df.columns:
            merged_df['平台'] = merged_df['平台'].fillna('无')
        if '平台金额分布' in merged_df.columns:
            merged_df['平台金额分布'] = merged_df['平台金额分布'].fillna('无')

        # 填充各平台的金额字段
        for col in merged_df.columns:
            if any(platform in col for platform in ['银行', '微信', '支付宝']) and any(field in col for field in ['收入总额', '支出总额', '交易次数']):
                merged_df[col] = merged_df[col].fillna(0)

        # 安全地填充可选字段的空值
        if '对方号码' in merged_df.columns:
            merged_df['对方号码'] = merged_df['对方号码'].fillna('')
        if '对方单位名称_<lambda>' in merged_df.columns:
            merged_df['对方单位名称_<lambda>'] = merged_df['对方单位名称_<lambda>'].fillna('')
        elif '对方单位名称' in merged_df.columns:
            merged_df['对方单位名称'] = merged_df['对方单位名称'].fillna('')
        if '对方职务_<lambda>' in merged_df.columns:
            merged_df['对方职务_<lambda>'] = merged_df['对方职务_<lambda>'].fillna('')
        elif '对方职务' in merged_df.columns:
            merged_df['对方职务'] = merged_df['对方职务'].fillna('')

        # 重新排列列的顺序，将对方详细信息放在对方姓名后面
        base_columns = ['本方姓名', '对方姓名']
        detail_columns = []

        # 安全地添加存在的详细信息字段，优先使用带lambda后缀的字段
        if '对方号码' in merged_df.columns:
            detail_columns.append('对方号码')
        if '对方单位名称_<lambda>' in merged_df.columns:
            detail_columns.append('对方单位名称_<lambda>')
        elif '对方单位名称' in merged_df.columns:
            detail_columns.append('对方单位名称')
        if '对方职务_<lambda>' in merged_df.columns:
            detail_columns.append('对方职务_<lambda>')
        elif '对方职务' in merged_df.columns:
            detail_columns.append('对方职务')

        # 话单相关列
        call_columns = []
        if '通话次数' in merged_df.columns:
            call_columns.append('通话次数')
        if '通话总时长(分钟)' in merged_df.columns:
            call_columns.append('通话总时长(分钟)')
        elif '通话时长' in merged_df.columns:
            call_columns.append('通话时长')
        if '数据来源' in merged_df.columns:
            call_columns.append('数据来源')

        # 账单汇总列
        bill_summary_columns = []
        if '收入总额' in merged_df.columns:
            bill_summary_columns.extend(['收入总额', '支出总额', '交易次数'])
        if '平台' in merged_df.columns:
            bill_summary_columns.append('平台')
        if '平台金额分布' in merged_df.columns:
            bill_summary_columns.append('平台金额分布')

        # 各平台独立列（按平台名称排序）
        platform_columns = []
        platforms = ['银行', '微信', '支付宝']
        for platform in platforms:
            for field in ['收入总额', '支出总额', '交易次数']:
                col_name = f'{platform}_{field}'
                if col_name in merged_df.columns:
                    platform_columns.append(col_name)

        # 剩余列
        used_columns = base_columns + detail_columns + call_columns + bill_summary_columns + platform_columns
        remaining_columns = [col for col in merged_df.columns if col not in used_columns]

        final_columns = base_columns + detail_columns + call_columns + bill_summary_columns + platform_columns + remaining_columns
        merged_df = merged_df[[col for col in final_columns if col in merged_df.columns]]

        return merged_df
    
    def cross_analyze_with_bill_base(self, bill_df: pd.DataFrame, call_df: pd.DataFrame) -> pd.DataFrame:
        """
        以账单类为基准进行交叉分析，支持跨数据源对手信息显示
        
        Parameters:
        -----------
        bill_df : pd.DataFrame
            账单数据
        call_df : pd.DataFrame
            话单数据
            
        Returns:
        --------
        pd.DataFrame
            交叉分析结果
        """
        # 以账单数据为基础，不创建额外组合

        # 对账单类数据按对方姓名进行金额累计和去重，并计算平台分布
        agg_dict = {}
        if '收入总额' in bill_df.columns:
            agg_dict['收入总额'] = 'sum'
        if '支出总额' in bill_df.columns:
            agg_dict['支出总额'] = 'sum'
        if '交易次数' in bill_df.columns:
            agg_dict['交易次数'] = 'sum'
            
        if agg_dict:
            bill_platform_summary = bill_df.groupby(['本方姓名', '对方姓名', '平台']).agg(agg_dict).reset_index()
        else:
            bill_platform_summary = pd.DataFrame()

        # 计算总金额
        total_agg_dict = {}
        if '收入总额' in bill_df.columns:
            total_agg_dict['收入总额'] = 'sum'
        if '支出总额' in bill_df.columns:
            total_agg_dict['支出总额'] = 'sum'
        if '交易次数' in bill_df.columns:
            total_agg_dict['交易次数'] = 'sum'
            
        if total_agg_dict:
            total_agg_dict['平台'] = lambda x: '、'.join(x.unique())
            bill_total_summary = bill_df.groupby(['本方姓名', '对方姓名']).agg(total_agg_dict).reset_index()
        else:
            bill_total_summary = pd.DataFrame()

        # 计算各平台的金额分布
        platform_details = bill_platform_summary.groupby(['本方姓名', '对方姓名']).apply(
            lambda group: self._format_platform_details(group)
        ).reset_index(name='平台金额分布')

        # 为每个平台创建独立字段
        platform_individual_data = {}
        if not bill_platform_summary.empty:
            platforms = bill_platform_summary['平台'].unique()
            for platform in platforms:
                platform_data = bill_platform_summary[bill_platform_summary['平台'] == platform]
                
                # 动态构建聚合字典，只包含存在的列
                agg_dict = {}
                if '收入总额' in platform_data.columns:
                    agg_dict['收入总额'] = 'sum'
                if '支出总额' in platform_data.columns:
                    agg_dict['支出总额'] = 'sum'
                if '交易次数' in platform_data.columns:
                    agg_dict['交易次数'] = 'sum'
                    
                if agg_dict:
                    platform_summary = platform_data.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()

                    # 重命名列以区分不同平台
                    rename_dict = {}
                    if '收入总额' in platform_summary.columns:
                        rename_dict['收入总额'] = f'{platform}_收入总额'
                    if '支出总额' in platform_summary.columns:
                        rename_dict['支出总额'] = f'{platform}_支出总额'
                    if '交易次数' in platform_summary.columns:
                        rename_dict['交易次数'] = f'{platform}_交易次数'
                        
                    if rename_dict:
                        platform_summary = platform_summary.rename(columns=rename_dict)
                        platform_individual_data[platform] = platform_summary

        # 合并总金额和平台详情
        if not bill_total_summary.empty and not platform_details.empty:
            bill_summary_with_details = pd.merge(bill_total_summary, platform_details, on=['本方姓名', '对方姓名'])
        else:
            bill_summary_with_details = bill_total_summary.copy() if not bill_total_summary.empty else pd.DataFrame()

        # 获取话单中的对方详细信息
        agg_dict = {
            '通话次数': 'sum'
        }

        # 检查通话时长列名
        if '通话总时长(分钟)' in call_df.columns:
            agg_dict['通话总时长(分钟)'] = 'sum'
        elif '通话时长' in call_df.columns:
            agg_dict['通话时长'] = 'sum'

        # 安全地添加可选字段
        if '对方号码' in call_df.columns:
            agg_dict['对方号码'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        # 检查带lambda后缀的字段名（来自话单频率分析）
        if '对方单位名称_<lambda>' in call_df.columns:
            agg_dict['对方单位名称_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        elif '对方单位名称' in call_df.columns:
            agg_dict['对方单位名称'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        if '对方职务_<lambda>' in call_df.columns:
            agg_dict['对方职务_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
        elif '对方职务' in call_df.columns:
            agg_dict['对方职务'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''

        call_details = call_df.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()

        # 以账单数据为基础进行合并
        if not bill_summary_with_details.empty:
            merged_df = bill_summary_with_details.copy()
        else:
            merged_df = pd.DataFrame()

        # 与话单数据合并 - 支持跨人员匹配
        if not call_details.empty and not merged_df.empty:
            # 首先尝试完全匹配
            merged_df = pd.merge(
                merged_df,
                call_details,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

            # 对于没有匹配到的记录，尝试基于对方姓名匹配
            # 创建话单数据的对方姓名汇总
            call_agg_dict = {
                '通话次数': 'sum'
            }
            # 检查通话时长列名
            if '通话总时长(分钟)' in call_details.columns:
                call_agg_dict['通话总时长(分钟)'] = 'sum'
            elif '通话时长' in call_details.columns:
                call_agg_dict['通话时长'] = 'sum'

            call_contact_summary = call_details.groupby('对方姓名').agg(call_agg_dict).reset_index()

            # 添加单位信息字段
            if '对方单位名称_<lambda>' in call_details.columns:
                call_contact_summary = pd.merge(
                    call_contact_summary,
                    call_details.groupby('对方姓名')['对方单位名称_<lambda>'].first().reset_index(),
                    on='对方姓名'
                )
            elif '对方单位名称' in call_details.columns:
                call_contact_summary = pd.merge(
                    call_contact_summary,
                    call_details.groupby('对方姓名')['对方单位名称'].first().reset_index(),
                    on='对方姓名'
                )

            # 找出没有话单数据的账单记录
            no_call_mask = merged_df['通话次数'].isna()
            if no_call_mask.any():
                # 基于对方姓名进行跨人员匹配
                cross_match = pd.merge(
                    merged_df[no_call_mask][['本方姓名', '对方姓名']],
                    call_contact_summary,
                    on='对方姓名',
                    how='left'
                )

                # 更新没有匹配到的记录
                for idx, row in cross_match.iterrows():
                    if pd.notna(row['通话次数']):
                        mask = (merged_df['本方姓名'] == row['本方姓名']) & (merged_df['对方姓名'] == row['对方姓名'])
                        merged_df.loc[mask, '通话次数'] = row['通话次数']

                        # 更新通话时长（检查列名）
                        if '通话总时长(分钟)' in row and pd.notna(row['通话总时长(分钟)']):
                            merged_df.loc[mask, '通话总时长(分钟)'] = row['通话总时长(分钟)']
                        elif '通话时长' in row and pd.notna(row['通话时长']):
                            merged_df.loc[mask, '通话时长'] = row['通话时长']

                        # 更新单位信息
                        if '对方单位名称_<lambda>' in row and pd.notna(row['对方单位名称_<lambda>']):
                            merged_df.loc[mask, '对方单位名称_<lambda>'] = row['对方单位名称_<lambda>']
                        elif '对方单位名称' in row and pd.notna(row['对方单位名称']):
                            merged_df.loc[mask, '对方单位名称'] = row['对方单位名称']

        # 与各平台独立数据合并
        for platform, platform_data in platform_individual_data.items():
            merged_df = pd.merge(
                merged_df,
                platform_data,
                on=['本方姓名', '对方姓名'],
                how='left'
            )

        # 填充空值
        merged_df['通话次数'] = merged_df['通话次数'].fillna(0)
        if '通话总时长(分钟)' in merged_df.columns:
            merged_df['通话总时长(分钟)'] = merged_df['通话总时长(分钟)'].fillna(0)
        elif '通话时长' in merged_df.columns:
            merged_df['通话时长'] = merged_df['通话时长'].fillna(0)

        # 填充各平台的金额字段
        for col in merged_df.columns:
            if any(platform in col for platform in ['银行', '微信', '支付宝']) and any(field in col for field in ['收入总额', '支出总额', '交易次数']):
                merged_df[col] = merged_df[col].fillna(0)

        # 安全地填充可选字段的空值
        if '对方号码' in merged_df.columns:
            merged_df['对方号码'] = merged_df['对方号码'].fillna('')
        if '对方单位名称_<lambda>' in merged_df.columns:
            merged_df['对方单位名称_<lambda>'] = merged_df['对方单位名称_<lambda>'].fillna('')
        elif '对方单位名称' in merged_df.columns:
            merged_df['对方单位名称'] = merged_df['对方单位名称'].fillna('')
        if '对方职务_<lambda>' in merged_df.columns:
            merged_df['对方职务_<lambda>'] = merged_df['对方职务_<lambda>'].fillna('')
        elif '对方职务' in merged_df.columns:
            merged_df['对方职务'] = merged_df['对方职务'].fillna('')

        # 重新排列列的顺序，将对方详细信息放在对方姓名后面
        base_columns = ['本方姓名', '对方姓名']
        detail_columns = []

        # 安全地添加存在的详细信息字段，优先使用带lambda后缀的字段
        if '对方号码' in merged_df.columns:
            detail_columns.append('对方号码')
        if '对方单位名称_<lambda>' in merged_df.columns:
            detail_columns.append('对方单位名称_<lambda>')
        elif '对方单位名称' in merged_df.columns:
            detail_columns.append('对方单位名称')
        if '对方职务_<lambda>' in merged_df.columns:
            detail_columns.append('对方职务_<lambda>')
        elif '对方职务' in merged_df.columns:
            detail_columns.append('对方职务')

        # 账单汇总列
        bill_summary_columns = []
        if '收入总额' in merged_df.columns:
            bill_summary_columns.extend(['收入总额', '支出总额', '交易次数'])
        if '平台' in merged_df.columns:
            bill_summary_columns.append('平台')
        if '平台金额分布' in merged_df.columns:
            bill_summary_columns.append('平台金额分布')

        # 话单相关列
        call_columns = []
        if '通话次数' in merged_df.columns:
            call_columns.append('通话次数')
        if '通话总时长(分钟)' in merged_df.columns:
            call_columns.append('通话总时长(分钟)')
        elif '通话时长' in merged_df.columns:
            call_columns.append('通话时长')

        # 各平台独立列（按平台名称排序）
        platform_columns = []
        platforms = ['银行', '微信', '支付宝']
        for platform in platforms:
            for field in ['收入总额', '支出总额', '交易次数']:
                col_name = f'{platform}_{field}'
                if col_name in merged_df.columns:
                    platform_columns.append(col_name)

        # 剩余列
        used_columns = base_columns + detail_columns + bill_summary_columns + call_columns + platform_columns
        remaining_columns = [col for col in merged_df.columns if col not in used_columns]

        final_columns = base_columns + detail_columns + bill_summary_columns + call_columns + platform_columns + remaining_columns
        merged_df = merged_df[[col for col in final_columns if col in merged_df.columns]]

        return merged_df
    
    def cross_analyze_with_platform_base(self, bill_df: pd.DataFrame, call_df: pd.DataFrame, platform: str) -> pd.DataFrame:
        """
        以特定平台为基准进行交叉分析
        
        Parameters:
        -----------
        bill_df : pd.DataFrame
            账单数据
        call_df : pd.DataFrame
            话单数据
        platform : str
            平台名称（银行/微信/支付宝）
            
        Returns:
        --------
        pd.DataFrame
            平台基准交叉分析结果
        """
        # 过滤出指定平台的账单数据
        platform_bill_df = bill_df[bill_df['平台'] == platform].copy()
        
        if platform_bill_df.empty:
            return pd.DataFrame()
        
        # 安全地构建聚合字典，只包含存在的列
        agg_dict = {}
        if '收入总额' in platform_bill_df.columns:
            agg_dict['收入总额'] = 'sum'
        if '支出总额' in platform_bill_df.columns:
            agg_dict['支出总额'] = 'sum'
        if '交易次数' in platform_bill_df.columns:
            agg_dict['交易次数'] = 'sum'
        if '数据来源' in platform_bill_df.columns:
            agg_dict['数据来源'] = 'first'
        
        # 如果没有可聚合的列，返回空DataFrame
        if not agg_dict:
            return pd.DataFrame()
        
        # 对平台数据进行分组聚合
        platform_summary = platform_bill_df.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()
        
        # 添加平台信息
        platform_summary['平台'] = platform
        
        # 获取其他平台的数据（非当前平台）
        other_platforms_bill_df = bill_df[bill_df['平台'] != platform].copy()
        
        # 对其他平台数据进行分组聚合
        other_platforms_summary = pd.DataFrame()
        if not other_platforms_bill_df.empty:
            # 安全地构建聚合字典，只包含存在的列
            other_agg_dict = {}
            if '收入总额' in other_platforms_bill_df.columns:
                other_agg_dict['收入总额'] = 'sum'
            if '支出总额' in other_platforms_bill_df.columns:
                other_agg_dict['支出总额'] = 'sum'
            if '交易次数' in other_platforms_bill_df.columns:
                other_agg_dict['交易次数'] = 'sum'
            
            # 如果没有可聚合的列，跳过其他平台聚合
            if other_agg_dict:
                other_platforms_summary = other_platforms_bill_df.groupby(['本方姓名', '对方姓名', '平台']).agg(other_agg_dict).reset_index()
            
            # 计算各平台的金额分布
            platform_details = other_platforms_summary.groupby(['本方姓名', '对方姓名']).apply(
                lambda group: self._format_platform_details(group)
            ).reset_index(name='其他平台金额分布')
        else:
            platform_details = pd.DataFrame(columns=['本方姓名', '对方姓名', '其他平台金额分布'])
        
        # 获取话单数据汇总
        call_summary = pd.DataFrame()
        if not call_df.empty:
            agg_dict = {
                '通话次数': 'sum'
            }
            
            # 检查通话时长列名
            if '通话总时长(分钟)' in call_df.columns:
                agg_dict['通话总时长(分钟)'] = 'sum'
            elif '通话时长' in call_df.columns:
                agg_dict['通话时长'] = 'sum'
            
            # 安全地添加可选字段
            if '对方号码' in call_df.columns:
                agg_dict['对方号码'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            if '对方单位名称_<lambda>' in call_df.columns:
                agg_dict['对方单位名称_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            elif '对方单位名称' in call_df.columns:
                agg_dict['对方单位名称'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            if '对方职务_<lambda>' in call_df.columns:
                agg_dict['对方职务_<lambda>'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            elif '对方职务' in call_df.columns:
                agg_dict['对方职务'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
            
            call_summary = call_df.groupby(['本方姓名', '对方姓名']).agg(agg_dict).reset_index()
        
        # 以平台数据为基础进行合并
        merged_df = platform_summary.copy()
        
        # 合并其他平台详情
        if not platform_details.empty:
            merged_df = pd.merge(merged_df, platform_details, on=['本方姓名', '对方姓名'], how='left')
        
        # 合并话单数据
        if not call_summary.empty:
            merged_df = pd.merge(merged_df, call_summary, on=['本方姓名', '对方姓名'], how='left')
        
        # 安全地填充空值
        if '收入总额' in merged_df.columns:
            merged_df['收入总额'] = merged_df['收入总额'].fillna(0)
        if '支出总额' in merged_df.columns:
            merged_df['支出总额'] = merged_df['支出总额'].fillna(0)
        if '交易次数' in merged_df.columns:
            merged_df['交易次数'] = merged_df['交易次数'].fillna(0)
        
        if '其他平台金额分布' in merged_df.columns:
            merged_df['其他平台金额分布'] = merged_df['其他平台金额分布'].fillna('无')
        
        if '通话次数' in merged_df.columns:
            merged_df['通话次数'] = merged_df['通话次数'].fillna(0)
        if '通话总时长(分钟)' in merged_df.columns:
            merged_df['通话总时长(分钟)'] = merged_df['通话总时长(分钟)'].fillna(0)
        elif '通话时长' in merged_df.columns:
            merged_df['通话时长'] = merged_df['通话时长'].fillna(0)
        
        # 安全地填充可选字段的空值
        if '对方号码' in merged_df.columns:
            merged_df['对方号码'] = merged_df['对方号码'].fillna('')
        if '对方单位名称_<lambda>' in merged_df.columns:
            merged_df['对方单位名称_<lambda>'] = merged_df['对方单位名称_<lambda>'].fillna('')
        elif '对方单位名称' in merged_df.columns:
            merged_df['对方单位名称'] = merged_df['对方单位名称'].fillna('')
        if '对方职务_<lambda>' in merged_df.columns:
            merged_df['对方职务_<lambda>'] = merged_df['对方职务_<lambda>'].fillna('')
        elif '对方职务' in merged_df.columns:
            merged_df['对方职务'] = merged_df['对方职务'].fillna('')
        
        # 重新排列列的顺序
        base_columns = ['本方姓名', '对方姓名', '平台']
        detail_columns = []
        
        # 安全地添加存在的详细信息字段
        if '对方号码' in merged_df.columns:
            detail_columns.append('对方号码')
        if '对方单位名称_<lambda>' in merged_df.columns:
            detail_columns.append('对方单位名称_<lambda>')
        elif '对方单位名称' in merged_df.columns:
            detail_columns.append('对方单位名称')
        if '对方职务_<lambda>' in merged_df.columns:
            detail_columns.append('对方职务_<lambda>')
        elif '对方职务' in merged_df.columns:
            detail_columns.append('对方职务')
        
        # 平台交易列
        platform_columns = ['收入总额', '支出总额', '交易次数']
        
        # 其他平台列
        other_platform_columns = []
        if '其他平台金额分布' in merged_df.columns:
            other_platform_columns.append('其他平台金额分布')
        
        # 话单相关列
        call_columns = []
        if '通话次数' in merged_df.columns:
            call_columns.append('通话次数')
        if '通话总时长(分钟)' in merged_df.columns:
            call_columns.append('通话总时长(分钟)')
        elif '通话时长' in merged_df.columns:
            call_columns.append('通话时长')
        
        # 数据来源列
        source_columns = []
        if '数据来源' in merged_df.columns:
            source_columns.append('数据来源')
        
        # 剩余列
        used_columns = base_columns + detail_columns + platform_columns + other_platform_columns + call_columns + source_columns
        remaining_columns = [col for col in merged_df.columns if col not in used_columns]
        
        final_columns = base_columns + detail_columns + platform_columns + other_platform_columns + call_columns + source_columns + remaining_columns
        merged_df = merged_df[[col for col in final_columns if col in merged_df.columns]]
        
        return merged_df
    
    def _format_platform_details(self, group: pd.DataFrame) -> str:
        """格式化平台金额分布详情"""
        details = []
        for _, row in group.iterrows():
            platform = row['平台']
            
            # 安全地获取收入总额和支出总额
            income = 0
            expense = 0
            if '收入总额' in group.columns:
                income = row['收入总额']
            if '支出总额' in group.columns:
                expense = row['支出总额']
                
            if income > 0 or expense > 0:
                detail = f"{platform}(收入{income:.0f}元,支出{expense:.0f}元)"
                details.append(detail)
        return '; '.join(details) if details else '无'