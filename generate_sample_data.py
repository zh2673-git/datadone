#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import string

# 确保输出目录存在
os.makedirs('data/samples', exist_ok=True)

# 设置随机种子以保证可重现性
random.seed(42)
np.random.seed(42)

# 生成人员信息
def generate_people(count=10):
    first_names = ['张', '王', '李', '赵', '刘', '陈', '杨', '黄', '周', '吴', '郑', '冯', '朱', '孙', '马']
    last_names = ['伟', '芳', '娜', '秀英', '敏', '静', '丽', '强', '磊', '洋', '艳', '勇', '军', '杰', '娟', '涛', '明', '超', '秀兰', '霞']
    
    people = []
    for _ in range(count):
        first = random.choice(first_names)
        last = random.choice(last_names)
        name = first + last
        people.append(name)
    
    return people

# 生成银行数据
def generate_bank_data(people, records_per_person=200):
    # 定义交易摘要和备注的多样化选项
    transaction_summaries = [
        '转账', '网上转账', '手机银行转账', '跨行转账', '行内转账', '工资', '奖金', 
        '报销', '退款', '贷款', '还款', '利息收入', '理财收益', '基金赎回', '股息', 
        '保险理赔', '租金收入', '购物', '消费', '取现', '存现', '缴费', '充值',
        '水电费', '燃气费', '物业费', '电话费', '网费', '房贷', '车贷', '信用卡还款',
        'ATM取款', 'ATM存款', '柜台取款', '柜台存款', '自动取款机取款', '自助存款',
        '跨行ATM取款', '他行ATM取款', '本行ATM取款', '现金支票取款', '转存定期', 
        '定期存款', '活期存款', '定期取款', '活期取款', '定活互转', '活期转定期',
        '定期转活期', '消费退款', '消费撤销', '快捷支付', '扫码支付', '网银支付',
        '手机银行支付', '第三方支付', '支付宝充值', '微信充值', '信用卡消费',
        '信用卡取现', '信用卡还款', '信用卡分期', '信用卡年费', '信用卡逾期费',
        '信用卡利息', '信用卡退款', '信用卡撤销', '信用卡溢缴款', '信用卡溢缴款领回'
    ]
    
    transaction_remarks = [
        '日常消费', '购物', '餐饮', '服装', '电子产品', '家居用品', '生活用品', 
        '交通出行', '旅游', '住宿', '医疗', '教育', '娱乐', '健身', '美容', 
        '工资', '奖金', '报销', '退款', '贷款', '还款', '利息收入', '理财收益', 
        '基金赎回', '股息', '保险理赔', '租金收入', '水电费', '燃气费', '物业费', 
        '电话费', '网费', '房贷', '车贷', '信用卡还款', '转账给家人', '转账给朋友', 
        '转账给同事', '转账给客户', '转账给供应商', '转账给合作伙伴', '转账给子公司', 
        '转账给母公司', '转账给关联公司', '转账给个人账户', '转账给公司账户', 
        '转账给银行账户', '转账给支付宝', '转账给微信', '转账给财付通', '转账给京东金融',
        '转账给余额宝', '转账给理财通', '转账给基金账户', '转账给证券账户', '转账给期货账户',
        '转账给保险账户', '转账给信托账户', '转账给私募账户', '转账给公募账户',
        '现金取款', '现金存款', '支票取款', '支票存款', '电子汇款', '电子转账',
        'ATM取款', 'ATM存款', '柜台取款', '柜台存款', '自动取款机取款', '自助存款'
    ]
    
    bank_types = ['工商银行', '农业银行', '中国银行', '建设银行', '交通银行', '招商银行', '浦发银行', '民生银行', '兴业银行', '光大银行']
    
    data = []
    today = datetime.now()
    
    for person in people:
        for _ in range(records_per_person):
            # 基本信息
            record = {
                '本方姓名': person,
                '本方账号': ''.join(random.choices(string.digits, k=16)),
                '本方卡号': ''.join(random.choices(string.digits, k=16)),
                '银行类型': random.choice(bank_types),
            }
            
            # 日期和时间
            days_ago = random.randint(1, 365 * 3)  # 最多3年前的数据
            trans_date = today - timedelta(days=days_ago)
            record['交易日期'] = trans_date.strftime('%Y-%m-%d')
            record['日期'] = trans_date.strftime('%Y-%m-%d')
            record['交易时间'] = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            record['交易星期'] = ['一', '二', '三', '四', '五', '六', '日'][trans_date.weekday()]
            
            # 交易摘要和备注
            record['交易摘要'] = random.choice(transaction_summaries)
            record['交易备注'] = random.choice(transaction_remarks)
            
            # 交易类型和金额
            is_transfer = random.random() < 0.7  # 70%的概率是转账
            is_deposit = random.random() < 0.5  # 存取现中，50%的概率是存款
            
            if is_transfer:
                record['交易类型'] = '转账'
                record['对方姓名'] = generate_people(1)[0]
                record['对方账号'] = ''.join(random.choices(string.digits, k=16))
                record['对方卡号'] = ''.join(random.choices(string.digits, k=16))
                record['对方银行名称'] = random.choice(bank_types)
            else:
                # 存取现
                if is_deposit:
                    record['交易类型'] = '存款'
                    record['交易摘要'] = random.choice(['存现', '现金存款', '柜台存款', 'ATM存款', '自助存款'])
                else:
                    record['交易类型'] = '取款'
                    record['交易摘要'] = random.choice(['取现', '现金取款', '柜台取款', 'ATM取款', '自动取款机取款'])
                
                record['对方姓名'] = None
                record['对方账号'] = None
                record['对方卡号'] = None
                record['对方银行名称'] = None
            
            # 借贷标识和金额
            is_credit = random.random() < 0.5  # 50%的概率是收入
            amount = round(random.uniform(10, 10000), 2)
            
            if is_credit:
                record['借贷标识'] = '贷'
                record['交易金额'] = amount
                record['账户余额'] = round(random.uniform(amount, amount + 50000), 2)
            else:
                record['借贷标识'] = '借'
                record['交易金额'] = -amount
                record['账户余额'] = round(random.uniform(0, 50000), 2)
            
            # 其他字段
            record['币种'] = '人民币'
            record['对方银行卡归属地'] = random.choice(['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', '重庆', '武汉', '西安'])
            record['对方单位'] = random.choice(['某科技有限公司', '某贸易有限公司', '某服务有限公司', '某制造有限公司', '某教育机构', '某医疗机构', '某政府部门', '某事业单位', '个体工商户', '自由职业者', None])
            record['对方职位'] = random.choice(['经理', '主管', '总监', '总裁', '董事', '职员', '技术员', '销售', '客服', '财务', None])
            record['社会关系'] = random.choice(['亲属', '朋友', '同事', '客户', '供应商', '合作伙伴', None])
            record['交易柜员号'] = ''.join(random.choices(string.digits, k=6))
            record['交易机构号'] = ''.join(random.choices(string.digits, k=6))
            record['交易机构名称'] = f"{record['银行类型']}{random.choice(['总行', '分行', '支行', '营业部'])}"
            record['交易地址'] = random.choice(['北京市', '上海市', '广州市', '深圳市', '杭州市', '南京市', '成都市', '重庆市', '武汉市', '西安市'])
            record['交易渠道'] = random.choice(['网银', '手机银行', 'ATM', '柜台', '自助终端', '第三方支付'])
            record['交易场所'] = random.choice(['银行网点', '自助银行', '网上银行', '手机银行', '第三方平台'])
            
            data.append(record)
    
    # 创建DataFrame并返回
    df = pd.DataFrame(data)
    return df

# 生成话单数据
def generate_call_data(people, records_per_person=200):
    data = []
    today = datetime.now()
    
    for person in people:
        for i in range(records_per_person):
            # 基本信息
            record = {
                '通话次序': i + 1,
                '本方姓名': person,
                '本方号码': '1' + ''.join(random.choices(string.digits, k=10)),
            }
            
            # 呼叫类型和对方信息
            is_outgoing = random.random() < 0.5  # 50%的概率是主叫
            record['呼叫类型'] = '主叫' if is_outgoing else '被叫'
            
            # 随机生成一个对方联系人
            if random.random() < 0.7:  # 70%的概率是已有联系人
                record['对方姓名'] = generate_people(1)[0]
                record['对方单位'] = random.choice(['某科技有限公司', '某贸易有限公司', '某服务有限公司', '某制造有限公司', '某教育机构', '某医疗机构', '某政府部门', '某事业单位', '个体工商户', '自由职业者', None])
            else:
                record['对方姓名'] = None
                record['对方单位'] = None
            
            record['对方号码'] = '1' + ''.join(random.choices(string.digits, k=10))
            record['对方号码类型'] = random.choice(['移动', '联通', '电信', '虚拟运营商'])
            record['对方号码归属地'] = random.choice(['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', '重庆', '武汉', '西安'])
            
            # 日期和时间
            days_ago = random.randint(1, 365 * 3)  # 最多3年前的数据
            call_date = today - timedelta(days=days_ago)
            record['呼叫日期'] = call_date.strftime('%Y-%m-%d %H:%M:%S')
            record['星期'] = ['一', '二', '三', '四', '五', '六', '日'][call_date.weekday()]
            
            # 通话时长
            record['通话时长(秒)'] = random.randint(10, 3600)  # 10秒到1小时
            
            # 网络和基站信息
            record['网络号'] = ''.join(random.choices(string.digits, k=6))
            record['小区号'] = ''.join(random.choices(string.digits, k=6))
            record['基站号'] = ''.join(random.choices(string.digits, k=8))
            record['对方小区号'] = ''.join(random.choices(string.digits, k=6))
            
            # 其他信息
            record['特殊日期名称'] = random.choice(['元旦', '春节', '清明', '劳动节', '端午', '中秋', '国庆', None, None, None, None, None])  # 大部分是None
            record['通话地址'] = random.choice(['北京市', '上海市', '广州市', '深圳市', '杭州市', '南京市', '成都市', '重庆市', '武汉市', '西安市'])
            
            data.append(record)
    
    # 创建DataFrame并返回
    df = pd.DataFrame(data)
    return df

# 生成微信数据
def generate_wechat_data(people, records_per_person=200):
    data = []
    today = datetime.now()
    
    for person in people:
        for i in range(records_per_person):
            # 基本信息
            record = {
                '序号': i + 1,
                '本方姓名': person,
                '本方微信账号': 'wx_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
            }
            
            # 日期和时间
            days_ago = random.randint(1, 365 * 3)  # 最多3年前的数据
            trans_date = today - timedelta(days=days_ago)
            record['交易日期'] = trans_date.strftime('%Y-%m-%d')
            record['交易时间'] = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            record['交易星期'] = ['一', '二', '三', '四', '五', '六', '日'][trans_date.weekday()]
            
            # 交易信息
            is_transfer = random.random() < 0.8  # 80%的概率是转账
            
            if is_transfer:
                record['交易说明'] = random.choice(['转账', '微信转账', '红包', '群红包', '收款', '付款', '退款', '报销', '工资', '奖金'])
                record['对方姓名'] = generate_people(1)[0]
                record['对方微信昵称'] = record['对方姓名'] + random.choice(['', '_wx', '_happy', '_cool', '_nice', '_good', '_best', '_top', '_vip', '_pro'])
                record['对方微信账号'] = 'wx_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
            else:
                record['交易说明'] = random.choice(['微信支付', '扫码支付', '商户消费', '公众号支付', '小程序支付', '自动扣费', '充值', '提现', '理财', '基金'])
                record['对方姓名'] = None
                record['对方微信昵称'] = random.choice(['商户', '店铺', '超市', '餐厅', '酒店', '景点', '电影院', '健身房', '美容院', '医院'])
                record['对方微信账号'] = 'wx_business_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            
            # 借贷标识和金额
            is_credit = random.random() < 0.5  # 50%的概率是收入
            amount = round(random.uniform(1, 5000), 2)
            
            if is_credit:
                record['借贷标识'] = '贷'
                record['交易金额'] = amount
                record['账户余额'] = round(random.uniform(amount, amount + 10000), 2)
            else:
                record['借贷标识'] = '借'
                record['交易金额'] = -amount
                record['账户余额'] = round(random.uniform(0, 10000), 2)
            
            # 其他字段
            record['支付方式'] = random.choice(['微信零钱', '微信零钱通', '银行卡', '信用卡'])
            record['交易类型'] = random.choice(['转账', '红包', '收款', '付款', '退款', '充值', '提现', '理财', '消费'])
            record['交易用途类型'] = random.choice(['日常消费', '购物', '餐饮', '交通', '住宿', '娱乐', '教育', '医疗', '生活缴费', '其他'])
            
            record['注册地址'] = random.choice(['北京市', '上海市', '广州市', '深圳市', '杭州市', '南京市', '成都市', '重庆市', '武汉市', '西安市'])
            record['关联手机号码'] = '1' + ''.join(random.choices(string.digits, k=10))
            
            if not is_transfer:
                record['商户名称'] = record['对方微信昵称']
                record['商户单号'] = 'order_' + ''.join(random.choices(string.digits, k=12))
            else:
                record['商户名称'] = None
                record['商户单号'] = None
            
            record['交易单号'] = 'wx_' + ''.join(random.choices(string.digits, k=16))
            record['交易备注'] = random.choice(['日常消费', '购物', '餐饮', '交通', '住宿', '娱乐', '教育', '医疗', '生活缴费', '其他', None, None, None])
            record['特殊日期名称'] = random.choice(['元旦', '春节', '清明', '劳动节', '端午', '中秋', '国庆', None, None, None, None, None])  # 大部分是None
            record['社会关系'] = random.choice(['亲属', '朋友', '同事', '客户', '供应商', '合作伙伴', None, None, None])  # 大部分是None
            
            data.append(record)
    
    # 创建DataFrame并返回
    df = pd.DataFrame(data)
    return df

# 生成支付宝数据
def generate_alipay_data(people, records_per_person=200):
    data = []
    today = datetime.now()
    
    for person in people:
        for i in range(records_per_person):
            # 基本信息
            record = {
                '序号': i + 1,
                '本方姓名': person,
                '本方账号': 'alipay_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
            }
            
            # 日期和时间
            days_ago = random.randint(1, 365 * 3)  # 最多3年前的数据
            trans_date = today - timedelta(days=days_ago)
            record['交易日期'] = trans_date.strftime('%Y-%m-%d')
            record['交易时间'] = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            
            # 交易信息
            is_transfer = random.random() < 0.8  # 80%的概率是转账
            
            if is_transfer:
                record['交易类型'] = random.choice(['转账', '支付宝转账', '红包', '群红包', '收款', '付款', '退款', '报销', '工资', '奖金'])
                record['对方姓名'] = generate_people(1)[0]
                record['对方账号'] = 'alipay_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
            else:
                record['交易类型'] = random.choice(['支付宝支付', '扫码支付', '商户消费', '生活缴费', '信用卡还款', '自动扣费', '充值', '提现', '理财', '基金'])
                record['对方姓名'] = None
                record['对方账号'] = 'alipay_business_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            
            # 借贷标识和金额
            is_credit = random.random() < 0.5  # 50%的概率是收入
            amount = round(random.uniform(1, 5000), 2)
            
            if is_credit:
                record['借贷标识'] = '贷'
                record['交易金额'] = amount
            else:
                record['借贷标识'] = '借'
                record['交易金额'] = -amount
            
            # 其他字段
            record['支付方式'] = random.choice(['余额', '余额宝', '银行卡', '信用卡', '花呗', '借呗'])
            record['收货地址'] = random.choice(['北京市', '上海市', '广州市', '深圳市', '杭州市', '南京市', '成都市', '重庆市', '武汉市', '西安市', None, None, None])
            record['到账类型'] = random.choice(['即时到账', '次日到账', '2-3个工作日', None, None, None])
            
            record['交易商品名称'] = random.choice(['日用品', '食品', '饮料', '服装', '鞋帽', '电子产品', '家居用品', '美妆护肤', '母婴用品', '图书音像', None, None, None])
            record['提现流水号'] = 'alipay_' + ''.join(random.choices(string.digits, k=16)) if record['交易类型'] == '提现' else None
            
            record['交易涉及对象'] = record['对方姓名'] or record['交易商品名称'] or record['交易类型']
            record['交易来源地'] = random.choice(['北京市', '上海市', '广州市', '深圳市', '杭州市', '南京市', '成都市', '重庆市', '武汉市', '西安市'])
            record['消费名称'] = record['交易商品名称'] or record['交易类型']
            
            record['交易状态'] = random.choice(['成功', '失败', '处理中', '退款中', '已退款', '部分退款'])
            record['交易备注'] = random.choice(['日常消费', '购物', '餐饮', '交通', '住宿', '娱乐', '教育', '医疗', '生活缴费', '其他', None, None, None])
            
            record['交易号'] = 'alipay_' + ''.join(random.choices(string.digits, k=16))
            record['商户订单号'] = 'order_' + ''.join(random.choices(string.digits, k=12)) if not is_transfer else None
            
            record['关联手机号码'] = '1' + ''.join(random.choices(string.digits, k=10))
            record['注册地址'] = random.choice(['北京市', '上海市', '广州市', '深圳市', '杭州市', '南京市', '成都市', '重庆市', '武汉市', '西安市'])
            
            record['特殊日期名称'] = random.choice(['元旦', '春节', '清明', '劳动节', '端午', '中秋', '国庆', None, None, None, None, None])  # 大部分是None
            record['社会关系'] = random.choice(['亲属', '朋友', '同事', '客户', '供应商', '合作伙伴', None, None, None])  # 大部分是None
            
            data.append(record)
    
    # 创建DataFrame并返回
    df = pd.DataFrame(data)
    return df

# 生成所有示例数据
def generate_all_sample_data():
    # 生成人员
    people = generate_people(10)
    print(f"生成了 {len(people)} 个人员")
    
    # 生成银行数据
    bank_data = generate_bank_data(people)
    bank_data.to_excel('data/samples/银行数据.xlsx', index=False)
    print(f"生成了 {len(bank_data)} 条银行数据")
    
    # 生成话单数据
    call_data = generate_call_data(people)
    call_data.to_excel('data/samples/话单数据.xlsx', index=False)
    print(f"生成了 {len(call_data)} 条话单数据")
    
    # 生成微信数据
    wechat_data = generate_wechat_data(people)
    wechat_data.to_excel('data/samples/微信数据.xlsx', index=False)
    print(f"生成了 {len(wechat_data)} 条微信数据")
    
    # 生成支付宝数据
    alipay_data = generate_alipay_data(people)
    alipay_data.to_excel('data/samples/支付宝数据.xlsx', index=False)
    print(f"生成了 {len(alipay_data)} 条支付宝数据")
    
    # 生成存取现筛选字段文件
    if not os.path.exists('output'):
        os.makedirs('output')
    
    # 获取所有交易摘要和备注
    summaries = bank_data['交易摘要'].dropna().unique()
    remarks = bank_data['交易备注'].dropna().unique()
    
    # 写入未人工筛选字段.txt
    with open('output/未人工筛选字段.txt', 'w', encoding='utf-8') as f:
        for item in sorted(list(summaries) + list(remarks)):
            f.write(f"{item}\n")
    
    # 写入人工筛选字段.txt（选择与存取现相关的字段）
    cash_keywords = ['存现', '取现', '现金存款', '现金取款', '柜台存款', '柜台取款', 'ATM存款', 'ATM取款', 
                    '自助存款', '自动取款机取款', '存款', '取款']
    
    with open('output/人工筛选字段.txt', 'w', encoding='utf-8') as f:
        for keyword in cash_keywords:
            f.write(f"{keyword}\n")
    
    print("生成了存取现筛选字段文件")
    
    print("所有示例数据生成完成！")

# 执行生成
if __name__ == "__main__":
    generate_all_sample_data() 