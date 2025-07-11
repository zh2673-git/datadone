# 多源数据分析系统

## 📋 项目概述

多源数据分析系统是一个专业的数据分析工具，专门用于处理和分析银行、微信、支付宝、话单等多种数据源。系统采用模块化设计，提供全面的数据分析功能，包括频率分析、时间模式分析、金额分析、综合交叉分析等，并支持Excel和Word格式的专业报告导出。

## 🏗️ 系统架构

```
多源数据分析系统/
├── src/                          # 源代码目录
│   ├── base/                     # 基础模块
│   │   ├── base_analyzer.py      # 分析器基类
│   │   └── base_model.py         # 数据模型基类
│   ├── datasource/               # 数据源模块
│   │   ├── bank_model.py         # 银行数据模型
│   │   ├── call_model.py         # 话单数据模型
│   │   └── payment/              # 支付数据子模块
│   │       ├── wechat_model.py   # 微信数据模型
│   │       └── alipay_model.py   # 支付宝数据模型
│   ├── analysis/                 # 分析模块
│   │   ├── bank_analyzer.py      # 银行数据分析器
│   │   ├── call_analyzer.py      # 话单数据分析器
│   │   ├── comprehensive_analyzer.py  # 综合分析器
│   │   └── payment/              # 支付分析子模块
│   │       ├── wechat_analyzer.py    # 微信数据分析器
│   │       └── alipay_analyzer.py    # 支付宝数据分析器
│   ├── export/                   # 导出模块
│   │   ├── excel_exporter.py     # Excel导出器
│   │   └── word_exporter.py      # Word导出器
│   ├── interface/                # 用户界面模块
│   │   ├── cli_interface.py      # 命令行界面
│   │   ├── cli_interface_data.py # 数据管理界面
│   │   └── cli_interface_export.py # 导出界面
│   └── utils/                    # 工具模块
│       ├── config.py             # 配置管理
│       ├── exceptions.py         # 异常定义
│       ├── advanced_analysis.py  # 高级分析工具
│       └── cash_recognition.py   # 存取现识别工具
├── config.json                   # 配置文件
├── main.py                       # 主程序入口
└── README.md                     # 项目说明文档
```

## 🔧 核心功能模块

### 数据源模块 (datasource/)
- **银行数据模型**: 处理银行交易数据，支持存取现识别、收支分类
- **话单数据模型**: 处理通话记录数据，支持通话时长、频率统计
- **微信数据模型**: 处理微信支付数据，支持转账、红包等交易类型
- **支付宝数据模型**: 处理支付宝交易数据，支持多种支付场景

### 分析模块 (analysis/)
- **银行分析器**: 提供银行数据的频率分析、金额分析、时间模式分析
- **话单分析器**: 提供通话频率分析、时间分布分析、联系人分析
- **微信分析器**: 提供微信交易分析、转账模式分析
- **支付宝分析器**: 提供支付宝交易分析、消费模式分析
- **综合分析器**: 跨数据源的关联分析、交叉验证分析

### 导出模块 (export/)
- **Excel导出器**: 生成结构化的Excel分析报告，包含8个核心工作表
- **Word导出器**: 生成专业的Word分析报告，支持按人员分组显示

### 工具模块 (utils/)
- **配置管理**: 统一的配置文件管理，支持字段映射和参数配置
- **高级分析工具**: 提供时间模式、金额模式、异常检测等高级分析功能
- **存取现识别**: 智能识别银行数据中的存取现交易

## 📊 分析功能特性

### 基础分析
- **频率分析**: 统计交易频率、通话频率，识别高频联系人
- **金额分析**: 分析交易金额分布、大额交易识别
- **时间分析**: 分析交易时间模式、工作日/周末分布
- **存取现识别**: 智能识别银行数据中的存取现交易

### 高级分析
- **行为模式分析**: 识别用户的交易习惯和行为特征
- **异常检测**: 检测异常交易、可疑行为模式
- **关联分析**: 分析不同数据源之间的关联关系

### 综合分析
- **跨平台分析**: 整合银行、微信、支付宝数据进行综合分析
- **交叉验证**: 通过多数据源验证交易的真实性和一致性
- **关系网络**: 构建基于交易和通话的关系网络

## 📈 报告功能

### Excel报告
1. **分析汇总表** - 各平台汇总数据概览
2. **账单类频率表** - 银行、微信、支付宝频率分析
3. **话单类频率表** - 通话记录频率分析
4. **综合分析表** - 跨平台交叉分析结果（包含对方详细信息）
5. **平台原始数据** - 各平台的原始数据展示
6. **高级分析表** - 高级分析结果汇总（含通俗易懂的指标说明）

### Word报告
- **个人详细分析**: 按人员分组的详细分析报告
- **综合交叉分析**: 按本方姓名排序的关联分析，展示不同数据源间的关联关系
- **重点收支分析**: 重要交易和收支模式分析
- **专业图表**: 包含统计图表和可视化分析

## 🚀 使用方式

### 命令行启动
```bash
python main.py
```

### 主要操作流程
1. **数据加载**: 选择并加载银行、微信、支付宝、话单数据文件
2. **执行分析**: 对加载的数据执行全面分析或专项分析
3. **导出报告**: 生成Excel或Word格式的分析报告

## ⚙️ 配置说明

系统使用`config.json`文件进行配置管理，支持：
- **字段映射**: 自定义各数据源的字段名称映射
- **分析参数**: 配置分析算法的参数和阈值
- **导出设置**: 配置报告导出的格式和样式

## 🔄 最新更新内容

### Word报告综合交叉分析优化
- **前10条限制**: 只显示关联度最高的前10条记录，无论分析对象有多少个
- **本方姓名连续显示**: 相同本方姓名的记录连续显示，不会被其他人的记录打断
- **全局关联度排序**: 先按关联数据源数量全局排序取前10条，再按本方姓名分组排列
- **表格直接显示**: 使用统一表格显示，包含本方姓名列，便于查看数据归属

### Word报告特殊金额显示优化
- **移除名字长度过滤**: 不再过滤超过4个字的对方姓名，显示真实的完整姓名
- **优化显示格式**: 改为"金额（姓名，次数）"的格式，例如：520.00元（李四，1次、某某某某某公司，1次）
- **统计次数信息**: 显示每个对方姓名对应该特殊金额的交易次数
- **避免"未知"显示**: 解决因名字长度过滤导致的"未知"显示问题
- **爱情数字优先**: 1314、520、521金额必须列出，显示所有涉及的人
- **其他金额限制**: 其他特殊金额只显示金额最大的前3个，每个金额最多显示3个人

### 综合分析表字段增强
- **新增对方详细信息字段**: 在综合分析表的对方姓名列后添加三个重要字段
  - 对方号码：显示联系电话信息
  - 对方单位名称：显示工作单位信息
  - 对方职务：显示职位信息
- **智能字段排序**: 自动将新字段放置在对方姓名后面，便于查看完整的对方信息

### 高级分析说明字段优化
- **通俗易懂的指标解释**: 大幅改进高级分析表中"说明"字段的内容质量
  - 复杂指标友好化：将`distribution_工作日交易数`解释为"周一到周五期间的交易次数统计"
  - 元组指标解析：将`('交易日期', 'count')_1.0`解释为"交易次数的统计分析"
  - 智能描述生成：根据指标类型自动生成相应的通俗解释

### 配置文件同步更新
- **config.py和config.json双重更新**: 确保配置文件完全同步
  - 银行字段：新增本方卡号、对方卡号、对方银行名称、社会关系、交易地址等字段
  - 微信字段：新增对方微信昵称、商户名称、关联手机号码、社会关系等字段
  - 支付宝字段：新增本方账号、对方账号、支付方式、交易商品名称等字段
  - 话单字段：新增本方身份证号、对方单位名称、通话所在地、社会关系等字段

### Excel表格结构优化
- **精简表格生成**: 只保留8个核心分析表格，移除冗余内容
- **移除重复分析**: 自动过滤掉"综合分析_以XX为基准"的单独sheet
- **字段兼容性**: 修复字段兼容性问题，确保新旧数据格式的完全兼容

### 技术改进
- 优化Excel导出器架构，提高数据处理效率
- 增强字段标准化处理，确保数据一致性
- 改进错误处理机制，提高系统稳定性
- 修复日期时间解析警告，提高解析性能和一致性
- 完善向后兼容性，支持各种数据格式