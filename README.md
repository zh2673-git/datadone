# 多源数据分析系统

## 项目简介

本项目是一个面向司法、金融等领域的多源数据分析系统，支持对银行流水、微信、支付宝、话单等多种数据源的自动化加载、清洗、分析与报告生成。系统采用模块化设计，支持命令行交互，能够输出结构化的Excel分析表和叙述性Word报告。

## 主要功能
- 自动识别和加载多种数据源（银行、微信、支付宝、话单）
- 数据清洗与标准化处理
- 按人/分组/对手方的多维度统计分析
- 存取现、转账、通话频率等专项分析
- 综合交叉分析（多源数据关联）
- 结果导出为Excel和Word报告
- 支持人员分组管理
- 命令行交互式操作

## 目录结构
```text
├── data/                # 原始数据文件（Excel）
├── output/              # 分析结果输出（Excel/Word）
├── src/                 # 核心源代码
│   ├── analysis/        # 各类分析器
│   ├── base/            # 抽象基类
│   ├── datasource/      # 数据加载与预处理
│   ├── export/          # 结果导出（Excel/Word）
│   ├── group/           # 分组管理
│   ├── interface/       # 命令行界面
│   └── utils/           # 工具类（配置、日志）
├── requirements.txt     # 依赖包列表
├── main.py              # 程序入口
└── README.md            # 项目说明文档
```

## 安装与运行
1. **克隆项目**
```bash
git clone https://github.com/zh2673-git/datadone.git
cd datadone
```
2. **安装依赖**
```bash
pip install -r requirements.txt
```
3. **准备数据**
- 将待分析的Excel数据文件放入 `data/` 目录。

4. **运行程序**
```bash
python main.py
```

## 依赖说明
- Python 3.7+
- 主要依赖包：pandas, numpy, openpyxl, xlsxwriter, python-docx, argparse 等
- 详细依赖见 `requirements.txt`

## 使用示例
1. 启动后，按提示选择"加载数据"自动识别并导入 `data/` 目录下的所有Excel文件。
2. 选择"执行分析"可进行银行、微信、支付宝、话单及综合分析。
3. 选择"导出已有分析结果"可将分析结果导出为Excel或生成Word报告。

## 贡献方式
欢迎提交Issue、PR或建议！

## 许可证
本项目采用 MIT License。 