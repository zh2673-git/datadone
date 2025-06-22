#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import logging

from src.utils.logger import setup_logger
from src.interface import CommandLineInterface

def parse_args():
    """
    解析命令行参数
    
    Returns:
    --------
    argparse.Namespace
        解析后的参数
    """
    parser = argparse.ArgumentParser(description="多源数据分析系统")
    
    parser.add_argument('-v', '--verbose', action='store_true', help='显示详细日志')
    parser.add_argument('-l', '--log-file', type=str, help='日志文件路径')
    
    return parser.parse_args()

def main():
    """
    主函数
    """
    # 解析命令行参数
    args = parse_args()
    
    # 设置日志级别
    log_level = logging.DEBUG if args.verbose else logging.INFO
    
    # 设置日志文件
    log_file = args.log_file if args.log_file else "app.log"
    
    # 初始化日志
    logger = setup_logger("main", level=log_level, log_file=log_file, console=True)
    
    try:
        # 记录启动信息
        logger.info("多源数据分析系统启动")
        
        # 创建并启动命令行界面
        logger.info("启动交互界面")
        cli = CommandLineInterface(logger)
        cli.start()
        
        # 记录退出信息
        logger.info("多源数据分析系统退出")
    
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}", exc_info=True)
        print(f"\n程序发生错误: {str(e)}")
        print("详细信息请查看日志文件")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 