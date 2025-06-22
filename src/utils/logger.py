#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys
from datetime import datetime

def setup_logger(name=None, level=logging.INFO, log_file=None, console=True):
    """
    设置日志器
    
    Parameters:
    -----------
    name : str, optional
        日志器名称，如果不提供则使用根日志器
    level : int, optional
        日志级别，默认为INFO
    log_file : str, optional
        日志文件路径，如果不提供则不输出到文件
    console : bool, optional
        是否输出到控制台，默认为True
        
    Returns:
    --------
    logging.Logger
        日志器对象
    """
    # 创建日志器
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers = []  # 清除已有处理器
    
    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 如果需要输出到控制台
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # 如果需要输出到文件
    if log_file:
        # 创建日志目录
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def get_default_logger():
    """
    获取默认日志器
    
    Returns:
    --------
    logging.Logger
        默认日志器对象
    """
    # 如果根日志器已有处理器，则返回根日志器
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return root_logger
    
    # 否则创建一个新的日志器
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'app_{timestamp}.log'
    
    return setup_logger(level=logging.INFO, log_file=log_file, console=True) 