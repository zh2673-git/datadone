#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""多源数据分析系统 - 程序入口"""

import sys
import logging

from src.interface.cli import CLI


def setup_logging():
    """配置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    """程序入口"""
    setup_logging()
    cli = CLI()
    cli.start()


if __name__ == "__main__":
    main()
