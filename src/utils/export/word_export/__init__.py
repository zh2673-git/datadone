#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Word导出相关的辅助功能模块
包含Word段落生成、格式处理、总结生成等功能
"""

from .word_formatter import WordFormatter
from .word_report_generator import WordReportGenerator

__all__ = ['WordFormatter', 'WordReportGenerator']