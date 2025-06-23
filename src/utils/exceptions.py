#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自定义异常类，集中管理项目中的所有异常
"""

class BaseError(Exception):
    """所有自定义异常的基类"""
    pass

class ConfigError(BaseError):
    """配置相关错误"""
    pass

class DataError(BaseError):
    """数据处理相关错误"""
    pass

class DataLoadError(DataError):
    """数据加载错误"""
    pass

class DataValidationError(DataError):
    """数据验证错误"""
    pass

class AnalysisError(BaseError):
    """分析过程中的错误"""
    pass

class ExportError(BaseError):
    """导出过程中的错误"""
    pass

class GroupError(BaseError):
    """分组管理相关错误"""
    pass

class InvalidArgumentError(BaseError):
    """函数参数无效"""
    pass 