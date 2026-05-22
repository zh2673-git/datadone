"""关键词库管理模块"""

from typing import Any, List, Optional

from src.config.settings import AppSettings


class KeywordsManager:
    """关键词库管理 - 从 keywords.json 加载所有关键词"""

    def __init__(self, settings: Optional[AppSettings] = None):
        self._settings = settings or AppSettings.get_instance()

    @property
    def _keywords(self) -> dict:
        return self._settings.keywords

    def get(self, path: str, default: Any = None) -> Any:
        """通过点分隔路径获取配置值"""
        if path.startswith("analysis."):
            path = path[len("analysis."):]
        if path.startswith("data_sources."):
            path = path[len("data_sources."):]

        keys = path.split(".")
        value = self._keywords
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value

    # ---- 银行存取现关键词 ----

    def get_deposit_keywords(self) -> list[str]:
        """获取存现识别关键词"""
        return self._keywords.get("bank", {}).get("deposit_keywords", [])

    def get_withdraw_keywords(self) -> list[str]:
        """获取取现识别关键词"""
        return self._keywords.get("bank", {}).get("withdraw_keywords", [])

    def get_deposit_exclude_keywords(self) -> list[str]:
        """获取存现排除关键词"""
        return self._keywords.get("bank", {}).get("deposit_exclude_keywords", [])

    def get_withdraw_exclude_keywords(self) -> list[str]:
        """获取取现排除关键词"""
        return self._keywords.get("bank", {}).get("withdraw_exclude_keywords", [])

    def get_high_priority_deposit_keywords(self) -> list[str]:
        """获取高优先级存现关键词"""
        return self._keywords.get("bank", {}).get("high_priority_deposit_keywords", [])

    def get_high_priority_withdraw_keywords(self) -> list[str]:
        """获取高优先级取现关键词"""
        return self._keywords.get("bank", {}).get("high_priority_withdraw_keywords", [])

    # ---- 重点收支关键词 ----

    def get_work_income_keywords(self) -> list[str]:
        """获取工作收入关键词"""
        return (
            self._keywords.get("key_transactions", {})
            .get("work_income", {})
            .get("keywords", [])
        )

    def get_property_keywords(self) -> list[str]:
        """获取房产关键词"""
        return (
            self._keywords.get("key_transactions", {})
            .get("asset_income", {})
            .get("property", {})
            .get("keywords", [])
        )

    def get_rental_keywords(self) -> list[str]:
        """获取租金关键词"""
        return (
            self._keywords.get("key_transactions", {})
            .get("asset_income", {})
            .get("rental", {})
            .get("keywords", [])
        )

    def get_vehicle_keywords(self) -> list[str]:
        """获取车辆关键词"""
        return (
            self._keywords.get("key_transactions", {})
            .get("asset_income", {})
            .get("vehicle", {})
            .get("keywords", [])
        )

    def get_securities_keywords(self) -> list[str]:
        """获取证券关键词"""
        return (
            self._keywords.get("key_transactions", {})
            .get("asset_income", {})
            .get("securities", {})
            .get("keywords", [])
        )

    # ---- 习惯兴趣关键词 ----

    def get_habits_interests_categories(self) -> dict:
        """获取习惯兴趣类别配置（含label/keywords/merchant_patterns）"""
        return self._keywords.get("habits_interests", {})
