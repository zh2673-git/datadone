"""阈值管理模块"""

from typing import Any, List, Optional

from src.config.settings import AppSettings


class ThresholdsManager:
    """阈值管理 - 从 thresholds.json 加载所有阈值配置"""

    def __init__(self, settings: Optional[AppSettings] = None):
        self._settings = settings or AppSettings.get_instance()

    @property
    def _thresholds(self) -> dict:
        return self._settings.thresholds

    @property
    def _analysis(self) -> dict:
        """获取 analysis 子配置（thresholds.json 中顶层 analysis 键）"""
        return self._thresholds.get("analysis", self._thresholds)

    def get(self, path: str, default: Any = None) -> Any:
        """
        通过点分隔路径获取配置值

        支持两种路径格式：
        - analysis.xxx 格式（推荐，与原始配置结构一致）:
          "analysis.cash.recognition.high_priority_confidence"
        - 不带 analysis 前缀: "cash.recognition.high_priority_confidence"
          自动在 analysis 子树下查找

        Args:
            path: 点分隔路径
            default: 默认值

        Returns:
            配置值，路径不存在时返回 default
        """
        if path.startswith("analysis."):
            return self._resolve(self._analysis, path[len("analysis."):], default)

        # 先在 analysis 子树查找，再 fallback 到 thresholds 根
        result = self._resolve(self._analysis, path, None)
        if result is not None:
            return result
        return self._resolve(self._thresholds, path, default)

    def _resolve(self, data: dict, path: str, default: Any = None) -> Any:
        """解析点分隔路径"""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    def get_large_amount_thresholds(self) -> list[dict]:
        """
        获取大额交易阈值列表

        Returns:
            [{"min": 50000, "max": 100000, "name": "5万-10万"}, ...]
        """
        levels = self._analysis.get("key_transactions", {}).get("large_amount_thresholds", {})
        return [v for v in levels.values()]

    def get_special_dates(self) -> dict:
        """
        获取特殊日期配置

        Returns:
            {"元旦": {"type": "solar", "month": 1, "day": 1}, ...}
        """
        return self._analysis.get("special_date", {}).get("dates", {})

    def get_special_date_keywords(self) -> list[str]:
        """获取特殊日期关键词"""
        return self._analysis.get("special_date", {}).get("keywords", [])

    def get_special_amounts(self) -> list[float]:
        """
        获取特殊金额列表

        Returns:
            [6.66, 8.88, 13.14, ...]
        """
        return self._analysis.get("special_amount", {}).get("amounts", [])

    def get_special_amount_description(self, amount: float) -> str:
        """获取特殊金额的描述"""
        desc = self._analysis.get("special_amount", {}).get("description", {})
        return desc.get(str(amount), "")

    def get_amount_ranges(self) -> list[dict]:
        """
        获取金额区间配置

        Returns:
            [{"name": "小额", "min": 0, "max": 1000}, ...]
        """
        return (
            self._analysis.get("advanced", {})
            .get("amount_analysis", {})
            .get("ranges", [])
        )

    def get_integer_amount_thresholds(self) -> dict:
        """
        获取整数金额阈值

        Returns:
            {"bank_threshold": 1000, "wechat_threshold": 200, "alipay_threshold": 200}
        """
        return self._analysis.get("integer_amount", {})

    def get_cash_recognition_params(self) -> dict:
        """获取存取现识别参数"""
        return self._analysis.get("cash", {}).get("recognition", {})

    def get_advanced_params(self) -> dict:
        """获取高级分析参数"""
        return self._analysis.get("advanced", {})

