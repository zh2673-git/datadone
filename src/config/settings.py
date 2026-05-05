"""配置管理模块 - 惰性加载配置文件"""

import json
from pathlib import Path
from typing import Any, Optional


class AppSettings:
    """应用配置管理 - 惰性加载，支持点分隔路径访问"""

    _instance: Optional["AppSettings"] = None

    def __init__(self, config_dir: Optional[str] = None):
        if config_dir is None:
            config_dir = str(Path(__file__).resolve().parent.parent.parent / "config")
        self._config_dir = config_dir
        self._config: Optional[dict] = None
        self._keywords: Optional[dict] = None
        self._thresholds: Optional[dict] = None

    @classmethod
    def get_instance(cls, config_dir: Optional[str] = None) -> "AppSettings":
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls(config_dir)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（用于测试）"""
        cls._instance = None

    def _load_json(self, filename: str) -> dict:
        """加载 JSON 配置文件"""
        filepath = Path(self._config_dir) / filename
        if not filepath.exists():
            return {}
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    @property
    def config(self) -> dict:
        """主配置（惰性加载）"""
        if self._config is None:
            self._config = self._load_json("config.json")
        return self._config

    @property
    def keywords(self) -> dict:
        """关键词配置（惰性加载）"""
        if self._keywords is None:
            self._keywords = self._load_json("keywords.json")
        return self._keywords

    @property
    def thresholds(self) -> dict:
        """阈值配置（惰性加载）"""
        if self._thresholds is None:
            self._thresholds = self._load_json("thresholds.json")
        return self._thresholds

    def get(self, path: str, default: Any = None) -> Any:
        """
        通过点分隔路径获取配置值

        Args:
            path: 点分隔路径，如 "data_sources.bank.name_column"
            default: 默认值

        Returns:
            配置值，路径不存在时返回 default
        """
        # 按第一段路径分发到对应配置文件
        parts = path.split(".")
        if not parts:
            return default

        first_key = parts[0]

        # 确定使用哪个配置字典
        if first_key == "data_sources" or first_key == "analysis" or first_key == "export" or first_key == "app" or first_key == "output" or first_key == "ui":
            data = self.config
        elif first_key == "keywords" or first_key == "bank" or first_key == "key_transactions":
            data = self.keywords
        elif first_key == "special_date" or first_key == "integer_amount" or first_key == "special_amount" or first_key == "cash" or first_key == "advanced":
            data = self.thresholds
        else:
            # 尝试在所有配置中查找
            for data in [self.config, self.keywords, self.thresholds]:
                result = self._resolve_path(data, parts)
                if result is not None:
                    return result
            return default

        return self._resolve_path(data, parts)

    def _resolve_path(self, data: dict, parts: list) -> Any:
        """递归解析点分隔路径"""
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def get_data_source_config(self, source_type: str) -> dict:
        """获取指定数据源的配置"""
        return self.config.get("data_sources", {}).get(source_type, {})

    def reload(self) -> None:
        """重新加载所有配置"""
        self._config = None
        self._keywords = None
        self._thresholds = None
