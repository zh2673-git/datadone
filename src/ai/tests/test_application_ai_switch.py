#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Application 层 AI 可选开关集成测试"""

import pytest

from src.application import Application
from src.ai.provider import MockProvider


class TestApplicationAISwitch:
    def test_ai_disabled_by_default(self):
        app = Application(ai_enabled=False)
        assert app.ai_config.enabled is False
        assert app.narrative_engine is None
        assert app.hypothesis_engine is None

    def test_ai_enabled_initializes_components(self):
        app = Application(ai_enabled=True)
        assert app.ai_config.enabled is True
        assert app.narrative_engine is not None
        assert app.hypothesis_engine is not None
        assert app.ai_report_builder is not None
        assert app.fallback_manager is not None
        assert isinstance(app.ai_provider, MockProvider)

    def test_analyze_without_data_raises(self):
        app = Application(ai_enabled=True)
        with pytest.raises(RuntimeError, match="请先加载数据"):
            app.analyze("all", ai_enabled=True)
