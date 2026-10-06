import importlib

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "app.main",
        "app.config",
        "app.handlers.admin",
        "app.handlers.analytics",
        "app.handlers.common",
        "app.handlers.messages",
        "app.handlers.transactions",
        "app.services.ai_service",
        "app.services.analytics_service",
        "app.services.google_sheets",
        "app.utils.keyboards",
    ],
)
def test_module_imports(module: str) -> None:
    importlib.import_module(module)
