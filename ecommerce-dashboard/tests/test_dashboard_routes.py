from __future__ import annotations

from pathlib import Path


def _build_dashboard_client(monkeypatch, tmp_path: Path):
    import importlib

    from fastapi.testclient import TestClient

    monkeypatch.setenv("AUTO_SYNC_ON_STARTUP", "0")
    monkeypatch.setenv("AMAZON_AUTO_REFRESH_ENABLED", "0")
    monkeypatch.setenv("LIVE_SYNC_BACKGROUND_ENABLED", "0")
    monkeypatch.setenv("COMBINED_DB_PATH", str(tmp_path / "combined.sqlite3"))

    import app.main as main_module

    importlib.reload(main_module)
    return TestClient(main_module.app)


def test_amazon_direct_route_serves_dashboard_shell(monkeypatch, tmp_path) -> None:
    client = _build_dashboard_client(monkeypatch, tmp_path)

    response = client.get("/amazon")

    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
