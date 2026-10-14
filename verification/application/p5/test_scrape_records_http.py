from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.domains.media.router import router
from etk.domains.media.service import MediaService


def test_scrape_records_requires_admin_and_bounds_queries(monkeypatch):
    calls = []
    repository = SimpleNamespace(list_scrape_records=lambda **q: calls.append(q) or {"items": [], "total": 0})
    service = MediaService(
        repository=repository, settings=lambda: None, jobs=None, images=None, p5_events=None, future_events=None
    )
    settings = SimpleNamespace(auth_enabled=True)
    app = FastAPI()
    app.state.container = SimpleNamespace(
        settings=settings,
        repository=lambda _: SimpleNamespace(is_admin=lambda _: False),
        use_case=lambda _: service,
    )
    app.include_router(router)
    monkeypatch.setattr("etk.domains.media.router.get_request_session", lambda _: {})
    with TestClient(app) as client:
        assert client.get("/api/emby/scrape-records").status_code == 403
        assert not calls
        settings.auth_enabled = False
        assert client.get("/api/emby/scrape-records?per_page=5000").status_code == 422
        assert client.get("/api/emby/scrape-records?page=0").status_code == 422
        assert client.get("/api/emby/scrape-records?status=invalid").status_code == 422
        result = client.get("/api/emby/scrape-records?page=2&per_page=25&search=测试&status=incomplete")
        assert result.status_code == 200
        assert calls == [{"page": 2, "per_page": 25, "search": "测试", "status": "incomplete"}]
