from __future__ import annotations

import json
from typing import Any

import pytest

from app.services.importers import amazon_sp_api


class _FakeResponse:
    def __init__(self, payload: Any):
        self._payload = payload
        self.headers: dict[str, str] = {}

    def read(self, *args: Any, **kwargs: Any) -> bytes:
        if isinstance(self._payload, bytes):
            return self._payload
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(amazon_sp_api, "init_amazon_fba_db", lambda: None)
    monkeypatch.setattr(amazon_sp_api, "reserve_amazon_api_token", lambda bucket_key: 0)
    monkeypatch.setattr(amazon_sp_api, "update_amazon_api_rate_limit", lambda *a, **k: None)
    config = amazon_sp_api.AmazonSpApiConfig(
        client_id="id", client_secret="secret", refresh_token="refresh"
    )
    instance = amazon_sp_api.AmazonSpApiClient(config)
    calls: list[Any] = []
    monkeypatch.setattr(instance, "_lwa_access_token", lambda: calls.append("lwa") or "LWA-TOKEN")
    instance.test_lwa_calls = calls  # type: ignore[attr-defined]
    return instance


@pytest.fixture
def capture(monkeypatch):
    def _install(payload: Any = None):
        captured: list[Any] = []

        def fake_urlopen(request, timeout=None):
            captured.append(request)
            return _FakeResponse({} if payload is None else payload)

        monkeypatch.setattr(amazon_sp_api, "urlopen", fake_urlopen)
        return captured

    return _install


def _headers(request) -> dict[str, str]:
    return {key.lower(): value for key, value in request.header_items()}


def _body(request) -> dict[str, Any]:
    data = request.data
    return json.loads(data.decode("utf-8")) if data else {}


# ── Task 3: generische Report-Erzeugung ──────────────────────────────────────


def test_create_report_sends_report_type_and_window(client, capture):
    captured = capture({"reportId": "r-1"})
    report_id = client.create_report(
        "SC_VAT_TAX_REPORT",
        ["A1PA6795UKMFR9"],
        data_start_time="2026-07-01T00:00:00Z",
        data_end_time="2026-07-31T23:59:59Z",
    )
    assert report_id == "r-1"
    request = captured[0]
    assert request.full_url.endswith("/reports/2021-06-30/reports")
    assert request.get_method() == "POST"
    assert _body(request) == {
        "reportType": "SC_VAT_TAX_REPORT",
        "marketplaceIds": ["A1PA6795UKMFR9"],
        "dataStartTime": "2026-07-01T00:00:00Z",
        "dataEndTime": "2026-07-31T23:59:59Z",
    }


def test_create_report_includes_report_options_when_given(client, capture):
    captured = capture({"reportId": "r-2"})
    client.create_report(
        "GET_FLAT_FILE_VAT_INVOICE_DATA_REPORT",
        ["A1PA6795UKMFR9"],
        data_start_time="2026-07-01T00:00:00Z",
        data_end_time="2026-07-07T23:59:59Z",
        report_options={"ReportOption=All": "true"},
    )
    assert _body(captured[0])["reportOptions"] == {"ReportOption=All": "true"}


def test_create_report_omits_absent_window(client, capture):
    captured = capture({"reportId": "r-3"})
    client.create_report("SC_VAT_TAX_REPORT", ["A1PA6795UKMFR9"])
    body = _body(captured[0])
    assert "dataStartTime" not in body
    assert "dataEndTime" not in body
    assert "reportOptions" not in body


def test_create_report_raises_without_report_id(client, capture):
    capture({})
    with pytest.raises(amazon_sp_api.AmazonSpApiError):
        client.create_report("SC_VAT_TAX_REPORT", ["A1PA6795UKMFR9"])


def test_create_settlement_report_delegates_to_create_report(client, capture):
    captured = capture({"reportId": "r-4"})
    report_id = client.create_settlement_report(["A1PA6795UKMFR9"], "2026-01-01T00:00:00Z")
    assert report_id == "r-4"
    body = _body(captured[0])
    assert body["reportType"] == "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE"
    assert body["dataStartTime"] == "2026-01-01T00:00:00Z"


# ── Task 3: Restricted Data Tokens ───────────────────────────────────────────


def test_rdt_replaces_lwa_token_in_x_amz_access_token(client, capture):
    captured = capture({"payload": {}})
    client.get_report_document("doc-1", access_token="RDT-XYZ")
    headers = _headers(captured[0])
    assert headers["x-amz-access-token"] == "RDT-XYZ"
    assert client.test_lwa_calls == []


def test_no_invented_restricted_data_token_header_is_sent(client, capture):
    captured = capture({"payload": {}})
    client.get_report_document("doc-1", access_token="RDT-XYZ")
    headers = _headers(captured[0])
    assert "restricteddatatoken" not in headers
    assert "restricted-data-token" not in headers
    assert [key for key in headers if "restricted" in key] == []


def test_without_rdt_the_lwa_token_is_used(client, capture):
    captured = capture({"payload": {}})
    client.get_report_document("doc-1")
    assert _headers(captured[0])["x-amz-access-token"] == "LWA-TOKEN"
    assert client.test_lwa_calls == ["lwa"]


def test_exactly_one_access_token_header_is_sent(client, capture):
    captured = capture({"payload": {}})
    client.request_json("/reports/2021-06-30/reports/x", access_token="RDT-ABC")
    keys = [key.lower() for key, _ in captured[0].header_items() if key.lower() == "x-amz-access-token"]
    assert keys == ["x-amz-access-token"]


def test_create_restricted_data_token_posts_to_tokens_api(client, capture):
    captured = capture({"restrictedDataToken": "RDT-NEW", "expiresIn": 3600})
    token = client.create_restricted_data_token(
        [{"method": "GET", "path": "/reports/2021-06-30/documents/doc-1"}]
    )
    assert token == "RDT-NEW"
    request = captured[0]
    assert request.full_url.endswith("/tokens/2021-03-01/restrictedDataToken")
    assert request.get_method() == "POST"
    assert _body(request) == {
        "restrictedResources": [{"method": "GET", "path": "/reports/2021-06-30/documents/doc-1"}]
    }


def test_create_restricted_data_token_raises_without_token(client, capture):
    capture({})
    with pytest.raises(amazon_sp_api.AmazonSpApiError):
        client.create_restricted_data_token([{"method": "GET", "path": "/reports/x"}])


def test_download_report_text_accepts_rdt_header(client, capture):
    captured = capture(b"col-a\tcol-b\n1\t2\n")
    text = client.download_report_text("https://example.test/doc", access_token="RDT-DL")
    assert text.startswith("col-a")
    assert _headers(captured[0])["x-amz-access-token"] == "RDT-DL"


def test_restricted_report_types_are_declared():
    assert amazon_sp_api.REPORT_TYPES_REQUIRING_RDT == {
        "SC_VAT_TAX_REPORT",
        "GET_VAT_TRANSACTION_DATA",
        "GET_FLAT_FILE_VAT_INVOICE_DATA_REPORT",
        "GET_XML_VAT_INVOICE_DATA_REPORT",
    }
    assert amazon_sp_api.report_requires_rdt("SC_VAT_TAX_REPORT") is True
    assert amazon_sp_api.report_requires_rdt("GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE") is False
