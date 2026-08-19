from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _configure_db(monkeypatch, tmp_path):
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()


def test_orders_task_is_due_every_five_minutes(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    auto_refresh.initialize_task_state(now)

    assert auto_refresh.select_due_tasks(now) == ["orders", "finance", "inventory_inbound"]
    auto_refresh.record_task_success("orders", now)

    assert "orders" not in auto_refresh.select_due_tasks(now + timedelta(minutes=4, seconds=59))
    assert "orders" in auto_refresh.select_due_tasks(now + timedelta(minutes=5))


def test_task_backoff_grows_and_success_resets_it(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)

    assert auto_refresh.record_task_failure("finance", "SP-API 429", now)["backoff_seconds"] == 300
    assert auto_refresh.record_task_failure("finance", "SP-API 429", now)["backoff_seconds"] == 600
    assert auto_refresh.record_task_success("finance", now)["backoff_level"] == 0


def test_throttled_task_does_not_block_other_due_tasks(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh
    from app.services.importers.amazon_sp_api import AmazonSpApiError

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)

    def fake_run(task_name, _now):
        if task_name == "inventory_inbound":
            raise AmazonSpApiError("SP-API 429 for inventory")
        return {"changed": task_name == "orders"}

    monkeypatch.setattr(auto_refresh, "run_amazon_task", fake_run)
    result = auto_refresh.run_amazon_auto_refresh_cycle(now)

    assert result["tasks"]["orders"]["status"] == "success"
    assert result["tasks"]["inventory_inbound"]["status"] == "backoff"


def test_auto_refresh_status_has_all_task_states(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)

    status = auto_refresh.get_amazon_auto_refresh_status()

    assert set(status["tasks"]) == {"orders", "finance", "inventory_inbound", "reconcile"}


def test_reconcile_waits_one_day_after_scheduler_initialization(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    auto_refresh.initialize_task_state(now)

    assert auto_refresh.get_amazon_auto_refresh_status()["tasks"]["reconcile"]["last_success_at"] is None
    assert "reconcile" not in auto_refresh.select_due_tasks(now + timedelta(hours=23, minutes=59))
    assert "reconcile" in auto_refresh.select_due_tasks(now + timedelta(hours=24))


def test_manual_sync_is_rejected_while_auto_cycle_holds_lock(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    assert auto_refresh._CYCLE_LOCK.acquire(blocking=False)
    try:
        try:
            auto_refresh.run_manual_amazon_sync(include_orders=False)
        except auto_refresh.AmazonAutoRefreshBusyError:
            pass
        else:
            raise AssertionError("manual sync should not overlap auto refresh")
    finally:
        auto_refresh._CYCLE_LOCK.release()


def test_forced_delta_cycle_excludes_daily_reconciliation(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)

    assert auto_refresh.select_due_tasks(now, force=True) == ["orders", "finance", "inventory_inbound"]


def test_forced_delta_cycle_respects_active_quota_backoff(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    auto_refresh.record_task_failure("inventory_inbound", "SP-API 429", now)

    assert "inventory_inbound" not in auto_refresh.select_due_tasks(now + timedelta(minutes=1), force=True)


def test_orders_requests_last_updated_overlap(monkeypatch) -> None:
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    calls = []
    monkeypatch.setattr(client, "request_json", lambda path, **kwargs: calls.append((path, kwargs)) or {"payload": {"Orders": []}})

    client.orders(["A1PA6795UKMFR9"], "2026-08-17T12:00:00Z", updated_after="2026-08-17T12:00:00Z")

    assert calls[0][1]["params"]["LastUpdatedAfter"] == "2026-08-17T12:00:00Z"
    assert "CreatedAfter" not in calls[0][1]["params"]


def test_inbound_delta_passes_bounded_item_lookback(monkeypatch) -> None:
    from app.services.importers.amazon_sp_api import sync_inbound_shipments

    calls = []

    class Client:
        def inbound_shipments(self, _marketplaces): return []
        def bulk_inbound_shipment_items(self, _marketplace, *, lookback_days):
            calls.append(lookback_days)
            return {}
        def modern_inbound_shipments(self, _marketplaces): return {}

    sync_inbound_shipments(Client(), ["A1PA6795UKMFR9"], "run-1", item_lookback_days=1)

    assert calls == [1]


def test_success_interval_starts_when_task_finishes(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    finished = datetime(2026, 8, 17, 12, 6, tzinfo=timezone.utc)
    auto_refresh.record_task_success("orders", finished)

    assert "orders" not in auto_refresh.select_due_tasks(finished + timedelta(minutes=4, seconds=59))
    assert "orders" in auto_refresh.select_due_tasks(finished + timedelta(minutes=5))


def test_database_lease_blocks_second_scheduler_owner(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)

    assert auto_refresh.acquire_database_lease("owner-a", now) is True
    assert auto_refresh.acquire_database_lease("owner-b", now) is False
    auto_refresh.release_database_lease("owner-a")
    assert auto_refresh.acquire_database_lease("owner-b", now) is True


def test_lease_heartbeat_renews_ownership_before_expiry(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    assert auto_refresh.acquire_database_lease("owner-a", now) is True
    assert auto_refresh.renew_database_lease("owner-a", now + timedelta(seconds=80)) is True

    assert auto_refresh.acquire_database_lease("owner-b", now + timedelta(seconds=100)) is False


def test_generic_failure_waits_for_normal_task_interval(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    auto_refresh.record_task_failure("orders", "SP-API 500", now)

    assert "orders" not in auto_refresh.select_due_tasks(now + timedelta(minutes=4, seconds=59))
    assert "orders" in auto_refresh.select_due_tasks(now + timedelta(minutes=5))


def test_partial_sync_is_recorded_as_partial_not_success(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)

    def fake_run(task_name, _now):
        return {
            "changed": True,
            "status": "partial",
            "summary": {"errors": [{"scope": "inventory", "error": "SP-API 500 internal error"}]},
        }

    monkeypatch.setattr(auto_refresh, "run_amazon_task", fake_run)
    result = auto_refresh.run_amazon_auto_refresh_cycle(now)

    assert result["tasks"]["orders"]["status"] == "partial"
    assert "SP-API 500" in result["tasks"]["orders"]["error"]

    state = auto_refresh.get_amazon_auto_refresh_status()["tasks"]["orders"]
    assert state["last_status"] == "partial"
    assert state["backoff_level"] == 0
    assert state["last_success_at"] is None


def test_partial_sync_still_bumps_changestamp(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh
    from app import changestamp

    _configure_db(monkeypatch, tmp_path)
    now = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
    calls = []
    monkeypatch.setattr(changestamp, "bump", lambda: calls.append(1))

    def fake_run(task_name, _now):
        return {"changed": True, "status": "partial", "summary": {"errors": [{"scope": "x", "error": "SP-API 500"}]}}

    monkeypatch.setattr(auto_refresh, "run_amazon_task", fake_run)
    result = auto_refresh.run_amazon_auto_refresh_cycle(now)

    assert len(calls) == len(result["tasks"])
    assert len(calls) > 0


def test_manual_sync_releases_cycle_lock_when_lease_acquisition_raises(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)

    def raise_lease_error(owner_id, now=None):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(auto_refresh, "acquire_database_lease", raise_lease_error)

    try:
        auto_refresh.run_manual_amazon_sync(include_orders=False)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError to propagate")

    assert auto_refresh._CYCLE_LOCK.acquire(blocking=False)
    auto_refresh._CYCLE_LOCK.release()


def test_auto_cycle_releases_cycle_lock_when_lease_acquisition_raises(monkeypatch, tmp_path) -> None:
    import app.services.amazon_auto_refresh as auto_refresh

    _configure_db(monkeypatch, tmp_path)

    def raise_lease_error(owner_id, now=None):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(auto_refresh, "acquire_database_lease", raise_lease_error)

    try:
        auto_refresh.run_amazon_auto_refresh_cycle()
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError to propagate")

    assert auto_refresh._CYCLE_LOCK.acquire(blocking=False)
    auto_refresh._CYCLE_LOCK.release()
