from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from app.services.orders import list_all_orders_without_pagination


def _new_metric_bucket() -> dict[str, int]:
    return {
        "order_count": 0,
        "revenue_total_cents": 0,
        "fees_total_cents": 0,
        "after_fees_total_cents": 0,
        "purchase_total_cents": 0,
        "profit_total_cents": 0,
    }


def _parse_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None

    normalized = text
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None

    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_date_token(value: Optional[str]) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _normalize_status_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_return_like_status(value: Any) -> bool:
    token = _normalize_status_token(value)
    if not token:
        return False
    keywords = (
        "cancel",
        "cancelled",
        "canceled",
        "void",
        "return",
        "returned",
        "refund",
        "refunded",
        "partially_refunded",
        "rma",
        "revoked",
        "returning",
    )
    return any(keyword in token for keyword in keywords)


def _is_completed_like_status(value: Any) -> bool:
    token = _normalize_status_token(value)
    if not token:
        return False
    keywords = (
        "fulfilled",
        "shipped",
        "delivered",
        "completed",
        "sent",
        "done",
        "success",
    )
    return any(keyword in token for keyword in keywords)


def _is_pending_like_status(value: Any) -> bool:
    token = _normalize_status_token(value)
    if not token:
        return False
    keywords = (
        "pending",
        "open",
        "processing",
        "unfulfilled",
        "created",
        "new",
        "in_progress",
        "ready",
        "await",
        "hold",
    )
    return any(keyword in token for keyword in keywords)


def _safe_pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((part / total) * 100.0, 2)


def _normalize_customer_key(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token in {"", "-", "unknown", "unbekannt", "n/a", "na"}:
        return ""
    return token


def _week_start(day_value: date) -> date:
    return day_value - timedelta(days=day_value.weekday())


def _month_start(day_value: date) -> date:
    return day_value.replace(day=1)


def _shift_month(month_value: date, delta: int) -> date:
    month_index = (month_value.month - 1) + delta
    year = month_value.year + (month_index // 12)
    month = (month_index % 12) + 1
    return date(year, month, 1)


def _resolve_trend_granularity(
    requested: Optional[str],
    start_day: Optional[date],
    end_day: Optional[date],
) -> str:
    token = str(requested or "").strip().lower()
    aliases = {
        "day": "day",
        "daily": "day",
        "woche": "week",
        "week": "week",
        "weekly": "week",
        "monat": "month",
        "month": "month",
        "monthly": "month",
        "jahr": "month",
        "year": "month",
        "yearly": "month",
        "auto": "auto",
    }
    normalized = aliases.get(token, "auto")
    if normalized != "auto":
        return normalized

    if start_day is None or end_day is None:
        return "day"

    span_days = max((end_day - start_day).days + 1, 1)
    if span_days <= 62:
        return "day"
    if span_days <= 370:
        return "week"
    return "month"


def _trend_title(granularity: str, span_days: int) -> str:
    if granularity == "day":
        return "Tagesverlauf"
    if granularity == "week":
        return "Wochenverlauf"
    if span_days >= 540:
        return "Jahresverlauf"
    return "Monatsverlauf"


def _build_trend_points(
    *,
    granularity: str,
    start_day: Optional[date],
    end_day: Optional[date],
    day_map: dict[date, dict[str, int]],
    week_map: dict[date, dict[str, int]],
    month_map: dict[date, dict[str, int]],
) -> list[dict[str, Any]]:
    if start_day is None or end_day is None or start_day > end_day:
        return []

    points: list[dict[str, Any]] = []

    if granularity == "day":
        cursor = start_day
        while cursor <= end_day:
            row = day_map.get(cursor) or _new_metric_bucket()
            points.append(
                {
                    "bucket_start": cursor.isoformat(),
                    "bucket_end": cursor.isoformat(),
                    **row,
                }
            )
            cursor += timedelta(days=1)
        return points

    if granularity == "week":
        cursor = _week_start(start_day)
        while cursor <= end_day:
            bucket_end = min(cursor + timedelta(days=6), end_day)
            row = week_map.get(cursor) or _new_metric_bucket()
            points.append(
                {
                    "bucket_start": cursor.isoformat(),
                    "bucket_end": bucket_end.isoformat(),
                    **row,
                }
            )
            cursor += timedelta(days=7)
        return points

    cursor = _month_start(start_day)
    while cursor <= end_day:
        next_month = _shift_month(cursor, 1)
        bucket_end = min(next_month - timedelta(days=1), end_day)
        row = month_map.get(cursor) or _new_metric_bucket()
        points.append(
            {
                "bucket_start": cursor.isoformat(),
                "bucket_end": bucket_end.isoformat(),
                **row,
            }
        )
        cursor = next_month
    return points


def _sum_points(points: list[dict[str, Any]], key: str) -> int:
    total = 0
    for point in points:
        total += int(point.get(key) or 0)
    return total


def build_analytics(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    trend_granularity: Optional[str] = None,
) -> dict[str, Any]:
    orders = list_all_orders_without_pagination(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=query,
    )

    revenue_total = 0
    fees_total = 0
    after_fees_total = 0
    purchase_total = 0
    profit_total = 0
    shopify_revenue = 0
    kaufland_revenue = 0
    shipping_total = 0
    orders_with_purchase = 0
    purchase_missing_count = 0
    returns_order_count = 0

    customer_counter: Counter[str] = Counter()
    payment_counter: Counter[str] = Counter()

    status_summary = {
        "completed_like_count": 0,
        "pending_like_count": 0,
        "return_like_count": 0,
        "other_count": 0,
    }

    marketplace_map: dict[str, dict[str, int]] = {
        "shopify": {
            "order_count": 0,
            "revenue_total_cents": 0,
            "fees_total_cents": 0,
            "after_fees_total_cents": 0,
            "purchase_total_cents": 0,
            "profit_total_cents": 0,
            "shipping_total_cents": 0,
            "returns_order_count": 0,
            "orders_with_purchase_count": 0,
        },
        "kaufland": {
            "order_count": 0,
            "revenue_total_cents": 0,
            "fees_total_cents": 0,
            "after_fees_total_cents": 0,
            "purchase_total_cents": 0,
            "profit_total_cents": 0,
            "shipping_total_cents": 0,
            "returns_order_count": 0,
            "orders_with_purchase_count": 0,
        },
    }

    day_map: dict[date, dict[str, int]] = defaultdict(_new_metric_bucket)
    week_map: dict[date, dict[str, int]] = defaultdict(_new_metric_bucket)
    month_map_date: dict[date, dict[str, int]] = defaultdict(_new_metric_bucket)

    top_articles: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "order_count": 0,
            "revenue_total_cents": 0,
            "profit_total_cents": 0,
        }
    )

    min_order_day: Optional[date] = None
    max_order_day: Optional[date] = None

    # Heatmap: 7 days × 24 hours → order count
    heatmap: dict[tuple[int, int], int] = defaultdict(int)

    for order in orders:
        total = int(order.get("total_cents") or 0)
        fees = int(order.get("fees_cents") or 0)
        after_fees = int(order.get("after_fees_cents") or 0)
        purchase = int(order.get("purchase_cost_cents") or 0)
        profit = int(order.get("profit_cents") or (after_fees - purchase))
        shipping = int(order.get("shipping_cents") or 0)
        shipping = max(shipping, 0)

        fulfillment_status = order.get("fulfillment_status")
        financial_status = order.get("financial_status")
        raw_status = order.get("raw_status")
        is_return_like = any(
            _is_return_like_status(value) for value in (fulfillment_status, financial_status, raw_status)
        )
        if is_return_like:
            returns_order_count += 1

        if is_return_like:
            status_summary["return_like_count"] += 1
        elif any(_is_completed_like_status(value) for value in (fulfillment_status, raw_status, financial_status)):
            status_summary["completed_like_count"] += 1
        elif any(_is_pending_like_status(value) for value in (fulfillment_status, raw_status, financial_status)):
            status_summary["pending_like_count"] += 1
        else:
            status_summary["other_count"] += 1

        revenue_total += total
        fees_total += fees
        after_fees_total += after_fees
        purchase_total += purchase
        profit_total += profit
        shipping_total += shipping

        if purchase > 0:
            orders_with_purchase += 1
        else:
            purchase_missing_count += 1

        customer_key = _normalize_customer_key(order.get("customer"))
        if customer_key:
            customer_counter[customer_key] += 1

        payment_key = str(order.get("payment_method") or "").strip()
        if payment_key:
            payment_counter[payment_key] += 1

        if order.get("marketplace") == "shopify":
            shopify_revenue += total
        elif order.get("marketplace") == "kaufland":
            kaufland_revenue += total

        market_key = str(order.get("marketplace") or "").strip().lower()
        market = marketplace_map.get(market_key)
        if market is not None:
            market["order_count"] += 1
            market["revenue_total_cents"] += total
            market["fees_total_cents"] += fees
            market["after_fees_total_cents"] += after_fees
            market["purchase_total_cents"] += purchase
            market["profit_total_cents"] += profit
            market["shipping_total_cents"] += shipping
            if is_return_like:
                market["returns_order_count"] += 1
            if purchase > 0:
                market["orders_with_purchase_count"] += 1

        order_dt = _parse_datetime(order.get("order_date"))
        if order_dt is not None:
            order_day = order_dt.date()
            if min_order_day is None or order_day < min_order_day:
                min_order_day = order_day
            if max_order_day is None or order_day > max_order_day:
                max_order_day = order_day

            # Heatmap: weekday 0=Monday, hour 0-23
            heatmap[(order_dt.weekday(), order_dt.hour)] += 1

            week_key = _week_start(order_day)
            month_key = _month_start(order_day)
            for bucket in (day_map[order_day], week_map[week_key], month_map_date[month_key]):
                bucket["order_count"] += 1
                bucket["revenue_total_cents"] += total
                bucket["fees_total_cents"] += fees
                bucket["after_fees_total_cents"] += after_fees
                bucket["purchase_total_cents"] += purchase
                bucket["profit_total_cents"] += profit

        article_key = str(order.get("article") or "-").strip() or "-"
        article = top_articles[article_key]
        article["order_count"] += 1
        article["revenue_total_cents"] += total
        article["profit_total_cents"] += profit

    margin_pct = 0.0
    if after_fees_total > 0:
        margin_pct = round((profit_total / after_fees_total) * 100.0, 2)

    order_count = len(orders)
    aov_cents = int(round(revenue_total / order_count)) if order_count > 0 else 0
    avg_profit_per_order_cents = int(round(profit_total / order_count)) if order_count > 0 else 0
    fees_ratio_pct = _safe_pct(fees_total, revenue_total)
    purchase_coverage_pct = _safe_pct(orders_with_purchase, order_count)
    return_rate_pct = _safe_pct(returns_order_count, order_count)
    unique_customers = len(customer_counter)
    repeat_customers = sum(1 for count in customer_counter.values() if count > 1)
    repeat_customer_rate_pct = _safe_pct(repeat_customers, unique_customers)

    marketplace_rows: list[dict[str, Any]] = []
    for market_key in ("shopify", "kaufland"):
        market = marketplace_map[market_key]
        market_order_count = int(market["order_count"])
        market_revenue_total = int(market["revenue_total_cents"])
        market_after_fees_total = int(market["after_fees_total_cents"])
        market_profit_total = int(market["profit_total_cents"])

        market_margin_pct = 0.0
        if market_after_fees_total > 0:
            market_margin_pct = round((market_profit_total / market_after_fees_total) * 100.0, 2)

        market_aov_cents = int(round(market_revenue_total / market_order_count)) if market_order_count > 0 else 0
        market_avg_profit_cents = int(round(market_profit_total / market_order_count)) if market_order_count > 0 else 0

        marketplace_rows.append(
            {
                "marketplace": market_key,
                **market,
                "margin_pct": market_margin_pct,
                "aov_cents": market_aov_cents,
                "avg_profit_per_order_cents": market_avg_profit_cents,
                "return_rate_pct": _safe_pct(market["returns_order_count"], market_order_count),
                "purchase_coverage_pct": _safe_pct(market["orders_with_purchase_count"], market_order_count),
                "revenue_share_pct": _safe_pct(market_revenue_total, revenue_total),
            }
        )

    payment_rows = [
        {
            "payment_method": payment_method,
            "order_count": count,
            "share_pct": _safe_pct(count, order_count),
        }
        for payment_method, count in payment_counter.most_common(6)
    ]

    monthly_rows = [
        {
            "month": key.strftime("%Y-%m"),
            **values,
        }
        for key, values in month_map_date.items()
    ]
    monthly_rows.sort(key=lambda item: item["month"])

    article_rows = [
        {
            "article": article,
            **values,
        }
        for article, values in top_articles.items()
    ]
    article_rows.sort(
        key=lambda item: (item["revenue_total_cents"], item["order_count"]),
        reverse=True,
    )

    requested_start = _parse_date_token(from_date)
    requested_end = _parse_date_token(to_date)
    if requested_start is not None and requested_end is not None and requested_start > requested_end:
        requested_start, requested_end = requested_end, requested_start

    if min_order_day is not None and max_order_day is not None:
        if requested_start is None:
            trend_start = min_order_day
        else:
            trend_start = max(requested_start, min_order_day)
        if requested_end is None:
            trend_end = max_order_day
        else:
            trend_end = min(requested_end, max_order_day)
    else:
        trend_start = requested_start
        trend_end = requested_end

    if trend_start is not None and trend_end is None:
        trend_end = trend_start
    if trend_end is not None and trend_start is None:
        trend_start = trend_end
    if trend_start is not None and trend_end is not None and trend_start > trend_end:
        trend_start, trend_end = trend_end, trend_start

    resolved_granularity = _resolve_trend_granularity(trend_granularity, trend_start, trend_end)
    trend_points = _build_trend_points(
        granularity=resolved_granularity,
        start_day=trend_start,
        end_day=trend_end,
        day_map=day_map,
        week_map=week_map,
        month_map=month_map_date,
    )

    trend_span_days = 0
    if trend_start is not None and trend_end is not None:
        trend_span_days = max((trend_end - trend_start).days + 1, 0)

    trend_payload = {
        "granularity": resolved_granularity,
        "title": _trend_title(resolved_granularity, trend_span_days),
        "from": trend_start.isoformat() if trend_start is not None else "",
        "to": trend_end.isoformat() if trend_end is not None else "",
        "point_count": len(trend_points),
        "order_count": _sum_points(trend_points, "order_count"),
        "revenue_total_cents": _sum_points(trend_points, "revenue_total_cents"),
        "profit_total_cents": _sum_points(trend_points, "profit_total_cents"),
        "points": trend_points,
    }

    # ── Build heatmap grid (7 days × 24 hours) ──
    heatmap_grid: list[list[int]] = [[0] * 24 for _ in range(7)]
    for (day_idx, hour_idx), count in heatmap.items():
        heatmap_grid[day_idx][hour_idx] = count

    # ── Previous period comparison ──
    prev_period: Optional[dict[str, Any]] = None
    if requested_start is not None and requested_end is not None:
        span_days = (requested_end - requested_start).days + 1
        prev_end = requested_start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=span_days - 1)
        prev_orders = list_all_orders_without_pagination(
            from_date=prev_start.isoformat(),
            to_date=prev_end.isoformat(),
            marketplace=marketplace,
            query=query,
        )
        prev_revenue = 0
        prev_profit = 0
        prev_after_fees = 0
        prev_purchase = 0
        prev_fees = 0
        for prev_order in prev_orders:
            prev_revenue += int(prev_order.get("total_cents") or 0)
            p_fees = int(prev_order.get("fees_cents") or 0)
            p_after = int(prev_order.get("after_fees_cents") or 0)
            p_purchase = int(prev_order.get("purchase_cost_cents") or 0)
            p_profit = int(prev_order.get("profit_cents") or (p_after - p_purchase))
            prev_fees += p_fees
            prev_after_fees += p_after
            prev_purchase += p_purchase
            prev_profit += p_profit
        prev_order_count = len(prev_orders)
        prev_margin = 0.0
        if prev_after_fees > 0:
            prev_margin = round((prev_profit / prev_after_fees) * 100.0, 2)
        prev_period = {
            "from": prev_start.isoformat(),
            "to": prev_end.isoformat(),
            "span_days": span_days,
            "order_count": prev_order_count,
            "revenue_total_cents": prev_revenue,
            "fees_total_cents": prev_fees,
            "after_fees_total_cents": prev_after_fees,
            "purchase_total_cents": prev_purchase,
            "profit_total_cents": prev_profit,
            "margin_pct": prev_margin,
        }

    return {
        "order_count": order_count,
        "revenue_total_cents": revenue_total,
        "fees_total_cents": fees_total,
        "after_fees_total_cents": after_fees_total,
        "purchase_total_cents": purchase_total,
        "profit_total_cents": profit_total,
        "margin_pct": margin_pct,
        "aov_cents": aov_cents,
        "avg_profit_per_order_cents": avg_profit_per_order_cents,
        "fees_ratio_pct": fees_ratio_pct,
        "shipping_total_cents": shipping_total,
        "orders_with_purchase_count": orders_with_purchase,
        "purchase_missing_count": purchase_missing_count,
        "purchase_coverage_pct": purchase_coverage_pct,
        "returns_order_count": returns_order_count,
        "return_rate_pct": return_rate_pct,
        "unique_customers": unique_customers,
        "repeat_customers": repeat_customers,
        "repeat_customer_rate_pct": repeat_customer_rate_pct,
        "status_summary": status_summary,
        "marketplaces": marketplace_rows,
        "top_payment_methods": payment_rows,
        "shopify_revenue_total_cents": shopify_revenue,
        "kaufland_revenue_total_cents": kaufland_revenue,
        "monthly": monthly_rows,
        "trend": trend_payload,
        "top_articles": article_rows[:12],
        "purchase_heatmap": heatmap_grid,
        "previous_period": prev_period,
    }
