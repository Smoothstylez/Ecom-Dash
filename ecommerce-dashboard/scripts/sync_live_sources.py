from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _bootstrap_path() -> None:
    current = Path(__file__).resolve()
    project_root = current.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run live Shopify/Kaufland API sync for Dashboard-Combined.")
    parser.add_argument("--skip-shopify", action="store_true", help="Do not sync Shopify")
    parser.add_argument("--skip-kaufland", action="store_true", help="Do not sync Kaufland")

    parser.add_argument("--shopify-status", default="any", help="Shopify order status filter (default: any)")
    parser.add_argument("--shopify-page-limit", type=int, default=250)
    parser.add_argument("--shopify-max-pages", type=int, default=500)

    parser.add_argument("--kaufland-storefront", default="de")
    parser.add_argument("--kaufland-page-limit", type=int, default=100)
    parser.add_argument("--kaufland-max-pages", type=int, default=5000)
    parser.add_argument("--skip-returns", action="store_true", help="Skip Kaufland returns sync")
    parser.add_argument("--skip-order-unit-details", action="store_true", help="Skip Kaufland order unit detail calls")

    return parser.parse_args()


def main() -> int:
    _bootstrap_path()

    from app.services.live_sync import run_live_sync

    args = parse_args()

    result = run_live_sync(
        run_shopify=not args.skip_shopify,
        run_kaufland=not args.skip_kaufland,
        shopify_status=str(args.shopify_status or "any"),
        shopify_page_limit=int(args.shopify_page_limit),
        shopify_max_pages=int(args.shopify_max_pages),
        shopify_include_line_items=True,
        shopify_include_fulfillments=True,
        shopify_include_refunds=True,
        shopify_include_transactions=True,
        kaufland_storefront=str(args.kaufland_storefront or "de"),
        kaufland_page_limit=int(args.kaufland_page_limit),
        kaufland_max_pages=int(args.kaufland_max_pages),
        kaufland_include_returns=not args.skip_returns,
        kaufland_include_order_unit_details=not args.skip_order_unit_details,
    )

    print(json.dumps(result, indent=2, ensure_ascii=False))
    status = str(result.get("status") or "").lower()
    return 0 if status in {"success", "partial", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
