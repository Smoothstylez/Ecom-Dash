# Project Brief: E-Commerce Dashboard

## Purpose
Unified analytics and bookkeeping dashboard for multi-channel e-commerce operations. Consolidates order data from Shopify and Kaufland marketplaces into a single interface for profit analysis, customer insights, and financial bookkeeping.

## Core Requirements
- Aggregate orders from multiple e-commerce platforms (Shopify, Kaufland, eBay legacy)
- Calculate real profit margins by tracking marketplace fees, payment processor fees, and purchase costs
- Provide customer analytics with geographic visualization (Leaflet map, Globe.gl hexbin)
- Integrate bookkeeping module for transaction tracking, recurring subscriptions, and document management
- Support Google Ads cost tracking and ROAS analysis per product
- Enable live data synchronization from marketplace APIs
- Deploy as Home Assistant add-on with ingress support

## Business Goals
- Single source of truth for e-commerce profitability across all channels
- Automated fee calculations (Shopify Payments, PayPal, Kaufland settlement fees)
- Manual purchase cost enrichment per order for accurate profit tracking
- Cross-platform customer deduplication and repeat customer analytics
- Financial bookkeeping integration with monthly invoice reconciliation

## Target Users
- Dropshipping business owners operating on Shopify and Kaufland
- Users requiring profit visibility after fees and purchase costs
- Businesses tracking Google Ads spend against product revenue

## Key Constraints
- SQLite-based storage (no external database dependency)
- Read-only bootstrap data synced from external API projects
- German language UI (locale: de-DE)
- Home Assistant ingress deployment model
