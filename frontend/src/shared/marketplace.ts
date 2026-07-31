export const MARKETPLACES = ["shopify", "kaufland", "amazon"] as const;

export type Marketplace = (typeof MARKETPLACES)[number];
export type MarketplaceFilter = Marketplace | "";

export function normalizeMarketplaceFilter(value: unknown): MarketplaceFilter {
  const marketplace = String(value || "").trim().toLowerCase();
  return MARKETPLACES.includes(marketplace as Marketplace) ? marketplace as Marketplace : "";
}

export function marketplaceLabel(value: unknown) {
  switch (String(value || "").trim().toLowerCase()) {
    case "shopify":
      return "Shopify";
    case "kaufland":
      return "Kaufland";
    case "amazon":
      return "Amazon";
    default:
      return String(value || "").trim() || "Alle";
  }
}

export function isAmazonFba(marketplace: unknown, ...fulfillmentMarkers: unknown[]) {
  if (String(marketplace || "").trim().toLowerCase() !== "amazon") {
    return false;
  }

  return fulfillmentMarkers.some((marker) => {
    if (marker === true) {
      return true;
    }
    const token = String(marker || "").trim().toLowerCase().replace(/[\s-]+/g, "_");
    return token === "afn" || token.includes("fba") || token.includes("amazon_fulfilled");
  });
}
