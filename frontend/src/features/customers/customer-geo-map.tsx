import { useEffect, useRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import type { CustomerLocationPoint } from "@/features/customers/types";
import { formatCurrencyFromCents, formatNumber } from "@/lib/format";

interface CustomerGeoMapProps {
  points: CustomerLocationPoint[];
}

export function CustomerGeoMap({ points }: CustomerGeoMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const layerRef = useRef<L.LayerGroup | null>(null);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) {
      return;
    }

    const map = L.map(containerRef.current, {
      zoomControl: true,
      worldCopyJump: true,
      minZoom: 1,
      maxZoom: 18,
    });

    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      subdomains: "abcd",
      maxZoom: 18,
      attribution: "&copy; OpenStreetMap &copy; CARTO",
    }).addTo(map);

    const layer = L.layerGroup().addTo(map);
    map.setView([20, 10], 2);

    mapRef.current = map;
    layerRef.current = layer;

    return () => {
      map.remove();
      mapRef.current = null;
      layerRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    const layer = layerRef.current;
    if (!map || !layer) {
      return;
    }

    layer.clearLayers();

    if (!points.length) {
      map.setView([20, 10], 2);
      return;
    }

    const bounds: L.LatLngTuple[] = [];

    for (const point of points) {
      const lat = Number(point.lat);
      const lng = Number(point.lng);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
        continue;
      }

      const dominant = String(point.dominant_marketplace ?? "").trim().toLowerCase();
      const color = dominant === "kaufland" ? "#1f8b5f" : "#2d5ea8";
      const orderCount = Number(point.order_count ?? 0);
      const radius = Math.max(4, Math.min(22, 4 + Math.sqrt(Math.max(orderCount, 0)) * 1.8));
      const locationLabel = [String(point.city ?? "").trim(), String(point.country ?? point.country_code ?? "").trim()]
        .filter(Boolean)
        .join(", ") || "Unbekannt";

      const marker = L.circleMarker([lat, lng], {
        radius,
        color,
        weight: 1.4,
        fillColor: color,
        fillOpacity: 0.35,
      });

      marker.bindPopup(
        [
          `<strong>${locationLabel}</strong>`,
          `Orders: ${formatNumber(orderCount)}`,
          `Umsatz: ${formatCurrencyFromCents(point.revenue_total_cents)}`,
          `Gewinn: ${formatCurrencyFromCents(point.profit_total_cents)}`,
        ].join("<br>"),
      );

      marker.addTo(layer);
      bounds.push([lat, lng]);
    }

    map.invalidateSize();
    if (bounds.length) {
      map.fitBounds(bounds, { padding: [26, 26], maxZoom: 5 });
    }
  }, [points]);

  return <div className="h-[430px] w-full overflow-hidden rounded-[20px]" ref={containerRef} />;
}
