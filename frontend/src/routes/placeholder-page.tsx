import { ArrowUpRight } from "lucide-react";
import { SurfaceCard } from "@/components/ui/surface-card";
import { Button } from "@/components/ui/button";

interface PlaceholderPageProps {
  title: string;
  description: string;
  legacyHref: string;
}

export function PlaceholderPage({ title, description, legacyHref }: PlaceholderPageProps) {
  return (
    <div className="grid gap-4 xl:grid-cols-[1.4fr_1fr]">
      <SurfaceCard description={description} title={title}>
        <p className="text-xs font-medium uppercase tracking-[0.18em] text-[var(--ink-4)]">Route in Vorbereitung</p>
      </SurfaceCard>

      <SurfaceCard title="Legacy Zugriff">
        <p className="text-sm leading-6 text-[var(--ink-4)]">
          Bis zur vollstaendigen Migration bleibt die bestehende Seite unter ihrer bisherigen URL verfuegbar.
        </p>
        <a className="mt-5 inline-flex" href={legacyHref} rel="noreferrer">
          <Button variant="outline">
            Legacy oeffnen
            <ArrowUpRight className="ml-2 h-4 w-4" />
          </Button>
        </a>
      </SurfaceCard>
    </div>
  );
}
