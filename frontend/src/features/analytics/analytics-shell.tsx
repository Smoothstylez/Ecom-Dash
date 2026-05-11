import { AnalyticsPage } from "@/features/analytics/analytics-page";

type AnalyticsShellProps = {
  isActive: boolean;
};

export function AnalyticsShell({ isActive }: AnalyticsShellProps) {
  return <AnalyticsPage isActive={isActive} />;
}
