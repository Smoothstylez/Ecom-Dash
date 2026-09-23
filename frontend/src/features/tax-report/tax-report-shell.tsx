import { TaxReportPage } from "./tax-report-page";

export function TaxReportShell({ isActive }: { isActive: boolean }) {
  return isActive ? <TaxReportPage /> : null;
}
