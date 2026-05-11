import { GoogleAdsPage } from "./google-ads-page";

type GoogleAdsShellProps = {
  isActive: boolean;
};

export function GoogleAdsShell({ isActive }: GoogleAdsShellProps) {
  return <GoogleAdsPage isActive={isActive} />;
}
