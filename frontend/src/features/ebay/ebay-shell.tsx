import { EbayPage } from "./ebay-page";

type EbayShellProps = {
  isActive: boolean;
};

export function EbayShell({ isActive }: EbayShellProps) {
  return <EbayPage isActive={isActive} />;
}
