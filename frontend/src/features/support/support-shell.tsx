import { SupportPage } from "./support-page";

type SupportShellProps = {
  isActive: boolean;
};

export function SupportShell({ isActive }: SupportShellProps) {
  return <SupportPage isActive={isActive} />;
}
