import { InvoicesPage } from "./invoices-page";

type InvoicesShellProps = {
  isActive: boolean;
};

export function InvoicesShell({ isActive }: InvoicesShellProps) {
  return <InvoicesPage isActive={isActive} />;
}
