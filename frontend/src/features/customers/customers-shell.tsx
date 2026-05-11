import { CustomersPage } from "./customers-page";

type CustomersShellProps = {
  isActive: boolean;
};

export function CustomersShell({ isActive }: CustomersShellProps) {
  return <CustomersPage isActive={isActive} />;
}
