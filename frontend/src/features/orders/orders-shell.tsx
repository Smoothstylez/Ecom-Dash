import { OrdersPage } from "./orders-page";

type OrdersShellProps = {
  isActive: boolean;
};

export function OrdersShell({ isActive }: OrdersShellProps) {
  return <OrdersPage isActive={isActive} />;
}
