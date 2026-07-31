import { AmazonPage } from "./amazon-page";

export function AmazonShell({ isActive }: { isActive: boolean }) {
  return isActive ? <AmazonPage /> : null;
}
