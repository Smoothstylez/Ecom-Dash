import { createRoot } from "react-dom/client";

import { App } from "@/app/app";
import "@/styles.css";

const rootElement = document.getElementById("root");

if (!(rootElement instanceof HTMLElement)) {
  throw new Error("Root element '#root' not found.");
}

createRoot(rootElement).render(<App />);
