import { Component, type ErrorInfo, type ReactNode } from "react";

type ErrorBoundaryProps = {
  fallbackTitle?: string;
  children: ReactNode;
};

type ErrorBoundaryState = {
  hasError: boolean;
};

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = {
    hasError: false,
  };

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("React boundary caught error", error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <section className="card" style={{ marginTop: 12, padding: 16 }} data-react-error-boundary="true">
          <h2 className="table-title">{this.props.fallbackTitle || "Ansicht konnte nicht geladen werden"}</h2>
          <div className="table-meta" style={{ marginTop: 8 }}>
            Ein Laufzeitfehler wurde abgefangen. Bitte Seite neu laden oder zu einer anderen Ansicht wechseln.
          </div>
        </section>
      );
    }

    return this.props.children;
  }
}
