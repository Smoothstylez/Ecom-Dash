import { render, screen } from "@testing-library/react";
import { Button } from "@/components/ui/button";

describe("Button", () => {
  it("renders its label", () => {
    render(<Button>Legacy oeffnen</Button>);

    expect(screen.getByRole("button", { name: "Legacy oeffnen" })).toBeInTheDocument();
  });
});
