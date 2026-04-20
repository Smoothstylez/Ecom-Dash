import type { BookingTransaction } from "@/features/bookings/types";
import { bookingTransactionCategory, filterBookingTransactions } from "@/features/bookings/utils";

describe("bookings utils", () => {
  it("maps booking types to the expected categories", () => {
    expect(bookingTransactionCategory("SALE")).toBe("sale");
    expect(bookingTransactionCategory("expense")).toBe("invoice");
    expect(bookingTransactionCategory("unknown")).toBe("other");
  });

  it("filters transactions by category and search term", () => {
    const items: BookingTransaction[] = [
      { id: "1", type: "SALE", provider: "shopify", reference: "#1001", notes: "Order payout" },
      { id: "2", type: "REFUND", provider: "paypal", reference: "PP-77", notes: "Customer refund" },
    ];

    expect(filterBookingTransactions(items, { query: "paypal", category: "", type: "" })).toEqual([items[1]]);
    expect(filterBookingTransactions(items, { query: "", category: "sale", type: "" })).toEqual([items[0]]);
    expect(filterBookingTransactions(items, { query: "refund", category: "refund", type: "REFUND" })).toEqual([items[1]]);
  });
});
