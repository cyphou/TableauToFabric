// Dataflow Gen2 Mashup Document
// Project: sample-superstore
// Generated: 2026-03-04T19:09:17.717318

section Section1;

// ── Orders ────────────────────────────────────────────────────────
// Source: Unknown
// → Lakehouse.orders
shared Orders = let
    // TODO: Configure the source for Unknown
    // Connection type not automatically supported
    Source = #table(
        {"Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID", "Customer Name", "Segment", "City", "State", "Country", "Postal Code", "Market", "Region", "Product ID", "Category", "Sub-Category", "Product Name", "Sales", "Quantity", "Discount", "Profit", "Shipping Cost", "Order Priority"},
        {
            {1, "Sample 2", 3, 4, "Sample 5", "Sample 6", "Sample 7", "Sample 8", "Sample 9", "Sample 10", "Sample 11", 12, "Sample 13", "Sample 14", "Sample 15", "Sample 16", "Sample 17", "Sample 18", 19, 20, 21, 22, 23, "Sample 24"},
            {2, "Sample 3", 4, 5, "Sample 6", "Sample 7", "Sample 8", "Sample 9", "Sample 10", "Sample 11", "Sample 12", 13, "Sample 14", "Sample 15", "Sample 16", "Sample 17", "Sample 18", "Sample 19", 20, 21, 22, 23, 24, "Sample 25"}
        }
    )
in
    Source;

