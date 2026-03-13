// Query: Extract
// Source: DATAENGINE
// Destination: Lakehouse → extract
// Generated: 2026-03-05T10:46:41.178108

let
    // TODO: Configure the source for DATAENGINE
    // Connection type not automatically supported
    Source = #table(
        {"Category", "Number of Records", "Order Date", "Sales Target", "Segment"},
        {
            {"Sample 1", 2, 3, 4, "Sample 5"},
            {"Sample 2", 3, 4, 5, "Sample 6"}
        }
    )
in
    Source