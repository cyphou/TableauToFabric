// Query: xy
// Source: Unknown
// Destination: Lakehouse → xy
// Generated: 2026-03-05T10:46:52.415775

let
    // TODO: Configure the source for Unknown
    // Connection type not automatically supported
    Source = #table(
        {"a", "x", "y"},
        {
            {"Sample 1", 2, 3},
            {"Sample 2", 3, 4}
        }
    )
in
    Source