// Dataflow Gen2 Mashup Document
// Project: ephemeral_field
// Generated: 2026-03-05T10:46:58.662324

section Section1;

// ── xy ────────────────────────────────────────────────────────────
// Source: Unknown
// → Lakehouse.xy
shared xy = let
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
    Source;

