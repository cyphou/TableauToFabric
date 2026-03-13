// Dataflow Gen2 Mashup Document
// Project: simple_report
// Generated: 2026-03-05T10:47:43.612534

section Section1;

// ── Sales ─────────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.sales
shared Sales = let
    // Source SQL Server
    Source = Sql.Database("sql-prod-01.contoso.com", "SalesDB"),
    #"Sales Table" = Source{[Schema="dbo", Item="Sales"]}[Data],
    Result = #"Sales Table"
in
    Result;

