// Dataflow Gen2 Mashup Document
// Project: TABLEAU_10_TWBX
// Generated: 2026-03-05T10:46:46.797646

section Section1;

// ── xy ────────────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.xy
shared xy = let
    // Source SQL Server
    Source = Sql.Database("mssql2012.test.tsi.lan", "TestV1"),
    #"xy Table" = Source{[Schema="dbo", Item="xy"]}[Data],
    Result = #"xy Table"
in
    Result;

// ── Extract ───────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.extract
shared Extract = let
    // Source SQL Server
    Source = Sql.Database("mssql2012.test.tsi.lan", "TestV1"),
    #"Extract Table" = Source{[Schema="dbo", Item="Extract"]}[Data],
    Result = #"Extract Table"
in
    Result;

