// Dataflow Gen2 Mashup Document
// Project: multiple_connections
// Generated: 2026-03-05T10:47:16.122678

section Section1;

// ── sqlproxy ──────────────────────────────────────────────────────
// Source: Unknown
// → Lakehouse.sqlproxy
shared sqlproxy = let
    // TODO: Configure the source for Unknown
    // Connection type not automatically supported
    Source = #table(
        {},
        {
            {},
            {}
        }
    )
in
    Source;

// ── xy ────────────────────────────────────────────────────────────
// Source: MySQL
// → Lakehouse.xy
shared xy = let
    // Source MySQL: mysql55:
    Source = MySQL.Database("mysql55:", "testv1"),
    #"xy Table" = Source{[Schema="testv1", Item="xy"]}[Data],
    Result = #"xy Table"
in
    Result;

// ── xy1 ───────────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.xy1
shared xy1 = let
    // Source SQL Server
    Source = Sql.Database("mssql2012", "TestV1"),
    #"xy1 Table" = Source{[Schema="dbo", Item="xy1"]}[Data],
    Result = #"xy1 Table"
in
    Result;

