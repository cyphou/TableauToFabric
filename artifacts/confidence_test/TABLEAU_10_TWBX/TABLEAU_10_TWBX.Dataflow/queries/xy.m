// Query: xy
// Source: SQL Server
// Destination: Lakehouse → xy
// Generated: 2026-03-05T10:46:46.794152

let
    // Source SQL Server
    Source = Sql.Database("mssql2012.test.tsi.lan", "TestV1"),
    #"xy Table" = Source{[Schema="dbo", Item="xy"]}[Data],
    Result = #"xy Table"
in
    Result