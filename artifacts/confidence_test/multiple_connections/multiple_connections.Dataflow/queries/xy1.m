// Query: xy1
// Source: SQL Server
// Destination: Lakehouse → xy1
// Generated: 2026-03-05T10:47:16.121261

let
    // Source SQL Server
    Source = Sql.Database("mssql2012", "TestV1"),
    #"xy1 Table" = Source{[Schema="dbo", Item="xy1"]}[Data],
    Result = #"xy1 Table"
in
    Result