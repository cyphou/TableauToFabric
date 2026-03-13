// Query: Extract
// Source: SQL Server
// Destination: Lakehouse → extract
// Generated: 2026-03-05T10:46:46.796114

let
    // Source SQL Server
    Source = Sql.Database("mssql2012.test.tsi.lan", "TestV1"),
    #"Extract Table" = Source{[Schema="dbo", Item="Extract"]}[Data],
    Result = #"Extract Table"
in
    Result