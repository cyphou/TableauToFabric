// Query: Regions
// Source: SQL Server
// Destination: Lakehouse → regions
// Generated: 2026-03-05T10:46:29.868170

let
    // Source SQL Server
    Source = Sql.Database("sql-finance.contoso.com", "FinanceDB"),
    #"Regions Table" = Source{[Schema="dbo", Item="Regions"]}[Data],
    Result = #"Regions Table"
in
    Result