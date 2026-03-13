// Query: Sales
// Source: SQL Server
// Destination: Lakehouse → sales
// Generated: 2026-03-05T10:47:43.611030

let
    // Source SQL Server
    Source = Sql.Database("sql-prod-01.contoso.com", "SalesDB"),
    #"Sales Table" = Source{[Schema="dbo", Item="Sales"]}[Data],
    Result = #"Sales Table"
in
    Result