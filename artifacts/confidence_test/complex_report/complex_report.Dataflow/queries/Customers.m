// Query: Customers
// Source: SQL Server
// Destination: Lakehouse → customers
// Generated: 2026-03-05T10:46:29.862695

let
    // Source SQL Server
    Source = Sql.Database("sql-finance.contoso.com", "FinanceDB"),
    #"Customers Table" = Source{[Schema="dbo", Item="Customers"]}[Data],
    Result = #"Customers Table"
in
    Result