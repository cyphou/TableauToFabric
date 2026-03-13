// Query: Products
// Source: SQL Server
// Destination: Lakehouse → products
// Generated: 2026-03-05T10:46:29.865441

let
    // Source SQL Server
    Source = Sql.Database("sql-finance.contoso.com", "FinanceDB"),
    #"Products Table" = Source{[Schema="dbo", Item="Products"]}[Data],
    Result = #"Products Table"
in
    Result