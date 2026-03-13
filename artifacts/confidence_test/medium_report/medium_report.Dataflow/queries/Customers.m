// Query: Customers
// Source: PostgreSQL
// Destination: Lakehouse → customers
// Generated: 2026-03-05T10:46:35.708677

let
    // Source PostgreSQL
    Source = PostgreSQL.Database("pg-analytics.contoso.com:5432", "ecommerce"),
    #"Customers Table" = Source{[Schema="public", Item="Customers"]}[Data],
    Result = #"Customers Table"
in
    Result