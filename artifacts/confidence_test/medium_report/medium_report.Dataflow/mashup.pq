// Dataflow Gen2 Mashup Document
// Project: medium_report
// Generated: 2026-03-05T10:46:35.710243

section Section1;

// ── Orders ────────────────────────────────────────────────────────
// Source: PostgreSQL
// → Lakehouse.orders
shared Orders = let
    // Source PostgreSQL
    Source = PostgreSQL.Database("pg-analytics.contoso.com:5432", "ecommerce"),
    #"Orders Table" = Source{[Schema="public", Item="Orders"]}[Data],
    Result = #"Orders Table"
,
    CalcCol_revenue_tier = Table.AddColumn(Result, "Revenue Tier", each if [Sales] > 500 then "High" ELSEIF [Sales] > 100 then "Medium" else "Low")
in
    CalcCol_revenue_tier;

// ── Customers ─────────────────────────────────────────────────────
// Source: PostgreSQL
// → Lakehouse.customers
shared Customers = let
    // Source PostgreSQL
    Source = PostgreSQL.Database("pg-analytics.contoso.com:5432", "ecommerce"),
    #"Customers Table" = Source{[Schema="public", Item="Customers"]}[Data],
    Result = #"Customers Table"
in
    Result;

