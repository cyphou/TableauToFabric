// Dataflow Gen2 Mashup Document
// Project: complex_report
// Generated: 2026-03-05T10:46:29.872660

section Section1;

// ── Orders ────────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.orders
shared Orders = let
    // Source SQL Server
    Source = Sql.Database("sql-finance.contoso.com", "FinanceDB"),
    #"Orders Table" = Source{[Schema="dbo", Item="Orders"]}[Data],
    Result = #"Orders Table"
,
    CalcCol_profit = Table.AddColumn(Result, "Profit", each [Revenue] - [Cost]),
    CalcCol_priority_label = Table.AddColumn(CalcCol_profit, "Priority Label", each CASE [Priority] WHEN "Critical" then "🔴 Critical" WHEN "High" then "🟠 High" WHEN "Medium" then "🟡 Medium" WHEN "Low" then "🟢 Low" else "⚪ Unknown"),
    CalcCol_revenue_tier = Table.AddColumn(CalcCol_priority_label, "Revenue Tier", each if [Revenue] > 10000 then "Platinum" ELSEIF [Revenue] > 5000 then "Gold" ELSEIF [Revenue] > 1000 then "Silver" else "Bronze"),
    CalcCol_days_to_ship = Table.AddColumn(CalcCol_revenue_tier, "Days to Ship", each DATEDIFF('day', [OrderDate], [ShipDate])),
    CalcCol_has_discount = Table.AddColumn(CalcCol_days_to_ship, "Has Discount", each if ZN([Discount]) > 0 then "Yes" else "No")
in
    CalcCol_has_discount;

// ── Customers ─────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.customers
shared Customers = let
    // Source SQL Server
    Source = Sql.Database("sql-finance.contoso.com", "FinanceDB"),
    #"Customers Table" = Source{[Schema="dbo", Item="Customers"]}[Data],
    Result = #"Customers Table"
in
    Result;

// ── Products ──────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.products
shared Products = let
    // Source SQL Server
    Source = Sql.Database("sql-finance.contoso.com", "FinanceDB"),
    #"Products Table" = Source{[Schema="dbo", Item="Products"]}[Data],
    Result = #"Products Table"
in
    Result;

// ── Regions ───────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.regions
shared Regions = let
    // Source SQL Server
    Source = Sql.Database("sql-finance.contoso.com", "FinanceDB"),
    #"Regions Table" = Source{[Schema="dbo", Item="Regions"]}[Data],
    Result = #"Regions Table"
in
    Result;

// ── Targets ───────────────────────────────────────────────────────
// Source: Excel
// → Lakehouse.targets
shared Targets = let
    // Source Excel: \\\\fileserver\\data\\Budget_2024.xlsx
    Source = Excel.Workbook(File.Contents(DataFolder & "\\\\\fileserver\\data\\Budget_2024.xlsx"), null, true),
    #"Targets Sheet" = Source{[Item="Targets",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Targets Sheet", [PromoteAllScalars=true]),
    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"Category", type text},
        {"Region", type text},
        {"TargetRevenue", type number},
        {"TargetProfit", type number},
        {"FiscalYear", Int64.Type}
    }),
    Result = #"Changed Types"
in
    Result;

// ── MonthlySummary ────────────────────────────────────────────────
// Source: Custom SQL
// → Lakehouse.monthlysummary
shared MonthlySummary = let
    // Custom SQL Query
    Source = Sql.Database("localhost", "MyDB", [Query="SELECT
              YEAR(OrderDate)  AS OrderYear,
              MONTH(OrderDate) AS OrderMonth,
              Category,
              SUM(Revenue)     AS TotalRevenue,
              SUM(Cost)        AS TotalCost,
              COUNT(*)         AS OrderCount
          FROM Orders
          GROUP BY YEAR(OrderDate), MONTH(OrderDate), Category"]),
    Result = Source
in
    Result;

