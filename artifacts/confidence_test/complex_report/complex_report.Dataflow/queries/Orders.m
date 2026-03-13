// Query: Orders
// Source: SQL Server
// Destination: Lakehouse → orders
// Generated: 2026-03-05T10:46:29.860347

let
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
    CalcCol_has_discount