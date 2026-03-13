// Query: MonthlySummary
// Source: Custom SQL
// Destination: Lakehouse → monthlysummary
// Generated: 2026-03-05T10:46:29.870875

let
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
    Result