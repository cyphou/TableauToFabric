-- Create table: orders
-- Source: Orders (SQL Server)
CREATE TABLE IF NOT EXISTS orders (
    OrderID INT,
    CustomerID INT,
    ProductID INT,
    RegionID INT,
    OrderDate DATE,
    ShipDate DATE,
    Revenue DOUBLE,
    Cost DOUBLE,
    Quantity INT,
    Discount DOUBLE,
    OrderStatus STRING,
    ShipMode STRING,
    Priority STRING,
    profit DOUBLE  -- calc: [Revenue] - [Cost],
    priority_label STRING  -- calc: CASE [Priority] WHEN "Critical" THEN "🔴 Critical" WHEN "High" THEN "🟠 High" WHEN "Medium" THEN "🟡 Medium" WHEN "Low" THEN "🟢 Low" ELSE "⚪ Unknown" END,
    revenue_tier STRING  -- calc: IF [Revenue] > 10000 THEN "Platinum" ELSEIF [Revenue] > 5000 THEN "Gold" ELSEIF [Revenue] > 1000 THEN "Silver" ELSE "Bronze" END,
    days_to_ship INT  -- calc: DATEDIFF('day', [OrderDate], [ShipDate]),
    has_discount STRING  -- calc: IF ZN([Discount]) > 0 THEN "Yes" ELSE "No" END
)
USING DELTA;
