-- Create table: orders
-- Source: Orders (PostgreSQL)
CREATE TABLE IF NOT EXISTS orders (
    OrderID INT,
    CustomerID INT,
    OrderDate DATE,
    Category STRING,
    Product STRING,
    Sales DOUBLE,
    Profit DOUBLE,
    Quantity INT,
    Discount DOUBLE,
    Region STRING,
    Country STRING,
    State STRING,
    City STRING,
    revenue_tier STRING  -- calc: IF [Sales] > 500 THEN "High" ELSEIF [Sales] > 100 THEN "Medium" ELSE "Low" END
)
USING DELTA;
