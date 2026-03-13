-- Create table: sales
-- Source: Sales (SQL Server)
CREATE TABLE IF NOT EXISTS sales (
    SaleID INT,
    Category STRING,
    Product STRING,
    Amount DOUBLE,
    Quantity INT,
    SaleDate DATE
)
USING DELTA;
