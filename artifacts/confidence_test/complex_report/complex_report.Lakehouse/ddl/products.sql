-- Create table: products
-- Source: Products (SQL Server)
CREATE TABLE IF NOT EXISTS products (
    ProductID INT,
    ProductName STRING,
    Category STRING,
    SubCategory STRING,
    UnitPrice DOUBLE
)
USING DELTA;
