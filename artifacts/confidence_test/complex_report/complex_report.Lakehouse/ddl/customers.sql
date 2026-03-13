-- Create table: customers
-- Source: Customers (SQL Server)
CREATE TABLE IF NOT EXISTS customers (
    CustomerID INT,
    CustomerName STRING,
    Segment STRING,
    Email STRING,
    JoinDate DATE
)
USING DELTA;
