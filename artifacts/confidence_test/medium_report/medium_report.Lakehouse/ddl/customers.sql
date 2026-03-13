-- Create table: customers
-- Source: Customers (PostgreSQL)
CREATE TABLE IF NOT EXISTS customers (
    CustomerID INT,
    CustomerName STRING,
    Segment STRING,
    Email STRING
)
USING DELTA;
