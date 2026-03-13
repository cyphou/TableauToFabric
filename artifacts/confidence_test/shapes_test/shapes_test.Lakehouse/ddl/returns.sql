-- Create table: returns
-- Source: Returns (Excel)
CREATE TABLE IF NOT EXISTS returns (
    Returned STRING,
    Order_ID STRING
)
USING DELTA;
