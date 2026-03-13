-- Create table: extract
-- Source: Extract (DATAENGINE)
CREATE TABLE IF NOT EXISTS extract (
    Category STRING NOT NULL,
    Number_of_Records INT NOT NULL,
    Order_Date DATE NOT NULL,
    Sales_Target INT NOT NULL,
    Segment STRING NOT NULL
)
USING DELTA;
