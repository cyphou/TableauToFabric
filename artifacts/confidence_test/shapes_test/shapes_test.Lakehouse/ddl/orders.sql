-- Create table: orders
-- Source: Orders (Excel)
CREATE TABLE IF NOT EXISTS orders (
    Row_ID INT,
    Order_ID STRING,
    Order_Date DATE,
    Ship_Date DATE,
    Ship_Mode STRING,
    Customer_ID STRING,
    Customer_Name STRING,
    Segment STRING,
    Country_Region STRING,
    City STRING,
    State STRING,
    Postal_Code INT,
    Region STRING,
    Product_ID STRING,
    Category STRING,
    Sub_Category STRING,
    Product_Name STRING,
    Sales DOUBLE,
    Quantity INT,
    Discount DOUBLE,
    Profit DOUBLE,
    profit_bin INT  -- calc: [Profit]
)
USING DELTA;
