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
    City STRING,
    State STRING,
    Country STRING,
    Postal_Code STRING,
    Market STRING,
    Region STRING,
    Product_ID STRING,
    Category STRING,
    Sub_Category STRING,
    Product_Name STRING,
    Sales DOUBLE,
    Quantity INT,
    Discount DOUBLE,
    Profit DOUBLE,
    Shipping_Cost DOUBLE,
    Order_Priority STRING,
    discount_bin INT  -- calc: [Discount]
)
USING DELTA;
