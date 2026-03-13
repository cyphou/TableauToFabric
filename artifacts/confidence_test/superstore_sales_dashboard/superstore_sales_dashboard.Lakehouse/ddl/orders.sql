-- Create table: orders
-- Source: Orders (Excel)
CREATE TABLE IF NOT EXISTS orders (
    Row_ID INT,
    Order_ID INT,
    Order_Date DATE,
    Order_Priority STRING,
    Order_Quantity INT,
    Sales DOUBLE,
    Discount DOUBLE,
    Ship_Mode STRING,
    Profit DOUBLE,
    Unit_Price DOUBLE,
    Shipping_Cost DOUBLE,
    Customer_Name STRING,
    City STRING,
    Zip_Code STRING,
    State STRING,
    Region STRING,
    Customer_Segment STRING,
    Product_Category STRING,
    Product_Sub_Category STRING,
    Product_Name STRING,
    Product_Container STRING,
    Product_Base_Margin DOUBLE,
    Ship_Date DATE,
    grouping_based_on_unit_price STRING  -- calc: IF [Unit Price] <= 2500 then 'A'
ELSEIF [Unit Price] <=5000 then 'B'
ELSE 'C' END
)
USING DELTA;
