-- Create table: extract
-- Source: Extract (Excel)
CREATE TABLE IF NOT EXISTS extract (
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
    Ship_Date DATE
)
USING DELTA;
