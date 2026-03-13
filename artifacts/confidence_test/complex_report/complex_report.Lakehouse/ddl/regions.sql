-- Create table: regions
-- Source: Regions (SQL Server)
CREATE TABLE IF NOT EXISTS regions (
    RegionID INT,
    RegionName STRING,
    Country STRING,
    State STRING,
    City STRING,
    Latitude DOUBLE,
    Longitude DOUBLE
)
USING DELTA;
