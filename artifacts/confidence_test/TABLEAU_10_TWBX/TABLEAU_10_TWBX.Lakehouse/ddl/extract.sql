-- Create table: extract
-- Source: Extract (SQL Server)
CREATE TABLE IF NOT EXISTS extract (
    Number_of_Records INT NOT NULL,
    a STRING NOT NULL,
    x INT NOT NULL,
    y INT NOT NULL
)
USING DELTA;
