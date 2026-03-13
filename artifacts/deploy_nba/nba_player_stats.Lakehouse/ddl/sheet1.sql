-- Create table: sheet1
-- Source: Sheet1 (Excel)
CREATE TABLE IF NOT EXISTS sheet1 (
    Player STRING,
    YR INT,
    TM STRING,
    GP INT,
    MIN DOUBLE,
    FGM DOUBLE,
    FGA DOUBLE,
    FG DOUBLE,
    PT DOUBLE,
    FT DOUBLE,
    OFF DOUBLE,
    DEF DOUBLE,
    REB DOUBLE,
    ASST DOUBLE,
    STL DOUBLE,
    BLK DOUBLE,
    TO DOUBLE,
    PF DOUBLE,
    PTS DOUBLE
)
USING DELTA;
