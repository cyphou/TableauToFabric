-- Create table: targets
-- Source: Targets (Excel)
CREATE TABLE IF NOT EXISTS targets (
    Category STRING,
    Region STRING,
    TargetRevenue DOUBLE,
    TargetProfit DOUBLE,
    FiscalYear INT
)
USING DELTA;
