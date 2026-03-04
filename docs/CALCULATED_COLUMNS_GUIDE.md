# Calculated Columns Materialisation Guide

How **calculated columns** from Tableau are materialised as physical Delta table columns in the Fabric Lakehouse, rather than DAX expressions in the Semantic Model.

---

## Why Materialise?

Microsoft Fabric's **DirectLake** mode requires that the Semantic Model reads data directly from Delta tables in the Lakehouse. Unlike Import mode (where DAX calculated columns are evaluated in-memory), DirectLake cannot evaluate row-level DAX expressions at query time.

Therefore, every Tableau calculated column must be **materialised** — computed during data ingestion and stored as a physical column in the Lakehouse Delta table.

| Mode | DAX Calculated Columns | Physical Columns |
|------|----------------------|-----------------|
| Import | ✅ Supported | ✅ Supported |
| DirectQuery | ❌ Not supported | ✅ Supported |
| **DirectLake** | **❌ Not supported** | **✅ Required** |

---

## Architecture Overview

Calculated columns flow through **four artifacts**, each handling a specific responsibility:

```
┌──────────────────────────────────────────────────────────────┐
│  1. LAKEHOUSE — DDL declares the column as physical          │
│     Revenue DOUBLE                                           │
│     Status_Label STRING                                      │
├──────────────────────────────────────────────────────────────┤
│  2. DATAFLOW GEN2 — Power Query M computes the value         │
│     Table.AddColumn(Source, "Revenue",                        │
│       each [Quantity] * [UnitPrice] * (1 - [Discount]))      │
├──────────────────────────────────────────────────────────────┤
│  3. NOTEBOOK — PySpark computes the value (alternative)      │
│     df = df.withColumn("Revenue",                            │
│       F.col("Quantity") * F.col("UnitPrice") *               │
│       (1 - F.col("Discount")))                               │
├──────────────────────────────────────────────────────────────┤
│  4. SEMANTIC MODEL — TMDL references the physical column     │
│     column Revenue                                           │
│       dataType: double                                       │
│       sourceColumn: Revenue     ← NOT expression = DAX(...)  │
└──────────────────────────────────────────────────────────────┘
```

---

## Classification: Measure vs Calculated Column

The migration tool automatically classifies each Tableau calculation as either a **measure** or a **calculated column** based on the Tableau `role` attribute:

| Tableau Role | Classification | Where Defined | How Referenced |
|-------------|---------------|---------------|----------------|
| `measure` | **DAX Measure** | Semantic Model (`expression = DAX(...)`) | Query-time evaluation |
| `dimension` | **Calculated Column** | Lakehouse (physical) + Dataflow/Notebook (computed) | `sourceColumn` in TMDL |

### Classification Logic (`calc_column_utils.py`)

```python
def classify_calculation(calc):
    """Classify a Tableau calculation as measure or calculated column."""
    role = calc.get('role', 'measure')
    if role == 'dimension':
        return 'calculated_column'
    return 'measure'
```

Key rules:
- **Aggregation functions** (`SUM`, `AVG`, `COUNT`, etc.) → always a **measure**
- **Row-level logic** (`IF`, string concat, `DATEDIFF`, etc.) with `role='dimension'` → **calculated column**
- **LOD expressions** (`{FIXED ...}`) → **measure** (evaluated at query time)
- **Table calculations** (`RANK`, `RUNNING_SUM`, etc.) → **measure**

---

## Materialisation in Each Artifact

### 1. Lakehouse (DDL)

The calculated column appears as a standard physical column in the Delta table DDL:

```sql
CREATE TABLE IF NOT EXISTS Orders (
    OrderID         BIGINT,
    CustomerID      BIGINT,
    OrderDate       DATE,
    Quantity         BIGINT,
    UnitPrice        DOUBLE,
    Discount         DOUBLE,
    -- Materialised calculated columns
    Revenue          DOUBLE,
    Status_Label     STRING
) USING DELTA;
```

**Type mapping** — the Tableau formula's result type maps to a Delta type:

| Formula Result | Delta Type |
|---------------|-----------|
| Numeric (`real`, `number`) | `DOUBLE` |
| Integer (`integer`) | `BIGINT` |
| Text (`string`) | `STRING` |
| Boolean (`boolean`) | `BOOLEAN` |
| Date (`date`) | `DATE` |
| DateTime (`datetime`) | `TIMESTAMP` |

### 2. Dataflow Gen2 (Power Query M)

A `Table.AddColumn()` step is injected into the M query after the type change step:

```
let
    Source = Snowflake.Databases("account.snowflake.com", "WH"),
    // ... source navigation and type changes ...
    #"Changed Types" = Table.TransformColumnTypes(Data, { ... }),

    // ── Calculated Column: Revenue ──
    #"Added Revenue" = Table.AddColumn(#"Changed Types", "Revenue",
        each [Quantity] * [UnitPrice] * (1 - [Discount]), type number),

    // ── Calculated Column: Status Label ──
    #"Added Status_Label" = Table.AddColumn(#"Added Revenue", "Status_Label",
        each if [Status] = "Active" then "Active" else "Inactive", type text)
in
    #"Added Status_Label"
```

**Formula conversion** — Tableau formulas are converted to M expressions:

| Tableau | Power Query M |
|---------|--------------|
| `[A] * [B]` | `[A] * [B]` |
| `IF cond THEN a ELSE b END` | `if cond then a else b` |
| `ISNULL([x])` | `[x] = null` |
| `[A] + " " + [B]` (string) | `[A] & " " & [B]` |
| `CONTAINS([x], "abc")` | `Text.Contains([x], "abc")` |
| `DATEDIFF('day', [d1], [d2])` | `Duration.Days([d2] - [d1])` |
| `DATEPART('year', [d])` | `Date.Year([d])` |

### 3. Notebook (PySpark)

A `.withColumn()` call is generated for each calculated column:

```python
from pyspark.sql import functions as F

df = spark.read.format("delta").load("Tables/Orders")

# Calculated Column: Revenue
df = df.withColumn("Revenue",
    F.col("Quantity") * F.col("UnitPrice") * (1 - F.col("Discount")))

# Calculated Column: Status Label
df = df.withColumn("Status_Label",
    F.when(F.col("Status") == "Active", F.lit("Active"))
     .otherwise(F.lit("Inactive")))

df.write.format("delta").mode("overwrite").saveAsTable("Orders")
```

**Formula conversion** — Tableau formulas are converted to PySpark:

| Tableau | PySpark |
|---------|---------|
| `[A] * [B]` | `F.col("A") * F.col("B")` |
| `IF cond THEN a ELSE b END` | `F.when(cond, a).otherwise(b)` |
| `ISNULL([x])` | `F.col("x").isNull()` |
| `[A] + " " + [B]` (string) | `F.concat(F.col("A"), F.lit(" "), F.col("B"))` |
| `CONTAINS([x], "abc")` | `F.col("x").contains("abc")` |
| Nested IF/ELSEIF | Chained `F.when(...).when(...).otherwise(...)` |

### 4. Semantic Model (TMDL)

The calculated column is declared with `sourceColumn` instead of `expression`:

```
table Orders

    // Regular physical column
    column OrderID
        dataType: int64
        sourceColumn: OrderID
        summarizeBy: none

    // Materialised calculated column
    column Revenue
        dataType: double
        sourceColumn: Revenue        // ← references physical Delta column
        summarizeBy: sum

    // Materialised calculated column
    column Status_Label
        dataType: string
        sourceColumn: Status_Label   // ← references physical Delta column
        summarizeBy: none

    // DAX measure (evaluated at query time — NOT materialised)
    measure TotalRevenue = SUM('Orders'[Revenue])
```

**Contrast with Import mode** (TableauToPowerBI project):
```
// Import mode — DAX calculated column (NOT used in DirectLake)
column Revenue
    dataType: double
    expression = [Quantity] * [UnitPrice] * (1 - [Discount])
    isCalculated
```

---

## Common Patterns

### Pattern 1: Arithmetic

```
Tableau:   [Quantity] * [UnitPrice] * (1 - [Discount])
M:         each [Quantity] * [UnitPrice] * (1 - [Discount])
PySpark:   F.col("Quantity") * F.col("UnitPrice") * (1 - F.col("Discount"))
Delta:     Revenue DOUBLE
TMDL:      sourceColumn: Revenue
```

### Pattern 2: Conditional (IF/ELSE)

```
Tableau:   IF [Revenue] > 10000 THEN "High" ELSEIF [Revenue] > 5000 THEN "Medium" ELSE "Low" END
M:         each if [Revenue] > 10000 then "High" else if [Revenue] > 5000 then "Medium" else "Low"
PySpark:   F.when(F.col("Revenue") > 10000, "High")
            .when(F.col("Revenue") > 5000, "Medium")
            .otherwise("Low")
Delta:     Revenue_Tier STRING
TMDL:      sourceColumn: Revenue_Tier
```

### Pattern 3: Null Handling

```
Tableau:   IFNULL([Discount], 0)
M:         each if [Discount] = null then 0 else [Discount]
PySpark:   F.coalesce(F.col("Discount"), F.lit(0))
Delta:     Discount_Safe DOUBLE
TMDL:      sourceColumn: Discount_Safe
```

### Pattern 4: String Concatenation

```
Tableau:   [First Name] + " " + [Last Name]
M:         each [First Name] & " " & [Last Name]
PySpark:   F.concat(F.col("First Name"), F.lit(" "), F.col("Last Name"))
Delta:     Full_Name STRING
TMDL:      sourceColumn: Full_Name
```

### Pattern 5: Date Extraction

```
Tableau:   DATEPART('year', [OrderDate])
M:         each Date.Year([OrderDate])
PySpark:   F.year(F.col("OrderDate"))
Delta:     Order_Year BIGINT
TMDL:      sourceColumn: Order_Year
```

### Pattern 6: Cross-Table Reference

When a calculated column references a column from another table, the cross-table reference is resolved at the Dataflow/Notebook level (via joins) rather than via DAX `RELATED()`:

```
Tableau:   [Segment]   ← from Customers table (referenced on Orders table)

Dataflow:  Join Orders with Customers on CustomerID, then add column
Notebook:  df_orders.join(df_customers, "CustomerID").withColumn("Segment", ...)
TMDL:      sourceColumn: Segment   (physical column after join)
```

---

## Testing

The calculated column materialisation is covered by dedicated tests:

```bash
# Run all calc column tests
python -m pytest tests/test_calc_column_utils.py -v

# Run specific artifact tests that include calc column scenarios
python -m pytest tests/test_lakehouse_generator.py -v
python -m pytest tests/test_dataflow_generator.py -v
python -m pytest tests/test_notebook_generator.py -v
```

### Test Coverage

| Test File | Calc Column Tests |
|-----------|------------------|
| `test_calc_column_utils.py` | 28 tests — classification, formula conversion (M + PySpark), type mapping |
| `test_lakehouse_generator.py` | DDL includes calc columns as physical columns |
| `test_dataflow_generator.py` | M queries include `Table.AddColumn()` steps |
| `test_notebook_generator.py` | PySpark includes `.withColumn()` calls |
| `test_semantic_model_generator.py` | TMDL uses `sourceColumn` (not `expression`) |

---

## Troubleshooting

| Issue | Cause | Resolution |
|-------|-------|-----------|
| Calc column has no values | Dataflow/Notebook not yet run | Execute the Pipeline to ingest data |
| Wrong data type | Type mismatch between DDL and M/PySpark | Verify Delta type matches formula output |
| Formula not converted | Unsupported Tableau function | Check [DAX reference](TABLEAU_TO_DAX_REFERENCE.md) status column; implement manually |
| `expression` in TMDL | Column incorrectly classified as measure | Check `role` attribute in Tableau extraction JSON |
| Cross-table calc column fails | Join not resolved at Dataflow level | Add explicit join in M query or Notebook |
