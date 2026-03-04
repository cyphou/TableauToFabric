# Tableau → Microsoft Fabric Mapping Reference

Complete mapping of Tableau objects to their Microsoft Fabric equivalents.
The migration tool generates **six artifact types** (Lakehouse, Dataflow Gen2, Notebook, Semantic Model, Pipeline, Power BI Report).

## Mapping Overview

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                            TABLEAU → FABRIC MAPPING                                  │
│                                                                                      │
│   TABLEAU OBJECTS                    FABRIC ARTIFACTS                                │
│   ───────────────                    ────────────────                                 │
│                                                                                      │
│   Data Sources ──────────────────>   Lakehouse (DDL) + Dataflow Gen2 (M queries)     │
│   Custom SQL ────────────────────>   Dataflow Gen2 (native query) + Notebook cells   │
│   Calculated columns (dim) ──────>   Lakehouse DDL + Dataflow M + Notebook PySpark   │
│   Calculated fields (measure) ───>   Semantic Model DAX measures                     │
│   LOD expressions ───────────────>   Semantic Model DAX CALCULATE(...)               │
│   Parameters ────────────────────>   Semantic Model What-If tables                   │
│   Worksheets ────────────────────>   Power BI Report visuals (60+ types)             │
│   Dashboards ────────────────────>   Power BI Report pages                           │
│   Stories ───────────────────────>   Power BI Report pages + bookmarks               │
│   Filters ───────────────────────>   Slicers + visual / report filters               │
│   User filters (RLS) ───────────>   Semantic Model RLS roles (USERPRINCIPALNAME)     │
│   Actions ───────────────────────>   Cross-filtering / buttons (approximate)         │
│   Relationships ─────────────────>   Semantic Model TMDL relationships               │
│   Sort orders ───────────────────>   Semantic Model sortByColumn                     │
│   Hierarchies ───────────────────>   Semantic Model display folders                  │
│   Sets ──────────────────────────>   Semantic Model calculated tables / filters      │
│   Groups ────────────────────────>   Semantic Model calculated columns               │
│   Bins ──────────────────────────>   Semantic Model calculated columns (bucket)      │
│   Aliases ───────────────────────>   Semantic Model column descriptions              │
│                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

> **Legend**  
> ✅ Automatic — fully converted by the migration tool  
> ⚠️ Approximate — converted with best-effort approximation  
> 🔧 Manual — placeholder generated, manual review needed  
> ❌ No equivalent — no Fabric counterpart exists

---

## 1. Visual Type Mappings (60+)

Tableau mark types → Power BI visual types in the generated `.pbip` report.

### Bar & Column Charts

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Bar chart (horizontal) | `clusteredBarChart` | ✅ |
| Side-by-side bar | `clusteredBarChart` | ✅ |
| Stacked bar | `100% Stacked Bar Chart` | ✅ |
| Bar chart (vertical / Column) | `clusteredColumnChart` | ✅ |
| Side-by-side column | `clusteredColumnChart` | ✅ |
| Stacked column | `stackedColumnChart` | ✅ |
| Histogram | `clusteredColumnChart` (binned) | ✅ |

### Line & Area Charts

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Line chart | `lineChart` | ✅ |
| Dual-axis line | `lineChart` (multi-measure) | ✅ |
| Area chart | `areaChart` | ✅ |
| Stacked area | `stackedAreaChart` | ✅ |
| Sparkline | `lineChart` (small multiple) | ⚠️ |

### Pie & Donut

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Pie chart | `pieChart` | ✅ |
| Donut chart | `donutChart` | ✅ |

### Scatter & Bubble

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Scatter plot | `scatterChart` | ✅ |
| Bubble chart | `scatterChart` (size encoding) | ✅ |

### Tables & Matrices

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Text table (crosstab) | `tableEx` | ✅ |
| Highlight table | `tableEx` (conditional format) | ⚠️ |
| Pivot table | `pivotTable` | ✅ |
| Matrix | `pivotTable` | ✅ |

### Maps

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Symbol map | `map` | ✅ |
| Filled map | `filledMap` | ✅ |
| Density map (heatmap) | `map` | ⚠️ |
| Dual-axis map | `map` (layered) | ⚠️ |
| Flow map | `map` | ⚠️ |

### Specialised

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Treemap | `treemap` | ✅ |
| Heat map | `tableEx` (conditional format) | ⚠️ |
| Box-and-whisker | Custom visual | 🔧 |
| Bullet graph | Custom visual | 🔧 |
| Gantt chart | Custom visual | 🔧 |
| Waterfall chart | `waterfallChart` | ✅ |
| Funnel chart | `funnel` | ✅ |
| Word cloud | Custom visual | 🔧 |
| Gauge / Speedometer | `gauge` | ✅ |
| KPI | `card` / `multiRowCard` | ✅ |
| Reference line (on chart) | Visual analytics pane | ⚠️ |
| Combo chart (bar + line) | `lineClusteredColumnComboChart` | ✅ |
| Packed bubble | `scatterChart` (size) | ⚠️ |
| Radial chart | Custom visual | 🔧 |
| Lollipop chart | Custom visual | 🔧 |
| Sankey diagram | Custom visual | 🔧 |
| Chord diagram | Custom visual | 🔧 |

### Data Entry & Interaction

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Dropdown filter | `slicer` (dropdown) | ✅ |
| Single-value slider | `slicer` (between) | ✅ |
| Range slider | `slicer` (between) | ✅ |
| Multi-select list | `slicer` (list) | ✅ |
| Relative date filter | `slicer` (relative date) | ✅ |
| Hierarchy filter (drill) | `slicer` with hierarchy | ✅ |
| Parameter control | `slicer` on What-If table | ✅ |

---

## 2. Calculation Functions

Tableau formulas → DAX (measures and calculated columns in the Semantic Model).

> Full 172-function reference: [TABLEAU_TO_DAX_REFERENCE.md](TABLEAU_TO_DAX_REFERENCE.md)

### Aggregation

| Tableau | DAX | Status |
|---------|-----|--------|
| `SUM(expr)` | `SUM(col)` / `SUMX('T', expr)` | ✅ |
| `AVG(expr)` | `AVERAGE(col)` / `AVERAGEX('T', expr)` | ✅ |
| `COUNT(expr)` | `COUNT(col)` / `COUNTX('T', expr)` | ✅ |
| `COUNTD(expr)` | `DISTINCTCOUNT(col)` | ✅ |
| `MIN(expr)` | `MIN(col)` / `MINX('T', expr)` | ✅ |
| `MAX(expr)` | `MAX(col)` / `MAXX('T', expr)` | ✅ |
| `MEDIAN(expr)` | `MEDIAN(col)` / `MEDIANX('T', expr)` | ✅ |

### Logical

| Tableau | DAX | Status |
|---------|-----|--------|
| `IF ... THEN ... ELSE ... END` | `IF(cond, then, else)` | ✅ |
| `CASE ... WHEN ... END` | `SWITCH(field, val, result, ...)` | ✅ |
| `ISNULL(expr)` | `ISBLANK(expr)` | ✅ |
| `IFNULL(expr, alt)` | `IF(ISBLANK(expr), alt, expr)` | ✅ |
| `ZN(expr)` | `IF(ISBLANK(expr), 0, expr)` | ✅ |

### Text

| Tableau | DAX | Status |
|---------|-----|--------|
| `CONTAINS(str, sub)` | `CONTAINSSTRING(str, sub)` | ✅ |
| `REPLACE(str, old, new)` | `SUBSTITUTE(str, old, new)` | ✅ |
| `LEFT / RIGHT / MID` | `LEFT / RIGHT / MID` | ✅ |
| `UPPER / LOWER / TRIM` | `UPPER / LOWER / TRIM` | ✅ |

### Date

| Tableau | DAX | Status |
|---------|-----|--------|
| `DATEPART('year', date)` | `YEAR(date)` | ✅ |
| `DATEDIFF(part, start, end)` | `DATEDIFF(start, end, INTERVAL)` | ✅ |
| `DATETRUNC('month', date)` | `STARTOFMONTH(date)` | ✅ |
| `NOW()` / `TODAY()` | `NOW()` / `TODAY()` | ✅ |

---

## 3. LOD Expressions

| Tableau LOD | DAX | Status |
|-------------|-----|--------|
| `{FIXED [dim] : AGG(expr)}` | `CALCULATE(AGG(expr), ALLEXCEPT('T', 'T'[dim]))` | ✅ |
| `{INCLUDE [dim] : AGG(expr)}` | `CALCULATE(AGG(expr))` | ✅ |
| `{EXCLUDE [dim] : AGG(expr)}` | `CALCULATE(AGG(expr), REMOVEFILTERS('T'[dim]))` | ✅ |
| `{AGG(expr)}` (no dims) | `CALCULATE(AGG(expr))` | ✅ |

---

## 4. Parameters

| Tableau Parameter | Fabric Semantic Model | Status |
|-------------------|----------------------|--------|
| Integer range (min, max, step) | `GENERATESERIES(min, max, step)` table + `SELECTEDVALUE` measure | ✅ |
| Real range (min, max, step) | `GENERATESERIES(min, max, step)` table + `SELECTEDVALUE` measure | ✅ |
| String list | `DATATABLE("Value", STRING, {{"v1"}, ...})` table + `SELECTEDVALUE` measure | ✅ |
| Date range | `GENERATESERIES(...)` table | ⚠️ |
| Any domain (type-in) | `DATATABLE` with current value | ⚠️ |

---

## 5. Filters

| Tableau Filter Type | Fabric Equivalent | Status |
|--------------------|-------------------|--------|
| Dimension filter (include/exclude) | Slicer visual | ✅ |
| Measure filter | Visual-level filter | ✅ |
| Relative date filter | Relative date slicer | ✅ |
| Top N filter | Visual-level TopN filter | ✅ |
| Context filter | Report-level filter | ⚠️ |
| Data source filter | M `Table.SelectRows()` in Dataflow / notebook filter | ⚠️ |
| Extract filter | M filter / notebook filter | ⚠️ |

---

## 6. Actions & Interactions

| Tableau Action | Fabric (Power BI) | Status |
|---------------|-------------------|--------|
| Filter action | Cross-filtering (visual interactions) | ⚠️ |
| Highlight action | Cross-highlighting (visual interactions) | ⚠️ |
| URL action | Button / hyperlink | ⚠️ |
| Sheet navigation | Bookmark / page navigation button | ⚠️ |
| Parameter action | Slicer interaction | ⚠️ |
| Set action | Slicer / filter reset | 🔧 |

---

## 7. Stories & Bookmarks

| Tableau | Fabric (Power BI) | Status |
|---------|-------------------|--------|
| Story | Multiple report pages | ✅ |
| Story point | Bookmark | ✅ |
| Story caption | Bookmark display name | ✅ |
| Story description | Bookmark description | ⚠️ |

---

## 8. Data Sources → Fabric Artifacts

### Connector Mapping (25 types)

| Tableau Connector | Fabric Dataflow Gen2 (Power Query M) | Status |
|-------------------|--------------------------------------|--------|
| Excel (.xlsx/.xls) | `Excel.Workbook(File.Contents())` | ✅ |
| CSV / Text | `Csv.Document(File.Contents())` | ✅ |
| SQL Server | `Sql.Database(server, database)` | ✅ |
| PostgreSQL | `PostgreSQL.Database(server:port, db)` | ✅ |
| MySQL | `MySQL.Database(server:port, db)` | ✅ |
| Oracle | `Oracle.Database(server:port/service)` | ✅ |
| Google BigQuery | `GoogleBigQuery.Database()` | ✅ |
| Snowflake | `Snowflake.Databases(server, warehouse)` | ✅ |
| Teradata | `Teradata.Database(server)` | ✅ |
| SAP HANA | `SapHana.Database(server:port)` | ✅ |
| SAP BW | `SapBusinessWarehouse.Cubes(server, sysNr, clientId)` | ✅ |
| Amazon Redshift | `AmazonRedshift.Database(server:port, db)` | ✅ |
| Databricks | `Databricks.Catalogs(server, http_path)` | ✅ |
| Spark SQL | `SparkSql.Database(server, port)` | ✅ |
| Azure SQL Database | `AzureSQL.Database(server, database)` | ✅ |
| Azure Synapse | `AzureSQL.Database(server, database)` | ✅ |
| Google Sheets | `Web.Contents() + Csv.Document()` | ✅ |
| SharePoint | `SharePoint.Files(site_url)` | ✅ |
| JSON | `Json.Document(File.Contents())` | ✅ |
| XML | `Xml.Tables(File.Contents())` | ✅ |
| PDF | `Pdf.Tables(File.Contents())` | ✅ |
| GeoJSON | `Json.Document(File.Contents())` | ✅ |
| Salesforce | `Salesforce.Data()` | ✅ |
| Web API | `Web.Contents(url) + Json.Document()` | ✅ |
| Custom SQL | `Sql.Database(server, db, [Query=...])` | ✅ |

### Data Flow: Tableau → Lakehouse

```
               ┌────────────────────────┐
               │   Tableau Data Source   │
               │   (25 connector types)  │
               └───────────┬────────────┘
                           │
                           ▼
          ┌────────────────────────────────┐
          │   Dataflow Gen2                │
          │   Power Query M                │
          │   ┌────────────────────────┐   │
          │   │ Source connector       │   │
          │   │ Type changes           │   │
          │   │ Table.AddColumn(calc)  │   │
          │   └────────────────────────┘   │
          └───────────────┬────────────────┘
                          │  writes Delta
                          ▼
          ┌────────────────────────────────┐
          │   Lakehouse                    │
          │   ┌──────┐ ┌──────┐ ┌──────┐  │
          │   │Orders│ │Cust. │ │Cal.  │  │
          │   │Delta │ │Delta │ │Delta │  │
          │   └──────┘ └──────┘ └──────┘  │
          └───────────────┬────────────────┘
                          │  PySpark ETL
                          ▼
          ┌────────────────────────────────┐
          │   Notebook                     │
          │   .withColumn(calc columns)    │
          │   .write.saveAsTable(Delta)    │
          └───────────────┬────────────────┘
                          │  DirectLake read
                          ▼
          ┌────────────────────────────────┐
          │   Semantic Model (TMDL)        │
          │   DirectLake entity partitions │
          │   DAX measures + RLS roles     │
          └───────────────┬────────────────┘
                          │  live connection
                          ▼
          ┌────────────────────────────────┐
          │   Power BI Report (.pbip)      │
          │   PBIR visuals + slicers       │
          │   60+ visual types             │
          └────────────────────────────────┘
```

---

## 9. Data Type Mappings

### Tableau → Lakehouse (Delta) Types

| Tableau Type | Delta Lake Type | Status |
|-------------|----------------|--------|
| `string` | `STRING` | ✅ |
| `integer` | `BIGINT` | ✅ |
| `int64` | `BIGINT` | ✅ |
| `real` | `DOUBLE` | ✅ |
| `double` | `DOUBLE` | ✅ |
| `decimal` | `DOUBLE` | ✅ |
| `number` | `DOUBLE` | ✅ |
| `boolean` | `BOOLEAN` | ✅ |
| `date` | `DATE` | ✅ |
| `datetime` | `TIMESTAMP` | ✅ |
| `time` | `STRING` | ⚠️ |
| `spatial` | `STRING` | ⚠️ |
| `binary` | `BINARY` | ✅ |

### Tableau → Power Query M Types (Dataflow Gen2)

| Tableau Type | Power Query M Type | Status |
|-------------|-------------------|--------|
| `string` | `type text` | ✅ |
| `integer` | `Int64.Type` | ✅ |
| `real` | `type number` | ✅ |
| `boolean` | `type logical` | ✅ |
| `date` | `type date` | ✅ |
| `datetime` | `type datetime` | ✅ |

### Tableau → TMDL Types (Semantic Model)

| Tableau Type | TMDL Type | Status |
|-------------|-----------|--------|
| `string` | `String` | ✅ |
| `integer` | `Int64` | ✅ |
| `real` | `Double` | ✅ |
| `boolean` | `Boolean` | ✅ |
| `date` | `DateTime` | ✅ |
| `datetime` | `DateTime` | ✅ |

---

## 10. Formatting

### Number Formats

| Tableau Format | DAX formatString | Status |
|---------------|-----------------|--------|
| Number (2 decimals) | `#,##0.00` | ✅ |
| Percentage | `0.0%` | ✅ |
| Currency | `$#,##0.00` | ✅ |
| Integer | `#,##0` | ✅ |
| Custom | Mapped as-is | ⚠️ |

### Date Formats

| Tableau Format | DAX formatString | Status |
|---------------|-----------------|--------|
| `YYYY-MM-DD` | `yyyy-MM-dd` | ✅ |
| `MM/DD/YYYY` | `MM/dd/yyyy` | ✅ |
| `Month Day, Year` | `MMMM dd, yyyy` | ✅ |
| Short date | Locale default | ✅ |

---

## 11. Semantic Roles

| Tableau semantic-role | TMDL dataCategory | Status |
|----------------------|-------------------|--------|
| `[Country].[Name]` | `Country` | ✅ |
| `[State].[Name]` | `StateOrProvince` | ✅ |
| `[County].[Name]` | `County` | ✅ |
| `[City].[Name]` | `City` | ✅ |
| `[Postal Code].[Name]` | `PostalCode` | ✅ |
| `[Latitude]` | `Latitude` | ✅ |
| `[Longitude]` | `Longitude` | ✅ |

---

## 12. Complex Transformation Examples

### Example 1: SUM(IF) → SUMX

```
Tableau:
  SUM(IF [order_status] != "Cancelled"
      THEN [quantity] * [unit_price] * (1 - [discount])
      ELSE 0 END)

DAX (Semantic Model measure):
  SUMX('Orders',
    IF('Orders'[order_status] != "Cancelled",
       'Orders'[quantity] * 'Orders'[unit_price] * (1 - 'Orders'[discount]),
       0))
```

### Example 2: LOD FIXED → CALCULATE

```
Tableau:
  {FIXED [customer_id] : SUM([quantity] * [unit_price])}

DAX:
  CALCULATE(
    SUM('Orders'[quantity] * 'Orders'[unit_price]),
    ALLEXCEPT('Orders', 'Orders'[customer_id]))
```

### Example 3: Nested IF/ELSEIF

```
Tableau:
  IF [Revenue] > 10000 THEN "Platinum"
  ELSEIF [Revenue] > 5000 THEN "Gold"
  ELSEIF [Revenue] > 1000 THEN "Silver"
  ELSE "Bronze" END

DAX:
  IF([Revenue] > 10000, "Platinum",
    IF([Revenue] > 5000, "Gold",
      IF([Revenue] > 1000, "Silver", "Bronze")))
```

### Example 4: Null Handling

```
Tableau:
  IFNULL([discount], 0) * [price]

DAX:
  IF(ISBLANK([discount]), 0, [discount]) * [price]
```

### Example 5: String Concatenation

```
Tableau:
  [First Name] + " " + [Last Name]

DAX:
  [First Name] & " " & [Last Name]
```

### Example 6: DATEDIFF

```
Tableau:
  DATEDIFF('day', [order_date], [ship_date])

DAX:
  DATEDIFF([order_date], [ship_date], DAY)
```

### Example 7: Window Functions

```
Tableau:
  WINDOW_AVG(SUM([revenue]))

DAX:
  CALCULATE(SUM('Table'[revenue]), ALL('Table'))
```

### Example 8: Cross-Table Reference

```
Tableau calc column (on Orders table):
  [segment]     ← from Customers table

DAX (manyToOne):
  RELATED('Customers'[segment])

DAX (manyToMany):
  LOOKUPVALUE('Customers'[segment], 'Customers'[id], [customer_id])
```

### Example 9: Row-Level Security

```
Tableau user filter:
  user@company.com → [Region] IN {"West", "East"}

TMDL RLS role:
  role Territory_Access
    tablePermission 'Orders'
      filterExpression = USERPRINCIPALNAME() = "user@company.com" && [Region] IN {"West", "East"}
```

### Example 10: Parameter → What-If Table

```
Tableau parameter:
  "Top N" — Integer range, min=5, max=50, step=5

TMDL:
  table 'Top N'
    partition 'Top N' = calculated
      expression = GENERATESERIES(5, 50, 5)

  measure 'Top N Value' = SELECTEDVALUE('Top N'[Value], 10)
```

### Example 11: Calculated Column Materialisation

```
Tableau calculated column:
  IF [Revenue] > 10000 THEN "High" ELSE "Low" END

Lakehouse DDL:
  Revenue_Tier STRING

Dataflow Gen2 (M):
  Table.AddColumn(Source, "Revenue_Tier",
    each if [Revenue] > 10000 then "High" else "Low")

Notebook (PySpark):
  df = df.withColumn("Revenue_Tier",
    F.when(F.col("Revenue") > 10000, "High").otherwise("Low"))

TMDL:
  column Revenue_Tier
      dataType: string
      sourceColumn: Revenue_Tier
```

### Example 12: Full Pipeline Flow

```
Tableau:
  Data source (Snowflake) → 3 tables joined → 5 calculated columns → Dashboard

Fabric artifacts generated:
  1. Lakehouse     — 3 Delta tables + 5 materialised calc columns
  2. Dataflow Gen2 — 3 M queries (Snowflake connector) + calc column steps
  3. Notebook      — PySpark cells for each table + calc column transforms
  4. SemanticModel — DirectLake TMDL with entity partitions
  5. Pipeline      — Dataflow refresh → Notebook run → Model refresh
  6. PBI Report    — .pbip with PBIR visuals + DAX measures
```
