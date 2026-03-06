# Known Limitations

This document lists known limitations and approximations in the Tableau to Microsoft Fabric migration tool.

---

## Extraction Limitations

| Area | Limitation | Impact |
|------|-----------|--------|
| **Hyper files** | `.hyper` extract data is not parsed — only XML metadata is read | Tables from Hyper extracts will have structure but no inline data |
| **Tableau Server/Cloud** | Live connections to Tableau Server are not reconnected | Connection strings reference the original server; must be reconfigured |
| **Tableau 2024.3+** | Dynamic zone visibility extracted and mapped to bookmarks (v3.7.0); some newer container features may still be ignored | Most 2024.3 features now handled |
| **Custom shapes** | Shape encoding extracts the field reference only — actual image files are not migrated | Custom shape visuals will show default markers |
| **OAuth credentials** | Credential metadata is stripped by design | Data source connections need re-authentication |
| **Nested layout containers** | Deeply nested containers may lose relative positioning | Some dashboard layouts may need manual adjustment |
| **Rich tooltips** | Styled tooltip text runs are now converted to PBI tooltip pages with formatted textbox visuals (v3.7.0) | Most formatting preserved |
| **Multiple data sources** | All calculations are placed on the "main" table | Multi-datasource worksheets may lose datasource context |

## Generation Limitations

| Area | Limitation | Impact |
|------|-----------|--------|
| **No incremental migration** | Re-running regenerates everything from scratch | Manual edits to the project are overwritten |
| **DirectLake mode** | Semantic model uses DirectLake with entity partitions referencing a Lakehouse | Import/DirectQuery tables cannot coexist with DirectLake partitions |
| **No paginated reports** | Only interactive .pbip reports are generated | Paginated report layouts are not supported |
| **Data bars** | Now generated for table/matrix visuals with quantitative color encoding (v3.7.0) | |
| **Small Multiples** | Now auto-generated when `small_multiples` or `pages_shelf` field is present (v3.7.0) | Grid layouts configured automatically |
| **Visual positioning** | Dashboard objects are scaled proportionally from Tableau canvas to PBI page size | Not pixel-perfect; overlapping is possible |
| **Textbox/Image** | Rich text with bold/italic/color/font_size/url now preserved via PBI text runs (v3.7.0) | Most formatting migrated |

## DAX Conversion Limitations

### Functions with No DAX Equivalent

| Tableau Function | Output | Reason |
|-----------------|--------|--------|
| MAKEPOINT, MAKELINE, DISTANCE, BUFFER, AREA, INTERSECTION | `0` + comment | No spatial functions in DAX |
| HEXBINX, HEXBINY | `0` + comment | No hex-binning in DAX |
| COLLECT | `0` + comment | No spatial collection |
| SCRIPT_BOOL/INT/REAL/STR | `BLANK()` + comment | R/Python scripting has no DAX equivalent |
| SPLIT | `BLANK()` + comment | No string split to array in DAX |
| `PREVIOUS_VALUE` | `OFFSET(-1, ...)` | ✅ Converted to DAX OFFSET function (v3.7.0) |

### Approximated Functions

| Tableau Function | DAX Output | Accuracy |
|-----------------|------------|----------|
| REGEXP_MATCH | `CONTAINSSTRING()` | Substring only, not true regex |
| REGEXP_REPLACE | `SUBSTITUTE()` | Literal replacement, no regex groups |
| REGEXP_EXTRACT | `BLANK()` | Placeholder |
| RANK_PERCENTILE | `DIVIDE(RANKX()-1, COUNTROWS()-1)` | Edge cases with ties |
| RUNNING_SUM/AVG/COUNT | `CALCULATE(AGG, ...)` | No window frame specification |
| WINDOW_SUM/AVG/MAX/MIN | `CALCULATE(inner, ALL/ALLEXCEPT)` | Loses window frame boundaries |
| LTRIM/RTRIM | `TRIM()` | Removes all leading/trailing spaces |
| `LOOKUP` | `OFFSET(n, ...)` | ✅ Converted to DAX OFFSET with signed offset (v3.7.0) |
| String `+` → `&` | Only at depth 0 | Arithmetic `+` in string contexts may be preserved |

## Visual Mapping Approximations

| Tableau Visual | Fabric PBI Mapping | Gap |
|---------------|------------|-----|
| Sankey / Chord / Network | decompositionTree | Structurally different — hierarchical vs flow |
| Gantt Bar / Lollipop | clusteredBarChart | Loses time-axis semantics |
| Butterfly / Waffle | hundredPercentStackedBarChart | Loses symmetry |
| Calendar Heat Map | matrix | Lacks calendar grid structure |
| Packed Bubble / Strip Plot | scatterChart | Size encoding may not transfer |
| Bump Chart / Slope / Sparkline | lineChart | Ranking semantics lost |
| Motion chart (animated) | Not handled | No PBI play-axis animation |
| Violin plot | Not handled | No standard PBI visual |
| Parallel coordinates | Not handled | No standard PBI visual |

## Power Query M Limitations

| Area | Limitation |
|------|-----------|
| **OAuth/SSO** | M queries use hardcoded connection strings; no OAuth configuration |
| **Data gateway** | No on-premises data gateway configuration generated |
| **Incremental refresh** | Auto-generated for tables with date/datetime columns (v3.7.0) — 30-day rolling + 1-day incremental |
| **Parameterized sources** | M parameters (ServerName/DatabaseName) now emitted for 12 connector types (v3.7.0) |
| **Hyper data** | `.hyper` files referenced in Prep flows produce empty `#table` |
| **Custom SQL params** | `Value.NativeQuery()` generated but parameter binding not supported |

## Fabric-Specific Limitations

| Area | Limitation |
|------|-----------|
| **DirectLake** | Calendar table uses import partition; cannot coexist with DirectLake entity partitions |
| **Lakehouse** | Generated lakehouse definition requires manual deployment to workspace |
| **Pipeline** | Generated pipeline is a template; connections must be configured post-deployment |
| **Dataflow Gen2** | M queries may need gateway configuration for on-premises sources |
| **Notebook** | PySpark notebook assumes Lakehouse is mounted; requires workspace setup |

## Deployment Limitations

| Area | Limitation |
|------|-----------|
| **No rollback** | If deployment fails mid-batch, there's no automatic undo |
| **No integration tests** | Deployment code is structurally tested but never against a real Fabric workspace |
| **PBIR schema** | Generated JSON isn't validated against Microsoft's published JSON schemas |
| **Windows paths** | OneDrive file locks may leave stale artifacts (handled via try/except) |

## Workarounds

For most limitations, the recommended workflow is:

1. Run the migration to generate the Fabric artifacts
2. Import the Lakehouse definition and configure data connections
3. Run the Dataflow Gen2 or PySpark Notebook for data ingestion
4. Deploy the Semantic Model to the workspace
5. Open the .pbip file in Power BI Desktop (Developer Mode)
6. Review the migration metadata JSON for conversion notes
7. Manually adjust unsupported features (spatial, custom shapes, advanced formatting)
8. Re-authenticate data source connections
9. Validate measures and relationships in Model view
