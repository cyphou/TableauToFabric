# Post-Migration Checklist

Use this checklist after running the migration tool to validate the generated Fabric artifacts.

---

## 1. Verify Generated Artifacts

- [ ] Check the output directory for all 6 artifact types
- [ ] Confirm `migration_metadata.json` is present and accurate
- [ ] Review any warnings in the migration log

## 2. Lakehouse Setup

- [ ] Import the Lakehouse definition into your Fabric workspace
- [ ] Verify all expected tables are listed
- [ ] Confirm Delta table schemas match the Tableau data sources

## 3. Data Ingestion

- [ ] **Dataflow Gen2**: Import and configure the dataflow
  - [ ] Re-enter credentials for each data source (OAuth, SQL auth, etc.)
  - [ ] Refresh the dataflow to confirm connectivity
  - [ ] For BigQuery: verify the billing project ID is correct
  - [ ] For Oracle: verify the TNS/Easy Connect string format
  - [ ] For on-premises sources: configure the data gateway
- [ ] **PySpark Notebook**: Upload and verify the notebook
  - [ ] Attach the notebook to the target Lakehouse
  - [ ] Run cells to verify data ingestion

## 4. Data Pipeline

- [ ] Import the pipeline definition
- [ ] Configure activity connections (Dataflow/Notebook references)
- [ ] Run a test pipeline execution
- [ ] Set up scheduled triggers if needed

## 5. Semantic Model (Model View)

- [ ] Deploy the Semantic Model to the workspace
- [ ] Switch to **Model View** and review the diagram
- [ ] Verify all tables are present and populated
- [ ] Check relationship cardinalities (manyToOne vs manyToMany)
- [ ] Confirm relationship directions (single vs bi-directional)
- [ ] Look for inactive relationships (may need manual activation)
- [ ] Verify the Calendar table date range covers your data
- [ ] Check Date Hierarchy (Year → Quarter → Month → Day)

## 6. Measures & Calculated Columns

- [ ] In each table, review measures for correctness
- [ ] Check calculated columns compute expected values
- [ ] Look for `/* Migration note: ... */` comments in DAX formulas
- [ ] Verify cross-table references use `RELATED()` or `LOOKUPVALUE()` correctly
- [ ] Test time intelligence measures (YTD, PY, YoY%) if applicable
- [ ] Check What-If parameter slicers and connected measures

## 7. Power BI Report Pages

- [ ] Open the `.pbip` file in Power BI Desktop (December 2025+)
- [ ] Confirm the project loads without errors
- [ ] Review each page layout — adjust visual positioning if needed
- [ ] Verify visual types match Tableau originals (check approximations)
- [ ] Click through slicers — confirm they filter correctly
- [ ] Test drill-through pages (right-click → Drill through)
- [ ] Check tooltip pages appear on hover
- [ ] Verify bookmarks (from Tableau stories) capture correct states
- [ ] Review mobile layout pages (if applicable)

## 8. Filters

- [ ] Check report-level filters in the Filters pane
- [ ] Verify page-level filters on each page
- [ ] Confirm visual-level filters work correctly
- [ ] Test TopN filters if present

## 9. Formatting & Theme

- [ ] Verify the custom theme colors match the Tableau originals
- [ ] Check conditional formatting (gradient colors)
- [ ] Review reference lines on axes
- [ ] Verify axis labels, legends, and data labels
- [ ] Check number formatting on measures and columns

## 10. Row-Level Security (RLS)

- [ ] Go to **Modeling** → **Manage Roles**
- [ ] Review each RLS role and its DAX filter expression
- [ ] Test with **View as Role** to confirm data filtering
- [ ] Assign Azure AD users/groups to roles after publishing

## 11. Performance

- [ ] Run **Performance Analyzer** on key pages
- [ ] Check for slow visuals or expensive DAX queries
- [ ] Consider adding aggregations for large datasets
- [ ] Review DirectLake vs Import mode for each table

## 12. Publish & Share

- [ ] Publish the report to the correct Fabric workspace
- [ ] Configure scheduled refresh for the Dataflow/Pipeline
- [ ] Set up RLS role assignments for end users
- [ ] Create a Power BI app for distribution (if needed)
- [ ] Compare key metrics between Tableau and Fabric outputs

## Quick Reference

| Tableau Feature | Where to Check in Fabric |
|----------------|----------------------|
| Worksheets | Report pages → individual visuals |
| Dashboards | Report pages |
| Parameters | What-If parameter slicers |
| Calculated fields | Measures & calculated columns in Model view |
| Stories | Bookmarks |
| Filters | Filters pane (report/page/visual level) |
| User filters | Manage Roles (RLS) |
| Custom SQL | Dataflow Gen2 → Advanced Editor |
| Actions | Action buttons, drill-through pages |
| Data sources | Lakehouse tables, Dataflow Gen2 queries |
