// Dataflow Gen2 Mashup Document
// Project: filtering
// Generated: 2026-03-05T10:47:04.734030

section Section1;

// ── TestData ──────────────────────────────────────────────────────
// Source: SQL Server
// → Lakehouse.testdata
shared TestData = let
    // Source SQL Server
    Source = Sql.Database("b5dpm3ihhu.database.windows.net", "EmptyDB"),
    #"TestData Table" = Source{[Schema="dbo", Item="TestData"]}[Data],
    Result = #"TestData Table"
,
    CalcCol_show = Table.AddColumn(Result, "SHOW", each if [BurstoutSet] then "Selected" else "not Selected")
in
    CalcCol_show;

