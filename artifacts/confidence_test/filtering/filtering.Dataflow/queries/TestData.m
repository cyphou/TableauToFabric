// Query: TestData
// Source: SQL Server
// Destination: Lakehouse → testdata
// Generated: 2026-03-05T10:47:04.726922

let
    // Source SQL Server
    Source = Sql.Database("b5dpm3ihhu.database.windows.net", "EmptyDB"),
    #"TestData Table" = Source{[Schema="dbo", Item="TestData"]}[Data],
    Result = #"TestData Table"
,
    CalcCol_show = Table.AddColumn(Result, "SHOW", each if [BurstoutSet] then "Selected" else "not Selected")
in
    CalcCol_show