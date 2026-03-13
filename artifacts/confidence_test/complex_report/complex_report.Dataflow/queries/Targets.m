// Query: Targets
// Source: Excel
// Destination: Lakehouse → targets
// Generated: 2026-03-05T10:46:29.869651

let
    // Source Excel: \\\\fileserver\\data\\Budget_2024.xlsx
    Source = Excel.Workbook(File.Contents(DataFolder & "\\\\\fileserver\\data\\Budget_2024.xlsx"), null, true),
    #"Targets Sheet" = Source{[Item="Targets",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Targets Sheet", [PromoteAllScalars=true]),
    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"Category", type text},
        {"Region", type text},
        {"TargetRevenue", type number},
        {"TargetProfit", type number},
        {"FiscalYear", Int64.Type}
    }),
    Result = #"Changed Types"
in
    Result