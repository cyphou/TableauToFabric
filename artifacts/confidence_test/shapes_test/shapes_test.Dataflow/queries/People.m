// Query: People
// Source: Excel
// Destination: Lakehouse → people
// Generated: 2026-03-05T10:47:32.638377

let
    // Source Excel: /Users/mkorotchenkov/Documents/My Tableau Repository/Datasources/2021.2/en_US-US/Sample - Superstore.xls
    Source = Excel.Workbook(File.Contents(DataFolder & "\\Users\mkorotchenkov\Documents\My Tableau Repository\Datasources\2021.2\en_US-US\Sample - Superstore.xls"), null, true),
    #"People Sheet" = Source{[Item="People",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"People Sheet", [PromoteAllScalars=true]),
    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"Regional Manager", type text},
        {"Region", type text}
    }),
    Result = #"Changed Types"
in
    Result