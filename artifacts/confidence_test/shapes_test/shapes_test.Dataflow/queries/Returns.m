// Query: Returns
// Source: Excel
// Destination: Lakehouse → returns
// Generated: 2026-03-05T10:47:32.639555

let
    // Source Excel: /Users/mkorotchenkov/Documents/My Tableau Repository/Datasources/2021.2/en_US-US/Sample - Superstore.xls
    Source = Excel.Workbook(File.Contents(DataFolder & "\\Users\mkorotchenkov\Documents\My Tableau Repository\Datasources\2021.2\en_US-US\Sample - Superstore.xls"), null, true),
    #"Returns Sheet" = Source{[Item="Returns",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Returns Sheet", [PromoteAllScalars=true]),
    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"Returned", type text},
        {"Order ID", type text}
    }),
    Result = #"Changed Types"
in
    Result