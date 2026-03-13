// Query: Extract
// Source: Excel
// Destination: Lakehouse → extract
// Generated: 2026-03-05T10:47:38.165267

let
    // Source Excel: 
    Source = Excel.Workbook(File.Contents(DataFolder & "\"), null, true),
    #"Extract Sheet" = Source{[Item="Extract",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Extract Sheet", [PromoteAllScalars=true]),
    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"Row ID", Int64.Type},
        {"Order ID", Int64.Type},
        {"Order Date", type date},
        {"Order Priority", type text},
        {"Order Quantity", Int64.Type},
        {"Sales", type number},
        {"Discount", type number},
        {"Ship Mode", type text},
        {"Profit", type number},
        {"Unit Price", type number},
        {"Shipping Cost", type number},
        {"Customer Name", type text},
        {"City", type text},
        {"Zip Code", type text},
        {"State", type text},
        {"Region", type text},
        {"Customer Segment", type text},
        {"Product Category", type text},
        {"Product Sub-Category", type text},
        {"Product Name", type text},
        {"Product Container", type text},
        {"Product Base Margin", type number},
        {"Ship Date", type date}
    }),
    Result = #"Changed Types"
in
    Result