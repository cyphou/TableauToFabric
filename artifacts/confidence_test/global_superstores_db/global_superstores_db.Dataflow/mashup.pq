// Dataflow Gen2 Mashup Document
// Project: global_superstores_db
// Generated: 2026-03-05T10:47:10.598052

section Section1;

// ── Orders ────────────────────────────────────────────────────────
// Source: Excel
// → Lakehouse.orders
shared Orders = let
    // Source Excel: /Users/bs/Mac Drive/MSBA/INFO584_01SP21/Datasets/Global Superstore.xlsx
    Source = Excel.Workbook(File.Contents(DataFolder & "\\Users\bs\Mac Drive\MSBA\INFO584_01SP21\Datasets\Global Superstore.xlsx"), null, true),
    #"Orders Sheet" = Source{[Item="Orders",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Orders Sheet", [PromoteAllScalars=true]),
    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"Row ID", Int64.Type},
        {"Order ID", type text},
        {"Order Date", type date},
        {"Ship Date", type date},
        {"Ship Mode", type text},
        {"Customer ID", type text},
        {"Customer Name", type text},
        {"Segment", type text},
        {"City", type text},
        {"State", type text},
        {"Country", type text},
        {"Postal Code", type text},
        {"Market", type text},
        {"Region", type text},
        {"Product ID", type text},
        {"Category", type text},
        {"Sub-Category", type text},
        {"Product Name", type text},
        {"Sales", type number},
        {"Quantity", Int64.Type},
        {"Discount", type number},
        {"Profit", type number},
        {"Shipping Cost", type number},
        {"Order Priority", type text}
    }),
    Result = #"Changed Types"
,
    CalcCol_discount_bin = Table.AddColumn(Result, "[Discount (bin)]", each [Discount])
in
    CalcCol_discount_bin;

