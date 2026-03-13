// Dataflow Gen2 Mashup Document
// Project: shapes_test
// Generated: 2026-03-05T10:47:32.640736

section Section1;

// ── Orders ────────────────────────────────────────────────────────
// Source: Excel
// → Lakehouse.orders
shared Orders = let
    // Source Excel: /Users/mkorotchenkov/Documents/My Tableau Repository/Datasources/2021.2/en_US-US/Sample - Superstore.xls
    Source = Excel.Workbook(File.Contents(DataFolder & "\\Users\mkorotchenkov\Documents\My Tableau Repository\Datasources\2021.2\en_US-US\Sample - Superstore.xls"), null, true),
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
        {"Country/Region", type text},
        {"City", type text},
        {"State", type text},
        {"Postal Code", Int64.Type},
        {"Region", type text},
        {"Product ID", type text},
        {"Category", type text},
        {"Sub-Category", type text},
        {"Product Name", type text},
        {"Sales", type number},
        {"Quantity", Int64.Type},
        {"Discount", type number},
        {"Profit", type number}
    }),
    Result = #"Changed Types"
,
    CalcCol_profit_bin = Table.AddColumn(Result, "Profit (bin)", each [Profit])
in
    CalcCol_profit_bin;

// ── People ────────────────────────────────────────────────────────
// Source: Excel
// → Lakehouse.people
shared People = let
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
    Result;

// ── Returns ───────────────────────────────────────────────────────
// Source: Excel
// → Lakehouse.returns
shared Returns = let
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
    Result;

