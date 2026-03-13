// Query: Sheet1
// Source: Excel
// Destination: Lakehouse → sheet1
// Generated: 2026-03-05T10:47:21.750173

let
    // Source Excel: Data/Datasets/nba_players.xlsx
    Source = Excel.Workbook(File.Contents(DataFolder & "\Data\Datasets\nba_players.xlsx"), null, true),
    #"Sheet1 Sheet" = Source{[Item="Sheet1",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(#"Sheet1 Sheet", [PromoteAllScalars=true]),
    #"Changed Types" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"Player", type text},
        {"YR", Int64.Type},
        {"TM", type text},
        {"GP", Int64.Type},
        {"MIN", type number},
        {"FGM", type number},
        {"FGA", type number},
        {"FG%", type number},
        {"3PT%", type number},
        {"FT%", type number},
        {"OFF", type number},
        {"DEF", type number},
        {"REB", type number},
        {"ASST", type number},
        {"STL", type number},
        {"BLK", type number},
        {"TO", type number},
        {"PF", type number},
        {"PTS", type number}
    }),
    Result = #"Changed Types"
in
    Result