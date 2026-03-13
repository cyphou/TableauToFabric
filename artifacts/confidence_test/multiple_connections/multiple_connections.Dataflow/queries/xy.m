// Query: xy
// Source: MySQL
// Destination: Lakehouse → xy
// Generated: 2026-03-05T10:47:16.119358

let
    // Source MySQL: mysql55:
    Source = MySQL.Database("mysql55:", "testv1"),
    #"xy Table" = Source{[Schema="testv1", Item="xy"]}[Data],
    Result = #"xy Table"
in
    Result