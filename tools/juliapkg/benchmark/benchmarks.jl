using BenchmarkTools, DuckDB, Dates, DataFrames, Tables, StaticArrays

const SUITE = BenchmarkGroup()

SUITE["UDF"] = BenchmarkGroup()
SUITE["TableScan"] = BenchmarkGroup()
SUITE["Result"] = BenchmarkGroup()
SUITE["ColumnConversion"] = BenchmarkGroup()

db = DuckDB.DB()
con = DuckDB.connect(db)

DuckDB.execute(con, "CREATE TABLE test AS SELECT random() a, random() b FROM range(1000000);")


f_add = (a, b) -> a + b
fun = DuckDB.@create_scalar_function f_add(a::Float64, b::Float64)::Float64 f_add
DuckDB.register_scalar_function(con, fun)

SUITE["UDF"]["float_add"] = @benchmarkable begin
    DuckDB.execute(con, "SELECT f_add(a, b) FROM test;")
end




include("bench_col_conversion.jl")