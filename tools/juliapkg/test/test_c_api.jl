
@testset "C API Type Checks" begin

    # Check struct sizes.
    # Timestamp struct size mismatch, eventually structs are stored as pointers. This happens if they are declared as mutable structs.
    @test sizeof(DuckDB.duckdb_timestamp_struct) ==
          sizeof(DuckDB.duckdb_date_struct) + sizeof(DuckDB.duckdb_time_struct)

    # Bot structs are equivalent and actually stored as a Union type in C.
    @test sizeof(DuckDB.duckdb_string_t) == sizeof(DuckDB.duckdb_string_t_ptr)

end

@testset "Periods & Conversions" begin
    N = 2048
    X = [random_compound_period() for i in 1:N]
    julia_type = eltype(X)
    internal_type = DuckDB.duckdb_interval
    Y = [convert(internal_type, x) for x in X]
    Z = [Dates.canonicalize(convert(julia_type, y)) for y in Y]
    @test isequal(X, Z)

    X = [random_period() for i in 1:N]
    julia_type = eltype(X)
    julia_type_out = Dates.CompoundPeriod
    internal_type = DuckDB.duckdb_interval
    Y = [convert(internal_type, x) for x in X]
    Z = [Dates.canonicalize(convert(julia_type_out, y)) for y in Y]
    @test isequal(X, Z)
end


@testset "Check Type Stability" begin

    @code_warntype convert(DuckDB.duckdb_interval, Dates.CompoundPeriod(Day(1)))
end
