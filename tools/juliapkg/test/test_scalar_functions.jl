

@testset "Scalar 1 - Macro" begin
    con = DuckDB.DB(":memory:")
    my_scalar_function = (a) -> a + 42
    fun = DuckDB.@create_scalar_function "forty_two" [Int64] Int64 my_scalar_function
    DuckDB.register_scalar_function(con, fun)
    # Create dummy data
    DuckDB.execute(con, "CREATE TABLE test (a BIGINT, b BIGINT, c BIGINT);")
    DuckDB.execute(con, "INSERT INTO test VALUES (1, 10, 100),(2, 10, 100),(3, 10, 100);")
    DuckDB.execute(con, "SELECT * FROM test;")

    # Test the scalar function
    result = DuckDB.query(con, "SELECT forty_two(1) as d;")
    println(result)
    df = DataFrame(result)
    @test df.d == [43]
    # 
    result = DuckDB.query(con, "SELECT forty_two(a) as d FROM test;")
    println(result)

    df = DataFrame(result)
    @test df.d == [43, 44, 45]
    GC.gc()
end

@testset "Scalar_2" begin
    con = DuckDB.DB(":memory:")

    my_scalar_function = (a, b) -> a + b
    fun = DuckDB.@create_scalar_function "my_sum" [Int64, Int64] Int64 my_scalar_function
    DuckDB.register_scalar_function(con, fun)

    DuckDB.execute(con, "CREATE TABLE test (a BIGINT, b BIGINT, c BIGINT);")
    DuckDB.execute(con, "INSERT INTO test VALUES (1, 10, 100),(2, 10, 100),(3, 10, 100);")
    DuckDB.execute(con, "SELECT * FROM test;")
    result = DuckDB.query(con, "SELECT my_sum(2,3) as d;")
    df = DataFrame(result)
    println(result)
    @test df.d == [5]

    result = DuckDB.query(con, "SELECT my_sum(a,b) as d FROM test;")
    df = DataFrame(result)
    println(result)
    @test df.d == [11, 12, 13]
    GC.gc()
end

@testset "Scalar_2_float_int_mixed" begin
    con = DuckDB.DB(":memory:")

    my_scalar_function = (a, b) -> a^b
    fun = DuckDB.@create_scalar_function "my_pow" [Float64, Int64] Float64 my_scalar_function
    DuckDB.register_scalar_function(con, fun)

    DuckDB.execute(con, "CREATE TABLE test (a BIGINT, b DOUBLE, c BIGINT);")
    DuckDB.execute(con, "INSERT INTO test VALUES (1, 10, 100),(2, 10, 100),(3, 10, 100);")
    DuckDB.execute(con, "SELECT * FROM test;")
    result = DuckDB.query(con, "SELECT my_pow(2,3) as d;")
    df = DataFrame(result)
    println(result)
    @test df.d == [8.0]

    result = DuckDB.query(con, "SELECT my_pow(b,a) as d FROM test;")
    df = DataFrame(result)
    println(result)
    @test df.d == [10.0, 100.0, 1000.0]
    GC.gc()
end

# @testset verbose = true "Scalar functions with Strings" begin
#     con = DuckDB.DB(":memory:")
#     my_repeat = (b::Int) -> repeat("Hello", b)

#     @info "Register Scalar Function"
#     fun = DuckDB.@create_scalar_function "my_repeat" [Int64] String my_repeat
#     DuckDB.register_scalar_function(con, fun)

#     DuckDB.execute(con, "CREATE TABLE test (a VARCHAR, b BIGINT);")
#     DuckDB.execute(con, "INSERT INTO test VALUES ('Hello', 2), ('World',3);")

#     @info "Test Scalar Function"
#     results = DuckDB.execute(con, "SELECT my_repeat(3) as c;")
#     df = DataFrame(results)
#     @test df.c == ["HelloHelloHello"]

#     results = DuckDB.execute(con, "SELECT my_repeat(b) as c FROM test;")
#     df = DataFrame(results)
#     @test df.c == ["HelloHello", "WorldWorld"]
#     GC.gc()
# end


# @testset verbose = true "Scalar functions with Strings" begin
#     con = DuckDB.DB(":memory:")
#     my_repeat = (a::String, b::Int) -> repeat(a, b)

#     @info "Register Scalar Function"
#     fun = DuckDB.@create_scalar_function "my_repeat" [String, Int64] String my_repeat
#     DuckDB.register_scalar_function(con, fun)

#     DuckDB.execute(con, "CREATE TABLE test (a VARCHAR, b BIGINT);")
#     DuckDB.execute(con, "INSERT INTO test VALUES ('Hello', 2), ('World',3);")

#     @info "Test Scalar Function"
#     results = DuckDB.execute(con, "SELECT my_repeat('Hello', 3) as c;")
#     df = DataFrame(results)
#     @test df.c == ["HelloHelloHello"]

#     results = DuckDB.execute(con, "SELECT my_repeat(a,b) as c FROM test;")
#     df = DataFrame(results)
#     @test df.c == ["HelloHello", "WorldWorld"]
#     GC.gc()
# end
