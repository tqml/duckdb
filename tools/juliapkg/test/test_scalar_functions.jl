
@testset "Scalar 1 - Int" begin
    con = DuckDB.DB(":memory:")
    my_scalar_function = (a) -> a + 42
    DuckDB.create_scalar_function(con, "forty_two", [Int64], Int64, my_scalar_function)
    DuckDB.execute(con, "CREATE TABLE test (a BIGINT, b BIGINT, c BIGINT);")
    DuckDB.execute(con, "INSERT INTO test VALUES (1, 10, 100),(2, 10, 100),(3, 10, 100);")
    DuckDB.execute(con, "SELECT * FROM test;")
    result = DuckDB.query(con, "SELECT forty_two(1) as d;")
    println(result)
    df = DataFrame(result)
    @test df.d == [43]

    result = DuckDB.query(con, "SELECT forty_two(a) as d FROM test;")
    println(result)

    df = DataFrame(result)
    @test df.d == [43, 44, 45]
    GC.gc()
end

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

# @testset verbose = true "Scalar_2" begin
#     con = DuckDB.DB(":memory:")
#     my_scalar_function = (a, b) -> a + b
#     DuckDB.create_scalar_function(con, "my_sum", [Int64, Int64], Int64, my_scalar_function)
#     DuckDB.execute(con, "CREATE TABLE test (a BIGINT, b BIGINT, c BIGINT);")
#     DuckDB.execute(con, "INSERT INTO test VALUES (1, 10, 100),(2, 10, 100),(3, 10, 100);")
#     DuckDB.execute(con, "SELECT * FROM test;")
#     result = DuckDB.query(con, "SELECT my_sum(2,3) as d;")
#     df = DataFrame(result)
#     println(result)
#     @test df.d == [5]

#     result = DuckDB.query(con, "SELECT my_sum(a,b) as d FROM test;")
#     df = DataFrame(result)
#     println(result)
#     @test df.d == [11, 12, 13]
#     GC.gc()
# end



# @testset verbose = true "Mixed datatypes" begin
#     con = DuckDB.DB(":memory:")
#     my_repeat = (a::String, b::Int) -> repeat(a, b)

#     DuckDB.create_scalar_function(con, "my_repeat", [String, Int64], String, my_repeat)
#     DuckDB.execute(con, "CREATE TABLE test (a VARCHAR, b BIGINT);")
#     DuckDB.execute(con, "INSERT INTO test VALUES ('Hello', 2), ('World',3);")

#     results = DuckDB.execute(con, "SELECT my_repeat('Hello', 3) as c;")
#     df = DataFrame(results)
#     @test df.c == ["HelloHelloHello"]

#     results = DuckDB.execute(con, "SELECT my_repeat(a,b) as c FROM test;")
#     df = DataFrame(results)
#     @test df.c == ["HelloHello", "WorldWorld"]
#     GC.gc()
# end
