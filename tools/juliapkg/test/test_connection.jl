# test_connection.jl

@testset "Test opening and closing an in-memory database" begin
    con = DBInterface.connect(DuckDB.DB, ":memory:")
    DBInterface.close!(con)
    # verify that double-closing does not cause any problems
    DBInterface.close!(con)
    DBInterface.close!(con)
    @test 1 == 1

    con = DBInterface.connect(DuckDB.DB, ":memory:")
    @test isopen(con)
    close(con)
    @test !isopen(con)
end

@testset "Test opening a bogus directory" begin
    @test_throws DuckDB.ConnectionException DBInterface.connect(DuckDB.DB, "/path/to/bogus/directory")
end

@testset "Test using a closed connection" begin
    db = DBInterface.connect(DuckDB.DB, ":memory:")
    con = DBInterface.connect(db)
    
    DuckDB.execute(db, "CREATE TABLE test (a INTEGER)")
    DuckDB.execute(db, "INSERT INTO test VALUES (1)")
    DBInterface.close!(db) # Close only the database object, not the connection
    @test isopen(con) == true
    @test isopen(db) == false
    @test_throws DuckDB.ConnectionException DuckDB.execute(db, "SELECT * FROM test")


    DuckDB.execute(con, "SELECT * FROM test") # This should work
    DBInterface.close!(con) # Close the connection
    @test_throws DuckDB.ConnectionException DuckDB.execute(con, "SELECT 1")
    @test_throws DuckDB.ConnectionException con2 = DuckDB.connect(db)
    Base.GC.gc()
    con = nothing
    db = nothing
    Base.GC.gc() # check if finalizers are working
end