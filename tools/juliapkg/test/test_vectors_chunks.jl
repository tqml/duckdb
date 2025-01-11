struct MyStruct
    a::Int
    b::Float64
    c::NTuple{3, Int}
end

@testset "Logical Types" begin

    int_duck_types =
        ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT")
    int_types = (Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64, UInt128)
    for t in int_types
        lt = DuckDB.create_logical_type(t)
        @test lt.handle != C_NULL
    end

    v = [1, 2, 3]
    lt = DuckDB.create_logical_type(typeof(v))
    @test lt.handle != C_NULL
    @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_LIST

    t = (1, 2, 3)
    lt = DuckDB.create_logical_type(typeof(t))
    @test lt.handle != C_NULL
    @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_ARRAY

    d = Dict(["a" => 1, "b" => 2, "c" => 3])
    lt = DuckDB.create_logical_type(typeof(d))
    @test lt.handle != C_NULL
    @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_MAP

    nt = (a = 1, b = 3.0, c = "hello", d = (1, 2, 3), e = [3.0, 4.0, 5.0])
    lt = DuckDB.create_logical_type(typeof(nt))
    @test lt.handle != C_NULL
    @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_STRUCT

    DuckDB.set_alias!(lt, "ComplexStruct")
    lt_alias = DuckDB.alias(lt)
    @test lt_alias.alias_value == "ComplexStruct"
    lt_alias = nothing # 
    Base.GC.gc()

end




@testset "Chunks with Complex types" begin

    N = 16
    int_types = (Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64, UInt128)
    types = [DuckDB.create_logical_type(t) for t in int_types]
    chunk = DuckDB.DataChunk(types)

    @test DuckDB.get_column_count(chunk) == length(int_types)
    @test DuckDB.get_size(chunk) == 0
    DuckDB.set_size(chunk, N)
    DuckDB.get_size(chunk) == N

    # Check if writing is fine
    for (i, T) in enumerate(int_types)
        X = DuckDB.get_array(chunk, i, T)
        for j in 1:N
            X[j] = T(j)
            @test X[j] == T(j)
        end
    end

    N = 32
    data = [MyStruct(rand(1:10), rand(), (rand(1:10), rand(1:10), rand(1:10))) for i in 1:N]
    #T = eltype(data)
    T = eltype(data)
    LT = DuckDB.create_logical_type(T)
    chunk = DuckDB.DataChunk([LT])
    DuckDB.set_size(chunk, length(data))
    @test DuckDB.get_size(chunk) == length(data)

    vec = DuckDB.get_vector(chunk, 1)

    lt = DuckDB.get_logical_type(vec)
    @test lt.handle != C_NULL
    @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_STRUCT
    vec_struct = DuckDB.struct_child(vec, 1)

    X = DuckDB.get_array(vec_struct, T, N)
    for i in 1:N
        X[i] = data[i]
        @test X[i] == data[i]
    end
end
