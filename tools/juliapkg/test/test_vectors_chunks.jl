# struct MyStruct
#     a::Int
#     b::Float64
#     c::NTuple{3, Int}
# end

# @testset "Logical Types" begin

#     int_duck_types =
#         ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT")
#     int_types = (Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64, UInt128)
#     for t in int_types
#         lt = DuckDB.create_logical_type(t)
#         @test lt.handle != C_NULL
#     end

#     v = [1, 2, 3]
#     lt = DuckDB.create_logical_type(typeof(v))
#     @test lt.handle != C_NULL
#     @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_LIST

#     t = (1, 2, 3)
#     lt = DuckDB.create_logical_type(typeof(t))
#     @test lt.handle != C_NULL
#     @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_ARRAY

#     d = Dict(["a" => 1, "b" => 2, "c" => 3])
#     lt = DuckDB.create_logical_type(typeof(d))
#     @test lt.handle != C_NULL
#     @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_MAP

#     nt = (a = 1, b = 3.0, c = "hello", d = (1, 2, 3), e = [3.0, 4.0, 5.0])
#     lt = DuckDB.create_logical_type(typeof(nt))
#     @test lt.handle != C_NULL
#     @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_STRUCT

#     DuckDB.set_alias!(lt, "ComplexStruct")
#     lt_alias = DuckDB.alias(lt)
#     @test lt_alias.alias_value == "ComplexStruct"
#     lt_alias = nothing # 
#     Base.GC.gc()

# end




# @testset "Chunks with Complex types" begin

#     N = 16
#     int_types = (Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64, UInt128)
#     types = [DuckDB.create_logical_type(t) for t in int_types]
#     chunk = DuckDB.DataChunk(types)

#     @test DuckDB.get_column_count(chunk) == length(int_types)
#     @test DuckDB.get_size(chunk) == 0
#     DuckDB.set_size(chunk, N)
#     DuckDB.get_size(chunk) == N

#     # Check if writing is fine
#     for (i, T) in enumerate(int_types)
#         X = DuckDB.get_array(chunk, i, T)
#         for j in 1:N
#             X[j] = T(j)
#             @test X[j] == T(j)
#         end
#     end

#     N = 32
#     data = [MyStruct(rand(1:10), rand(), (rand(1:10), rand(1:10), rand(1:10))) for i in 1:N]
#     #T = eltype(data)
#     T = eltype(data)
#     LT = DuckDB.create_logical_type(T)
#     chunk = DuckDB.DataChunk([LT])
#     DuckDB.set_size(chunk, length(data))
#     @test DuckDB.get_size(chunk) == length(data)

#     vec = DuckDB.get_vector(chunk, 1)

#     lt = DuckDB.get_logical_type(vec)
#     @test lt.handle != C_NULL
#     @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_STRUCT
#     vec_struct = DuckDB.struct_child(vec, 1)

#     X = DuckDB.get_array(vec_struct, T, N)
#     for i in 1:N
#         X[i] = data[i]
#         @test X[i] == data[i]
#     end
# end




@testset failfast=true "Conversions Julia to Internal to Julia: Combined Multi Chunk" begin
    using DuckDB
    include("test_logical_types_helper.jl")
    N = 2048

    types = [
        UUID,
        Bool,
        Int8,
        Int16,
        Int32,
        Int64,
        Int128,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        UInt128,
        Float32,
        Float64,
        String,
        Date,
        Time,
        DateTime,
        Dates.CompoundPeriod,
        NTuple{10, Int},
        #StaticArrays.SVector{10, Int},
        #StaticArrays.SMatrix{2, 2, Int},
        #StaticArrays.SArray{Tuple{2, 2, 2}, Int, 3},
        Vector{Int},
        Vector{Vector{Int}},
        Vector{Vector{Vector{Int}}},
        Union{Missing, Int},
        Union{Missing, Vector{Union{Missing, Int}}},
        Dict{String, String},
        Dict{String, Vector{Int}},
        Vector{Vector{Vector{Float64}}},  # Nested Lists
        Union{String, Int, Float64, Bool}
        # MyStruct,
        # Dict{String, MyStruct}, # Write works, but read creates NamedTuple instead of struct
    ]
    t_reads = Float64[]
    t_writes = Float64[]
    t_baselines = Float64[]
    for T in types
        println("Type: ", T)


        if T === Union{String, Int, Float64, Bool}
            # Make generic?
            logical_type = DuckDB.create_union_type((String, Int, Float64, Bool))
            X = Vector{T}([_random_element_union() for i in 1:N])
            julia_type_in = eltype(X)
        else
            X = [_random_element(T, 5) for i in 1:N]
            julia_type_in = eltype(X)
            logical_type = DuckDB.create_logical_type(julia_type_in)
        end
        chunks = [DuckDB.DataChunk([logical_type]) for _ in 1:10] # 10 chunks
        DuckDB.set_size.(chunks, N)


        julia_type_out = DuckDB.duckdb_type_to_julia_type(logical_type)
        @show julia_type_in julia_type_out


        Base.GC.@preserve chunks begin

            println("Write Test: ", T)
            t_write = @elapsed for chunk in chunks
                vec = DuckDB.get_vector(chunk, 1)
                writer = DuckDB.VecWriter(vec, logical_type, julia_type_in, N)
                sizehint!(writer, N)
                for i in 1:N
                    writer[i] = X[i]
                end
            end

            t_read = 0.0
            t_read = @elapsed for chunk in chunks
                vec = DuckDB.get_vector(chunk, 1)
                reader = DuckDB.VecReader(vec, logical_type, julia_type_out, N)
                julia_type_out_2 = DuckDB.julia_eltype(reader)
                @show julia_type_in julia_type_out julia_type_out_2
                out = Vector{julia_type_out_2}(undef, N)
                for i in 1:N
                    out[i] = reader[i]
                end
            end

            if T <: Period || T <: Dates.CompoundPeriod
                out = [Dates.canonicalize(x) for x in out]
            end

            @test isequal(X, out)

            t_baseline = 0.0
            if !(T <: Tuple) && !(T <: MyStruct) && !(T === Union{String, Int, Float64, Bool})
                # Tuple not supported
                try
                    data = DuckDB.ColumnConversionData(chunks, 1, logical_type, nothing)
                    t_baseline = @elapsed DuckDB.convert_column(data)
                catch e
                    println("Skipped baseline for type ", T, ", error:", e)
                end
            end
            push!(t_reads, t_read)
            push!(t_writes, t_write)
            push!(t_baselines, t_baseline)

            #println("Type: ", T, "\t Read: \t\t", t_read, "\t\t Write: \t\t", t_write, "\t\t Baseline: \t\t", t_baseline)
        end
    end

    df = DataFrame(Type = types, Read = t_reads, Write = t_writes, Baseline = t_baselines)
    PrettyTables.pretty_table(df)
end