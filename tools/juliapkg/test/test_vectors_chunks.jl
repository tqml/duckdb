

@testset "VecWriter Simple" begin
    N = 2048

    # Ints
    T = Int
    logical_type = DuckDB.create_logical_type(T)
    chunk = DuckDB.DataChunk([logical_type])
    DuckDB.set_size(chunk, N)
    
    vector = DuckDB.get_vector(chunk, 1)
    writer = DuckDB.VecWriter(vector, logical_type, T, N)

    @inferred DuckDB.VecWriter(vector, logical_type, T, N)
end

@testset "VecReader Simple" begin
    N = 2048

    # Ints
    T = Int
    logical_type = DuckDB.create_logical_type(T)
    chunk = DuckDB.DataChunk([logical_type])
    DuckDB.set_size(chunk, N)
    
    vector = DuckDB.get_vector(chunk, 1)
    writer = DuckDB.VecWriter(vector, logical_type, T, N)

    DuckDB.VecWriter(vector, logical_type, T, N)
    X = rand(T, N)
    DuckDB.write_to_vector(writer, X)


    reader = DuckDB.VecReader(vector, logical_type, T, N)
end

@testset "VecWriter Conversions Julia to Internal to Julia: Combined Multi Chunk" begin
    using DuckDB, DataFrames
    include("test_logical_types_helper.jl")
    N = 2048


    NT1 = @NamedTuple begin
        a::Int
        b::Float64
        c::String
    end
    NT2 = @NamedTuple begin
        a::Vector{Int}
        b::Dict{String, String}
    end


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
        #NTuple{10, Int},
        NT1,
        NT2,
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
        Vector{Vector{Vector{Float64}}}  # Nested Lists
        #Union{String, Int, Float64, Bool}
    ]
    t_reads = Float64[]
    t_writes = Float64[]
    t_writes2 = Float64[]
    t_baselines = Float64[]
    for T in types
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

        Base.GC.@preserve chunks begin
            t_write = @elapsed for chunk in chunks
                vec = DuckDB.get_vector(chunk, 1)
                writer = DuckDB.VecWriter(vec, logical_type, julia_type_in, N)
                sizehint!(writer, N)
                for i in 1:N
                    writer[i] = X[i]
                end
            end

            t_write_2 = 0.0
            # t_write_2 = @elapsed for chunk in chunks
            #     vec = DuckDB.get_vector(chunk, 1)
            #     N_chunk = Int(DuckDB.get_size(chunk))
            #     DuckDB.write_to_vector(vec, N_chunk, logical_type, X)
            # end

            t_read = 0.0
            t_read = @elapsed for chunk in chunks
                vec = DuckDB.get_vector(chunk, 1)
                reader = DuckDB.VecReader(vec, logical_type, julia_type_out, N)
                #julia_type_out_2 = DuckDB.julia_eltype(reader)
                #out = collect(reader)
                if DuckDB.all_valid(reader)
                    out = Array{eltype(reader)}(undef, N)
                else
                    out = Array{Union{eltype(reader), Missing}}(undef, N)
                end
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
            push!(t_writes2, t_write_2)
            push!(t_baselines, t_baseline)
        end
    end

    df = DataFrame(Type = types, Baseline = t_baselines, Read = t_reads, Write = t_writes, Write2 = t_writes2)
    PrettyTables.pretty_table(df)
end



@testset "VecWriter Type Inference" begin
    using DuckDB, UUIDs, Dates, DataFrames
    NT1 = @NamedTuple begin
        a::Int
        b::Float64
        c::String
    end
    NT2 = @NamedTuple begin
        a::Vector{Int}
        b::Dict{String, String}
    end
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
        NT1,
        NT2
        # Vector{Int},
        # Vector{Vector{Int}},
        # Vector{Vector{Vector{Int}}},
        # Union{Missing, Int},
        # Union{Missing, Vector{Union{Missing, Int}}},
        # Dict{String, String},
        # Dict{String, Vector{Int}},
        # Vector{Vector{Vector{Float64}}},  # Nested Lists
    ]

    logical_types = [DuckDB.create_logical_type(T) for T in types]
    chunk = DuckDB.DataChunk(logical_types)
    DuckDB.set_size(chunk, 0) # Empty chunk

    @test DuckDB.get_size(chunk) == 0
    @test DuckDB.get_column_count(chunk) == length(types)

    vecs = [DuckDB.get_vector(chunk, i) for i in 1:length(types)]


    for (vec, lt, T) in zip(vecs, logical_types, types)
        writer = DuckDB.VecWriter(vec, lt, T, 0)
        @show typeof(writer)
        @inferred DuckDB.VecWriter(vec, lt, T, 0)
    end

    #Base.return_types
    #R = Base.promote_op(DuckDB.VecWriter, vecs[end], logical_types[end], types[end], 0)
    #@inferred DuckDB.VecWriter(vec, logical_types[end], types[end], 0)

    #@inferred NT1.types[1]
end