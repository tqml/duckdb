mutable struct MyStruct
    a::Int
    b::Float64
    c::NTuple{3, Int}
end

function random_string(len)
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    unicode_alphabet = ["🦆", "α", "ξ"]
    return join(vcat(rand(alphabet, len), rand(unicode_alphabet, len)))
end

random_blob(len) = rand(UInt8, len)
random_time() = Dates.Time(rand(0:23), rand(0:59), rand(0:59))
random_date() = Dates.Date(rand(1971:2030), rand(1:12), rand(1:28))
random_datetime() = Dates.DateTime(random_date(), random_time())
random_period() = Dates.Period(rand((Day, Hour, Minute, Second, Millisecond, Microsecond, Week, Month, Year))(1))
random_compound_period() = Dates.CompoundPeriod([random_period() for _ in 1:rand(2:5)])

_random_element(::Type{T}, size) where {T} = rand(T)
_random_element(::Type{String}, size) = random_string(size)
_random_element(::Type{Vector{UInt8}}, size) = random_blob(size)
_random_element(::Type{Dates.Time}, size) = random_time()
_random_element(::Type{Dates.Date}, size) = random_date()
_random_element(::Type{Dates.DateTime}, size) = random_datetime()
_random_element(::Type{Dates.Period}, size) = random_period()
_random_element(::Type{Dates.CompoundPeriod}, size) = random_compound_period()
_random_element(::Type{NTuple{N, T}}, size) where {N, T} = Tuple(_random_element(T, size) for _ in 1:N)
_random_element(::Type{Vector{T}}, size) where {T} = [_random_element(T, size) for _ in 1:size]
_random_element(::Type{MyStruct}, size) = MyStruct(rand(Int), rand(), _random_element(NTuple{3, Int}, size))




# Conversions Julia to Internal to Julia: Combined Multi Chunk
N = 2048


column_conversion_judgement_pairs = Vector{Tuple{String,String}}()

types = [
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
    Vector{Int},
    #MyStruct
]


# Base Example
base_example = function()
    N = 2048
    T = Int64
    X::Vector{T} = [T(rand(T)) for i in 1:N]
    julia_type_in = eltype(X)
    logical_type = DuckDB.create_logical_type(julia_type_in)
    chunk = DuckDB.DataChunk([logical_type])
    DuckDB.set_size(chunk, N)
    vec = DuckDB.get_vector(chunk, 1)
    writer = DuckDB.VecWriter(vec, logical_type, T, N)

    loop_func = (writer, X) -> begin
        local N = length(X)
        for i in 1:N
            writer[i] = X[i]
        end
    end

    loop_func(writer, X)
end

for T in types
    X = [_random_element(T, 5) for i in 1:N]
    julia_type_in = eltype(X)
    logical_type = DuckDB.create_logical_type(julia_type_in)

    chunks = [DuckDB.DataChunk([logical_type]) for _ in 1:1] # 10 chunks
    DuckDB.set_size.(chunks, N)

    Base.GC.@preserve chunks begin

        # Write some random data to the chunks
        for chunk in chunks
            vec = DuckDB.get_vector(chunk, 1)
            writer = DuckDB.VecWriter(vec, logical_type, julia_type_in, N)
            sizehint!(writer, N)
            for i in 1:N
                writer[i] = X[i]
            end
        end

        
        read_key_vec = "read_" * string(T) * "_VecReader"
        write_key_vec = "write_" * string(T) * "_VecWriter"
        read_key_col = "read_" * string(T) * "_ColumnConversion"


        # Benchmark Write
        SUITE["ColumnConversion"][write_key_vec] = @benchmarkable begin
            for chunk in $chunks
                vec = DuckDB.get_vector(chunk, 1)
                writer = DuckDB.VecWriter(vec, $logical_type, $julia_type_in, $N)
                sizehint!(writer, $N)
                for i in 1:$N
                    writer[i] = $X[i]
                end
            end
        end

        # Benchmark Read
        SUITE["ColumnConversion"][read_key_vec] = @benchmarkable begin
            for chunk in $chunks
                vec = DuckDB.get_vector(chunk, 1)
                reader = DuckDB.VecReader(vec, $logical_type, $julia_type_in, $N)
                T_out = DuckDB.julia_eltype(reader)
                out = Array{$julia_type_in}(undef, N)
                #out = Array{T_out}(undef, N)
                for i in 1:$N
                    #out[i] = reader[i]
                    out[i] = reader.conversion_func(reader.getindex_func(reader.data, i))
                end
            end
        end

        # Benchmark ColumnConversion
        if !(T <: Tuple) && !(T <: MyStruct)
            # Tuple not supported
            SUITE["ColumnConversion"][read_key_col] = @benchmarkable begin
                data = DuckDB.ColumnConversionData($chunks, 1, $logical_type, nothing)
                output = DuckDB.convert_column(data)
                length(output) == $N
            end

            push!(column_conversion_judgement_pairs, (read_key_vec, read_key_col))
        end

        # @test isequal(out, X)
        # t_baseline = 0.0
        # if !(T <: Tuple) && !(T <: MyStruct)
        #     # Tuple not supported
        #     try
        #         data = DuckDB.ColumnConversionData(chunks, 1, logical_type, nothing)
        #         t_baseline = @elapsed DuckDB.convert_column(data)
        #     catch e
        #         println("Skipped baseline for type ", T, ", error:", e)
        #     end
        # end
        # push!(t_reads, t_read)
        # push!(t_writes, t_write)
        # push!(t_baselines, t_baseline)

        #println("Type: ", T, "\t Read: \t\t", t_read, "\t\t Write: \t\t", t_write, "\t\t Baseline: \t\t", t_baseline)
    end
end

#df = DataFrame(Type = types, Read = t_reads, Write = t_writes, Baseline = t_baselines)

