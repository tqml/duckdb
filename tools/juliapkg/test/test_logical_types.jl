
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

# @testset "Logical Types" begin

#     int_duck_types = (
#         DuckDB.DUCKDB_TYPE_BOOLEAN,
#         DuckDB.DUCKDB_TYPE_TINYINT,
#         DuckDB.DUCKDB_TYPE_SMALLINT,
#         DuckDB.DUCKDB_TYPE_INTEGER,
#         DuckDB.DUCKDB_TYPE_BIGINT,
#         DuckDB.DUCKDB_TYPE_HUGEINT,
#         DuckDB.DUCKDB_TYPE_UTINYINT,
#         DuckDB.DUCKDB_TYPE_USMALLINT,
#         DuckDB.DUCKDB_TYPE_UINTEGER,
#         DuckDB.DUCKDB_TYPE_UBIGINT,
#         DuckDB.DUCKDB_TYPE_UHUGEINT
#     )
#     int_types = (Bool, Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64, UInt128)
#     for (t, t_id) in zip(int_types, int_duck_types)
#         lt = DuckDB.create_logical_type(t)
#         @test lt.handle != C_NULL
#         @test DuckDB.get_type_id(lt) == t_id
#     end

#     datetime_duck_types = (
#         DuckDB.DUCKDB_TYPE_DATE,
#         DuckDB.DUCKDB_TYPE_TIME,
#         DuckDB.DUCKDB_TYPE_TIMESTAMP,
#         DuckDB.DUCKDB_TYPE_INTERVAL,
#         DuckDB.DUCKDB_TYPE_INTERVAL
#     )
#     datetime_types = (Date, Time, DateTime, Period, Dates.CompoundPeriod)
#     for (t, type_id) in zip(datetime_types, datetime_duck_types)
#         lt = DuckDB.create_logical_type(t)
#         @test lt.handle != C_NULL
#         @test DuckDB.get_type_id(lt) == type_id
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
#     Base.GC.gc()
#     @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_STRUCT
#     @test DuckDB.is_complex_type(lt) == true
#     @test DuckDB.get_struct_child_count(lt) == 5
#     @test DuckDB.get_struct_child_name(lt, 1) == "a"
#     @test DuckDB.get_struct_child_name(lt, 2) == "b"
#     @test DuckDB.get_struct_child_name(lt, 3) == "c"
#     @test DuckDB.get_struct_child_name(lt, 4) == "d"
#     @test DuckDB.get_struct_child_name(lt, 5) == "e"
#     @test length(lt.inner_types) == length(keys(nt))
#     @test DuckDB.get_type_id(lt.inner_types[1]) == DuckDB.DuckDB.DUCKDB_TYPE_BIGINT
#     @test DuckDB.get_type_id(lt.inner_types[2]) == DuckDB.DUCKDB_TYPE_DOUBLE
#     @test DuckDB.get_type_id(lt.inner_types[3]) == DuckDB.DUCKDB_TYPE_VARCHAR
#     @test DuckDB.get_type_id(lt.inner_types[4]) == DuckDB.DUCKDB_TYPE_ARRAY
#     @test DuckDB.get_type_id(lt.inner_types[5]) == DuckDB.DUCKDB_TYPE_LIST

#     @test DuckDB.alias(lt) == ""
#     DuckDB.set_alias!(lt, "ComplexStruct")
#     @test DuckDB.alias(lt) == "ComplexStruct"
#     Base.GC.gc()



#     ct = MyStruct(1, 2.0, (1, 2, 3))
#     lt = DuckDB.create_logical_type(typeof(ct))
#     @test lt.handle != C_NULL
#     Base.GC.gc()
#     @test DuckDB.get_type_id(lt) == DuckDB.DUCKDB_TYPE_STRUCT
#     @test DuckDB.is_complex_type(lt) == true
#     @test DuckDB.get_struct_child_count(lt) == 3
#     @test DuckDB.get_struct_child_name(lt, 1) == "a"
#     @test DuckDB.get_struct_child_name(lt, 2) == "b"
#     @test DuckDB.get_struct_child_name(lt, 3) == "c"

#     @test length(lt.inner_types) == 3
#     @test DuckDB.get_type_id(lt.inner_types[1]) == DuckDB.DUCKDB_TYPE_BIGINT
#     @test DuckDB.get_type_id(lt.inner_types[2]) == DuckDB.DUCKDB_TYPE_DOUBLE
#     @test DuckDB.get_type_id(lt.inner_types[3]) == DuckDB.DUCKDB_TYPE_ARRAY

#     @test DuckDB.alias(lt) == "JLMyStruct"
# end





# @testset failfast = true "Conversions Julia to Internal to Julia: Numerical" begin
#     # Integers
#     N = 8
#     int_types = (
#         Bool,
#         Int8,
#         Int16,
#         Int32,
#         Int64,
#         Int128,
#         UInt8,
#         UInt16,
#         UInt32,
#         UInt64,
#         UInt128,
#         Float32,
#         Float64,
#         Date,
#         Time,
#         DateTime,
#         #Period,
#         Dates.CompoundPeriod
#     )
#     types = [DuckDB.create_logical_type(t) for t in int_types]
#     chunk = DuckDB.DataChunk(types)
#     @test DuckDB.get_column_count(chunk) == length(int_types)
#     @test DuckDB.get_size(chunk) == 0
#     DuckDB.set_size(chunk, N)
#     @test DuckDB.get_size(chunk) == N

#     # Check if writing is fine
#     for (i, T) in enumerate(int_types)
#         logical_type = types[i]
#         vec = DuckDB.get_vector(chunk, i)
#         logical_type_chunk = DuckDB.get_logical_type(vec)
#         @test DuckDB.get_type_id(logical_type_chunk) == DuckDB.get_type_id(logical_type)
#         internal_type = DuckDB.duckdb_type_to_internal_type(DuckDB.get_type_id(logical_type))
#         X = DuckDB.get_array(vec, internal_type, N)
#         @test eltype(X) == internal_type
#         for j in 1:N
#             y = _random_element(T, 1)
#             X[j] = y  # write
#             x = X[j]  # read
#             # check conversions in both directions
#             @test x == convert(internal_type, y)
#             @test convert(T, x) == y
#         end

#         reader = DuckDB.VecReader(vec, logical_type, T, N)
#         writer = DuckDB.VecWriter(vec, logical_type, T, N)

#         for j in 1:N
#             y = _random_element(T, 1)
#             writer[j] = y
#             @test reader[j] == y
#         end
#     end


#     # Performance Test
#     # THIS SEGFAULTS if N > 2048 ?
#     N = 2048
#     chunk = DuckDB.DataChunk(types)
#     Base.GC.@preserve chunk begin
#         DuckDB.set_size(chunk, N)
#         @test DuckDB.get_size(chunk) == N
#         perf_dict = Dict{DataType, Vector{Float64}}()

#         for (i, T) in enumerate(int_types)
#             logical_type = types[i]
#             vec = DuckDB.get_vector(chunk, i)

#             reader = DuckDB.VecReader(vec, logical_type, T, N)
#             writer = DuckDB.VecWriter(vec, logical_type, T, N)

#             out = Array{Union{Missing, T}}(undef, N)

#             for j in 1:N
#                 writer[j] = _random_element(T, 1)
#             end

#             t_write, t_read = 0.0, 0.0
#             t_write = @elapsed for j in 1:N
#                 y = _random_element(T, 1)
#                 writer[j] = y
#             end

#             t_read = @elapsed for j in 1:N
#                 out[j] = reader[j]
#             end

#             data = DuckDB.ColumnConversionData((chunk,), i, logical_type, nothing)
#             t_baseline = @elapsed DuckDB.convert_column(data)

#             push!(perf_dict, T => [t_read, t_write, t_baseline])
#         end
#         println(DuckDB.get_size(chunk))
#     end

#     for t in int_types
#         k = t
#         v = perf_dict[k]
#         println("Type: ", k, "\t Read: \t\t", v[1], "\t\t Write: \t\t", v[2], "\t\t Baseline: \t\t", v[3])
#     end
# end

# @testset "Conversions Julia to Internal to Julia: Strings & Blobs" begin
#     N = 8
#     T = String


#     logical_type = DuckDB.create_logical_type(T)
#     chunk = DuckDB.DataChunk([logical_type])
#     @test DuckDB.get_size(chunk) == 0
#     DuckDB.set_size(chunk, N)
#     @test DuckDB.get_size(chunk) == N

#     internal_type = DuckDB.duckdb_type_to_internal_type(DuckDB.get_type_id(logical_type))
#     vec = DuckDB.get_vector(chunk, 1)
#     raw_data_array = DuckDB.get_array(vec, internal_type, N)

#     # Check if the internal array is the string union type
#     @test eltype(raw_data_array) == DuckDB.duckdb_string_t

#     for i in 1:N
#         s = random_string(rand(1:20))

#         DuckDB.assign_string_element(vec, i, s)
#         # Check if the string is correctly written
#         s_out = raw_data_array[i]
#         @test s == convert(String, s_out)
#     end


#     N = 2048
#     chunk = DuckDB.DataChunk([logical_type])
#     DuckDB.set_size(chunk, N)
#     reader = DuckDB.VecReader(vec, logical_type, T, N)
#     writer = DuckDB.VecWriter(vec, logical_type, T, N)

#     # check
#     for i in 1:N
#         s = random_string(rand(1:20))
#         writer[i] = s
#         @test reader[i] == s
#     end

#     in = [random_string(rand(1:20)) for i in 1:N]

#     T_out = DuckDB.julia_eltype(reader)
#     @show T_out
#     out = Array{T_out}(undef, N)
#     t_write = @elapsed for i in 1:N
#         writer[i] = in[i]
#     end

#     t_read = @elapsed for i in 1:N
#         out[i] = reader[i]
#     end

#     @test isequal(in, out)

#     t_baseline = 0.0
#     # GC.@preserve chunk logical_type begin
#     #     data = DuckDB.ColumnConversionData((chunk,), 1, logical_type, nothing)
#     #     t_baseline = @elapsed DuckDB.convert_column(data)
#     # end
#     println("String performance test done")
#     println("Type: ", T, "\t Read: \t\t", t_read, "\t\t Write: \t\t", t_write, "\t\t Baseline: \t\t", t_baseline)

# end

# @testset "Bug Report" begin
# using DuckDB


# N_list_entries = 100
# #N_list_entries = 2048
# N_entries_per_vec = 20
# X = [rand(1:20, rand(1:N_entries_per_vec)) for _ in 1:N_list_entries]
# T = typeof(X[1])
# @show T
# logical_type = DuckDB.create_logical_type(T)
# output_type = DuckDB.duckdb_type_to_julia_type(logical_type)
# chunk_ptr = DuckDB.duckdb_create_data_chunk([logical_type.handle], 1)
# chunk = DuckDB.DataChunk(chunk_ptr, true)

# DuckDB.set_size(chunk, N_list_entries) # N_list_entries lists

# GC.@preserve chunk begin
#     vec_i = DuckDB.get_vector(chunk, 1)
#     @test DuckDB.get_type_id(DuckDB.get_logical_type(vec_i)) == DuckDB.DUCKDB_TYPE_LIST

#     child_vec = DuckDB.list_child(vec_i)
#     child_vec_size = DuckDB.list_size(vec_i)
#     child_vec_type = DuckDB.LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle))
#     child_vec_type_id = DuckDB.get_type_id(child_vec_type)
#     internal_type = DuckDB.duckdb_type_to_internal_type(child_vec_type_id)
#     list_entries = DuckDB.get_array(vec_i, DuckDB.duckdb_list_entry_t, N_list_entries)

#     local offset = 0
#     for i in 1:N_list_entries
#         println("i: ", i)
#         xi = X[i]
#         N = length(xi)
#         N_total = N + offset

#         # UNCOMMENT THIS LINE AND IT NO LONGER SEGFAULTS
#         # DuckDB.duckdb_list_vector_reserve(vec_i.handle, N_total)
#         DuckDB.duckdb_list_vector_set_size(vec_i.handle, N_total)


#         child_data = DuckDB.get_array(child_vec, internal_type, N_total)

#         entry = DuckDB.duckdb_list_entry_t(offset, N)
#         list_entries[i] = entry
#         k = 1
#         for j in offset+1:N_total
#             child_data[j] = xi[k]
#             k+=1
#         end
#         offset += N
#     end
# end
# version_ptr = DuckDB.duckdb_library_version()
# version = unsafe_string(version_ptr)
# print("DUCKDB Version: ", version)

# end

# @testset "Conversions Julia to Internal to Julia: Lists" begin
#     N_list_entries = 2048
#     N_entries_per_vec = 20
#     #X = [rand(1:20, rand(1:N_entries_per_vec)) for _ in 1:N_list_entries]
#     X = [random_string(rand(1:20)) for i in 1:N_list_entries]
#     T = typeof(X[1])

#     logical_type = DuckDB.create_logical_type(T)
#     output_type = DuckDB.duckdb_type_to_julia_type(logical_type)
#     chunk = DuckDB.DataChunk([logical_type])
#     DuckDB.set_size(chunk, N_list_entries)
#     GC.@preserve chunk begin
#         vec_i = DuckDB.get_vector(chunk, 1)

#         writer = DuckDB.VecWriter(vec_i, logical_type, T, N_list_entries)
#         sizehint!(writer, sum(length(xi) for xi in X))
#         t_write = @elapsed for i in 1:N_list_entries
#             writer[i] = X[i] # write
#         end

#         reader = DuckDB.VecReader(vec_i, logical_type, T, N_list_entries)
#         T_out = DuckDB.julia_eltype(reader)
#         out = Vector{T_out}(undef, N_list_entries)
#         t_read = @elapsed for i in 1:N_list_entries
#             out[i] = reader[i] # read
#         end

#         T_out = DuckDB.julia_eltype(reader)
#         Y = [reader[i] for i in 1:N_list_entries]
#         @test isequal(Y, X)


#         t_baseline = 0.0
#         data = DuckDB.ColumnConversionData((chunk,), 1, logical_type, nothing)
#         t_baseline = @elapsed DuckDB.convert_column(data)

#         println("List performance test done")
#         println("Type: ", T, "\t Read: \t\t", t_read, "\t\t Write: \t\t", t_write, "\t\t Baseline: \t\t", t_baseline)
#     end
#     Base.GC.gc()
#     N = DuckDB.get_size(chunk) # 8 lists
# end

# @testset "Conversions Julia to Internal to Julia: Nested Lists" begin
#     N_list_entries = 2048
#     N_entries_per_vec = 20
#     #X = [rand(1:20, rand(1:N_entries_per_vec)) for _ in 1:N_list_entries]
#     X = [[[random_string(rand(1:20)) for j in 1:3] for _ in 1:3] for i in 1:N_list_entries]
#     T = typeof(X[1])

#     logical_type = DuckDB.create_logical_type(T)
#     output_type = DuckDB.duckdb_type_to_julia_type(logical_type)
#     chunk = DuckDB.DataChunk([logical_type])
#     DuckDB.set_size(chunk, N_list_entries)
#     GC.@preserve chunk begin
#         vec_i = DuckDB.get_vector(chunk, 1)

#         writer = DuckDB.VecWriter(vec_i, logical_type, T, N_list_entries)
#         sizehint!(writer, sum(length(xi) for xi in X))
#         t_write = @elapsed for i in 1:N_list_entries
#             writer[i] = X[i] # write
#         end

#         reader = DuckDB.VecReader(vec_i, logical_type, T, N_list_entries)
#         T_out = DuckDB.julia_eltype(reader)
#         out = Vector{T_out}(undef, N_list_entries)
#         t_read = @elapsed for i in 1:N_list_entries
#             out[i] = reader[i] # read
#         end

#         T_out = DuckDB.julia_eltype(reader)
#         Y = [reader[i] for i in 1:N_list_entries]
#         @test isequal(Y, X)


#         t_baseline = 0.0
#         data = DuckDB.ColumnConversionData((chunk,), 1, logical_type, nothing)
#         t_baseline = @elapsed DuckDB.convert_column(data)

#         println("Nested List performance test done")
#         println("Type: ", T, "\t Read: \t\t", t_read, "\t\t Write: \t\t", t_write, "\t\t Baseline: \t\t", t_baseline)
#     end
#     Base.GC.gc()
#     N = DuckDB.get_size(chunk) # 8 lists
#     @show N
# end

# @testset "Conversions Julia to Internal to Julia: Structs" begin

# end

# @testset "" begin
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


# using PrettyTables
# @testset "Conversions Julia to Internal to Julia: Combined" begin
#     N = 2048

#     types = [
#         Bool,
#         Int8,
#         Int16,
#         Int32,
#         Int64,
#         Int128,
#         UInt8,
#         UInt16,
#         UInt32,
#         UInt64,
#         UInt128,
#         Float32,
#         Float64,
#         String,
#         Date,
#         Time,
#         DateTime,
#         Dates.CompoundPeriod,
#         NTuple{10, Int},
#         #Base.CodeUnits{UInt8, String},
#         Vector{Int},
#         MyStruct
#     ]
#     t_reads = Float64[]
#     t_writes = Float64[]
#     t_baselines = Float64[]
#     for T in types
#         X = [_random_element(T, 5) for i in 1:N]
#         julia_type_in = eltype(X)
#         if T == Vector{UInt8}
#             logical_type = DuckDB.create_logical_type(UInt8)
#         else
#             logical_type = DuckDB.create_logical_type(julia_type_in)
#         end
#         chunk = DuckDB.DataChunk([logical_type])
#         DuckDB.set_size(chunk, N)
#         Base.GC.@preserve chunk begin
#             vec = DuckDB.get_vector(chunk, 1)


#             writer = DuckDB.VecWriter(vec, logical_type, julia_type_in, N)
#             sizehint!(writer, N)
#             t_write = @elapsed for i in 1:N
#                 writer[i] = X[i]
#             end

#             reader = DuckDB.VecReader(vec, logical_type, julia_type_in, N)
#             julia_type_out = DuckDB.julia_eltype(reader)
#             out = Vector{julia_type_out}(undef, N)
#             t_read = @elapsed for i in 1:N
#                 out[i] = reader[i]
#             end

#             if T <: Period || T <: Dates.CompoundPeriod
#                 out = [Dates.canonicalize(x) for x in out]
#             end

#             @test isequal(out, X)
#             t_baseline = 0.0
#             if !(T <: Tuple) && !(T <: MyStruct)
#                 # Tuple not supported
#                 try
#                     data = DuckDB.ColumnConversionData((chunk,), 1, logical_type, nothing)
#                     t_baseline = @elapsed DuckDB.convert_column(data)
#                 catch e
#                     println("Skipped baseline for type ", T, ", error:", e)
#                 end
#             end
#             push!(t_reads, t_read)
#             push!(t_writes, t_write)
#             push!(t_baselines, t_baseline)

#             #println("Type: ", T, "\t Read: \t\t", t_read, "\t\t Write: \t\t", t_write, "\t\t Baseline: \t\t", t_baseline)
#         end
#     end

#     df = DataFrame(Type = types, Read = t_reads, Write = t_writes, Baseline = t_baselines)
#     PrettyTables.pretty_table(df)
# end


# @testset "Conversions Julia to Internal to Julia: Combined Multi Chunk" begin
#     N = 2048

#     types = [
#         Bool,
#         Int8,
#         Int16,
#         Int32,
#         Int64,
#         Int128,
#         UInt8,
#         UInt16,
#         UInt32,
#         UInt64,
#         UInt128,
#         Float32,
#         Float64,
#         String,
#         Date,
#         Time,
#         DateTime,
#         Dates.CompoundPeriod,
#         NTuple{10, Int},
#         Vector{Int},
#         MyStruct
#     ]
#     t_reads = Float64[]
#     t_writes = Float64[]
#     t_baselines = Float64[]
#     for T in types
#         X = [_random_element(T, 5) for i in 1:N]
#         julia_type_in = eltype(X)
#         logical_type = DuckDB.create_logical_type(julia_type_in)

#         chunks = [DuckDB.DataChunk([logical_type]) for _ in 1:10] # 10 chunks
#         DuckDB.set_size.(chunks, N)

#         Base.GC.@preserve chunks begin

#             t_write = @elapsed for chunk in chunks
#                 vec = DuckDB.get_vector(chunk, 1)
#                 writer = DuckDB.VecWriter(vec, logical_type, julia_type_in, N)
#                 sizehint!(writer, N)
#                 for i in 1:N
#                     writer[i] = X[i]
#                 end
#             end

#             t_read = @elapsed for chunk in chunks
#                 vec = DuckDB.get_vector(chunk, 1)
#                 reader = DuckDB.VecReader(vec, logical_type, julia_type_in, N)
#                 julia_type_out = DuckDB.julia_eltype(reader)
#                 out = Vector{julia_type_out}(undef, N)
#                 for i in 1:N
#                     out[i] = reader[i]
#                 end
#             end

#             if T <: Period || T <: Dates.CompoundPeriod
#                 out = [Dates.canonicalize(x) for x in out]
#             end

#             @test isequal(out, X)
#             t_baseline = 0.0
#             if !(T <: Tuple) && !(T <: MyStruct)
#                 # Tuple not supported
#                 try
#                     data = DuckDB.ColumnConversionData(chunks, 1, logical_type, nothing)
#                     t_baseline = @elapsed DuckDB.convert_column(data)
#                 catch e
#                     println("Skipped baseline for type ", T, ", error:", e)
#                 end
#             end
#             push!(t_reads, t_read)
#             push!(t_writes, t_write)
#             push!(t_baselines, t_baseline)

#             #println("Type: ", T, "\t Read: \t\t", t_read, "\t\t Write: \t\t", t_write, "\t\t Baseline: \t\t", t_baseline)
#         end
#     end

#     df = DataFrame(Type = types, Read = t_reads, Write = t_writes, Baseline = t_baselines)
#     PrettyTables.pretty_table(df)
# end


@testset "New ScalarFunction" begin

    my_scalar_function = function (x, y)
        return x + y
    end

    input_types = [Float64, Float64]
    output_type = Float64

    f1 = DuckDB.@create_scalar_function my_scalar_function1(x::Float64, y::Float64)::Float64 my_scalar_function
    f2 = DuckDB._create_scalar_function_new("my_scalar_function2", my_scalar_function, input_types, output_type)

    con = DBInterface.connect(DuckDB.DB, ":memory:")
    DuckDB.register_scalar_function(con, f1)
    DuckDB.register_scalar_function(con, f2)

    DuckDB.execute(con, "CREATE TABLE test_table AS SELECT random() as x, random() as y FROM range(1,1_000_000);")
    df1 = DuckDB.execute(con, "SELECT my_scalar_function1(x, y) as z FROM test_table;") |> DataFrame
    df2 = DuckDB.execute(con, "SELECT my_scalar_function2(x, y) as z FROM test_table;") |> DataFrame

    @test isequal(df1.z, df2.z)

    t1 = @elapsed DuckDB.execute(con, "SELECT my_scalar_function1(x, y) FROM test_table;")
    t2 = @elapsed DuckDB.execute(con, "SELECT my_scalar_function2(x, y) FROM test_table;")

    @show t1 t2
end



@testset "New ScalarFunction Structs" begin

    my_scalar_function = function (x, y)
        return x.a + y.a + x.b + y.b
    end
    
    input_types = [NamedTuple{(:a,:b), Tuple{Float64, Float64}}, NamedTuple{(:a,:b), Tuple{Float64, Float64}}]
    output_type = Float64
    f2 = DuckDB._create_scalar_function_new("my_scalar_function2", my_scalar_function, input_types, output_type)

    con = DBInterface.connect(DuckDB.DB, ":memory:")
    DuckDB.register_scalar_function(con, f2)

    DuckDB.execute(con, """CREATE TABLE test_table AS 
        SELECT 
            {'a': random(), 'b': random()} AS x,
            {'a': random(), 'b': random()} AS y
        FROM range(1,1_000_000);""")
    

    DuckDB.execute(con, "SELECT my_scalar_function2(x, y) FROM test_table;")

    #t1 = @elapsed DuckDB.execute(con, "SELECT my_scalar_function1(x, y) FROM test_table;")
    t2 = @elapsed DuckDB.execute(con, "SELECT my_scalar_function2(x, y) FROM test_table;")

    @show t2
end