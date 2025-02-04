using Dates, DataFrames, StaticArrays, UUIDs, FixedPointDecimals

mutable struct MyStruct
    a::Int
    b::Float64
    c::NTuple{3, Int}
end

# Define Equality with NamedTuple -> DuckDB cannot recover the struct type from the NamedTuple
Base.:(==)(m::MyStruct, n::MyStruct) = m.a == n.a && m.b == n.b && m.c == n.c
Base.:(==)(m::MyStruct, n::NamedTuple{(:a, :b, :c), Tuple{Int, Float64, NTuple{3, Int}}}) =
    m.a == n.a && m.b == n.b && m.c == n.c
Base.:(==)(m::NamedTuple, n::MyStruct) = n == m

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
random_uuid() = UUID(rand(UInt128))

_random_element(::Type{Union{Missing, T}}, size) where {T} = rand([missing, _random_element(T, size)])
_random_element(::Type{T}, size) where {T} = rand(T)
_random_element(::Type{String}, size) = random_string(size)
_random_element(::Type{Vector{UInt8}}, size) = random_blob(size)
_random_element(::Type{Dates.Time}, size) = random_time()
_random_element(::Type{Dates.Date}, size) = random_date()
_random_element(::Type{Dates.DateTime}, size) = random_datetime()
_random_element(::Type{Dates.Period}, size) = random_period()
_random_element(::Type{UUID}, size) = random_uuid()
_random_element(::Type{Dates.CompoundPeriod}, size) = random_compound_period()
_random_element(::Type{NTuple{N, T}}, size) where {N, T} = Tuple(_random_element(T, size) for _ in 1:N)
_random_element(::Type{Vector{T}}, size) where {T} = Vector{T}([_random_element(T, size) for _ in 1:size])
_random_element(::Type{MyStruct}, size) = MyStruct(rand(Int), rand(), _random_element(NTuple{3, Int}, size))
_random_element(::Type{Dict{K, V}}, size) where {K, V} =
    Dict(s => _random_element(V, size) for s in unique([_random_element(K, size) for j in 1:size]))


_random_element_union() = rand([rand(Int), rand(Float64), rand(Bool), random_string(10)])


function _random_element(::Type{NamedTuple{names, T}}, size)::NamedTuple{names, T} where {names, T <: Tuple}
    return NamedTuple{names, T}(tuple((_random_element(Ti, size) for Ti in T.types)...))
end
