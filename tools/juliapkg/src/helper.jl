
function esc_id end

esc_id(x::AbstractString) = "\"" * replace(x, "\"" => "\"\"") * "\""
esc_id(X::AbstractVector{S}) where {S <: AbstractString} = join(map(esc_id, X), ',')


"""Typestable way to determine the number of elements in a n-tuple"""
_tuple_count(::Type{NTuple{N, T}}) where {N, T} = N
_tuple_count(::NTuple{N, T}) where {N, T} = N

"""Typestable way to determine the type of elements in a n-tuple"""
_tuple_type(::Type{NTuple{N, T}}) where {N, T} = T
_tuple_type(::NTuple{N, T}) where {N, T} = T

"""Reduce union type into a tuple of types"""
_union_types(x::Union) = (x.a, _union_types(x.b)...)
_union_types(x::Type) = (x,)



has_missing(::Vector{T}) where {T}                 = false
has_missing(::Vector{Union{Missing, T}}) where {T} = true
base_type(::Type{T}) where {T}                     = T
base_type(::Type{Union{Missing, T}}) where {T}     = T

function flatten_and_skip_missing(v, N_total = -1)
    if N_total == -1
        N_total = sum(length(vi) for vi in v if !ismissing(vi); init=0)
    end

    T = eltype(base_type(eltype(v)))
    v_out = Vector{T}(undef, N_total)
    k = 1
    for vi in v
        if ismissing(vi)
            continue
        end
        for vii in vi
            v_out[k] = vii
            k += 1
        end
    end
    return v_out
end