"""
DuckDB vector
"""
struct Vec
    handle::duckdb_vector

    function Vec(handle::duckdb_vector)
        result = new(handle)
        return result
    end
end

function get_logical_type(vector::Vec)::LogicalType
    return LogicalType(duckdb_vector_get_column_type(vector.handle))
end

function get_array(vector::Vec, ::Type{T}, size = VECTOR_SIZE) where {T}
    raw_ptr = duckdb_vector_get_data(vector.handle)
    ptr = Base.unsafe_convert(Ptr{T}, raw_ptr)
    return unsafe_wrap(Vector{T}, ptr, size, own = false)
end

function get_validity(vector::Vec, size = VECTOR_SIZE)::ValidityMask
    duckdb_vector_ensure_validity_writable(vector.handle)
    validity_ptr = duckdb_vector_get_validity(vector.handle)
    ptr = Base.unsafe_convert(Ptr{UInt64}, validity_ptr)
    size_words = div(size, BITS_PER_VALUE, RoundUp)
    validity_vector = unsafe_wrap(Vector{UInt64}, ptr, size_words, own = false)
    only_valid = _all_valid(validity_vector)
    return ValidityMask(only_valid, true, validity_vector)
end

function get_readonly_validity(vector::Vec, size = VECTOR_SIZE)::ValidityMask
    validity_ptr = duckdb_vector_get_validity(vector.handle)
    if validity_ptr == C_NULL
        return ValidityMask(true, false, Vector{UInt64}())
    end
    ptr = Base.unsafe_convert(Ptr{UInt64}, validity_ptr)
    size_words = div(size, BITS_PER_VALUE, RoundUp)
    validity_vector = unsafe_wrap(Vector{UInt64}, ptr, size_words, own = false)
    _only_valid = _all_valid(validity_vector)
    return ValidityMask(_only_valid, false, validity_vector)
end


function all_valid(vector::Vec, size = VECTOR_SIZE)::Bool
    mask = get_readonly_validity(vector, size)
    return all_valid(mask)
end

function array_child(vector::Vec)::Vec
    return Vec(duckdb_array_vector_get_child(vector.handle))
end

function list_child(vector::Vec)::Vec
    return Vec(duckdb_list_vector_get_child(vector.handle))
end

function list_size(vector::Vec)::UInt64
    return duckdb_list_vector_get_size(vector.handle)
end

function struct_child(vector::Vec, index)::Vec
    return Vec(duckdb_struct_vector_get_child(vector.handle, index))
end

function union_member(vector::Vec, index::UInt64)::Vec
    return Vec(duckdb_union_vector_get_member(vector.handle, index))
end

function assign_string_element(vector::Vec, index, str::AbstractString)
    return duckdb_vector_assign_string_element_len(vector.handle, index, str, sizeof(str))
end

function assign_string_element(vector::Vec, index, str::AbstractVector{UInt8})
    return duckdb_vector_assign_string_element_len(vector.handle, index, str, sizeof(str))
end

function assign_blob_element(vector::Vec, index, blob::AbstractVector{UInt8})
    # BLOBs are just strings
    return duckdb_vector_assign_string_element_len(vector.handle, index, blob, sizeof(blob))
end




# %% --------------------------------------------------------
#        Reader Interface V2
#------------------------------------------------------------

# mutable struct VecReader2{T,F}
#     logical_type::LogicalType
#     julia_type::Type{T}
#     getindex_func::F
# end

# struct VecReaderInitData{D}
#     vec::Vec
#     validity_mask::ValidityMask
#     data::D
# end

# function _init_reader(reader::VecReader2, vec::Vec)
#     # DO NOT USE THIS FUNCTION
# end

# function _getindex(reader::VecReader2, init_data::VecReaderInitData, index)
#     if !isvalid(init_data.validity_mask, index)
#         return missing
#     end
#     reader.getindex_func(init_data, index)
# end

# %% --------------------------------------------------------
#        Reader Interface
#------------------------------------------------------------

mutable struct VecReader{T, D, F <: Function}
    vec::Vec
    N::Int
    logical_type::LogicalType
    julia_type::Type{T}
    getindex_func::F # index Function: getindex(reader, index) -> T
    validity_mask::ValidityMask
    data::D
end

Base.length(reader::VecReader) = reader.N
Base.iterate(reader::VecReader, state = 1) = state > length(reader) ? nothing : (reader[state], state + 1)
Base.eltype(reader::VecReader) = julia_eltype(reader)
Base.HasEltype(::Type{VecReader}) = true
Base.HasLength(::Type{VecReader}) = true

all_valid(reader::VecReader) = all_valid(reader.validity_mask)


function Base.getindex(reader::VecReader{T}, index::Integer)::Union{Missing, T} where {T}
    if all_valid(reader.validity_mask)
        return reader.getindex_func(reader.data, index)
    else
        if isvalid(reader.validity_mask, index)
            return convert(T, reader.getindex_func(reader.data, index))
        else
            return missing
        end
    end
end

function _fill!(buf::BUF, reader::VecReader{T}) where {T, BUF <: AbstractArray{<:T}}
    i = 1
    for ix in eachindex(buf)
        buf[ix] = reader.getindex_func(reader.data, i)
        i += 1
    end
    return buf
end

function VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T}
    # This is the fallback implementation for all types that are not explicitly implemented.
    type_id = get_type_id(logical_type)
    if type_id == DUCKDB_TYPE_LIST
        return _create_vecreader_list(vec, logical_type, T, N)
    elseif type_id == DUCKDB_TYPE_ARRAY
        return _create_vecreader_array(vec, logical_type, T, N)
    elseif type_id == DUCKDB_TYPE_MAP
        return _create_vecreader_dict(vec, logical_type, T, N)
    elseif type_id == DUCKDB_TYPE_STRUCT
        return _create_vecreader_struct(vec, logical_type, T, N)
    elseif type_id == DUCKDB_TYPE_MAP
        return _create_vecreader_dict(vec, logical_type, T, N)
    elseif type_id == DUCKDB_TYPE_UNION
        return _create_vecreader_union(vec, logical_type, T, N)
    elseif is_complex_type(logical_type)
        throw(NotImplementedException("Complex types are not supported"))
    elseif type_id in INTERNAL_TYPE_MAP
        # Check the internal typemap
        internal_type = INTERNAL_TYPE_MAP[type_id]
        if internal_type !== nothing
            return _create_vecreader_simple(vec, logical_type, T, N)
        end
        return throw(NotImplementedException(string("VecReader not implemented for type ", T)))
    else
        return throw(NotImplementedException(string("VecReader not implemented for type ", T)))
    end
end



VecReader(vec::Vec, logical_type::LogicalType, ::Type{Union{Missing, T}}, N) where {T} =
    VecReader(vec, logical_type, T, N)
VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Integer} =
    _create_vecreader_simple(vec, logical_type, T, N)
VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: UUID} =
    _create_vecreader_simple(vec, logical_type, T, N)
VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: AbstractFloat} =
    _create_vecreader_simple(vec, logical_type, T, N)
VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: String} =
    _create_vecreader_string(vec, logical_type, T, N)
VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Date} =
    _create_vecreader_simple(vec, logical_type, T, N)
VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Time} =
    _create_vecreader_simple(vec, logical_type, T, N)
VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: DateTime} =
    _create_vecreader_simple(vec, logical_type, T, N)
VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Union{Period, Dates.CompoundPeriod}} =
    _create_vecreader_simple(vec, logical_type, T, N)


function VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: AbstractArray}
    return _create_vecreader_list(vec, logical_type, T, N)
end

function VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: NamedTuple}
    return _create_vecreader_struct(vec, logical_type, T, N)
end

function VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: AbstractDict}
    return _create_vecreader_dict(vec, logical_type, T, N)
end

function VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: Tuple}
    return _create_vecreader_array(vec, logical_type, T, N)
end


function julia_eltype(r::VecReader)
    #T = duckdb_type_to_julia_type(r.logical_type)
    T = r.julia_type
    if all_valid(r.validity_mask)
        return T
    else
        return Union{Missing, T}
    end
end


function _create_vecreader_simple(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    type_id = get_type_id(logical_type)
    #internal_type = duckdb_type_to_internal_type(type_id)
    internal_type_static = julia_to_duck_type(T)
    data = get_array(vec, internal_type_static, N), T
    getindex_func = _getindex_simple
    validity_mask = get_readonly_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, T, getindex_func, validity_mask, data)
end

function _getindex_simple(data, index)
    arr, _ = data
    return arr[index]
end

function _create_vecreader_string(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: AbstractString}
    data_inline = get_array(vec, duckdb_string_t, N)
    data_ptr = get_array(vec, duckdb_string_t_ptr, N)
    validity_mask = get_readonly_validity(vec, N)
    getindex_func = _getindex_string
    data = (data_inline, data_ptr)
    return VecReader(vec, Int(N), logical_type, T, getindex_func, validity_mask, data)
end

function _getindex_string(data::Tuple{Vector{duckdb_string_t}, Vector{duckdb_string_t_ptr}}, index)::String
    data_inline, data_ptr = data
    s1 = data_inline[index]
    if s1.length <= STRING_INLINE_LENGTH
        return convert(String, s1)
    else
        return convert(String, data_ptr[index])
    end
end

function _create_vecreader_list(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    child_vec = list_child(vec)
    child_type = get_list_child_type(logical_type)
    #julia_type = duckdb_type_to_julia_type(child_type)
    julia_type_static = eltype(T)
    list_vec = get_array(vec, duckdb_list_entry_t, N)
    N_total = Int(list_size(vec))
    child_reader = VecReader(child_vec, child_type, julia_type_static, N_total)
    data = (list_vec, child_reader) # TODO make struct
    validity_mask = get_readonly_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, T, _getindex_list, validity_mask, data)
end

function _getindex_list(data::Tuple{Vector{duckdb_list_entry_t}, VecReader}, index)
    list_vec, child_reader = data
    list_entry::duckdb_list_entry_t = list_vec[index]
    N = list_entry.length
    offset = list_entry.offset + 1 # 1-based indexing
    return [child_reader[i] for i in offset:(offset + N - 1)]
end

function _create_vecreader_array(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    # Get the child vector
    # The resulting vector has the size of the parent vector multiplied by the array size.
    child_vec = array_child(vec)
    child_type = get_array_child_type(logical_type)
    child_julia_type = duckdb_type_to_julia_type(child_type)
    child_julia_type_static = eltype(T)
    parent_type = T # keep the parent type
    N_array = get_array_child_size(logical_type)
    N_child = N * N_array
    child_reader = VecReader(child_vec, child_type, child_julia_type_static, N_child)
    data = (N_array, child_reader, parent_type) # TODO make struct
    validity_mask = get_readonly_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, T, _getindex_array, validity_mask, data)
end

function _getindex_array(data, index)
    N_array, child_reader, T_parent = data
    offset = (index - 1) * N_array + 1
    return T_parent(child_reader[i] for i in offset:(offset + N_array - 1))
end

function _create_vecreader_struct(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    julia_type = duckdb_type_to_julia_type(logical_type)
    K = get_struct_child_count(logical_type)

    # TODO use propertynames?
    names = fieldnames(T)
    types_tuple = fieldtypes(T)
    Nf = length(names)

    if K != Nf
        throw(ArgumentError(string("Internal Conversion Error: Struct field count mismatch: ", K, " != ", Nf)))
    end

    for i in 1:K
        s_name = get_struct_child_name(logical_type, i)
        if string(names[i]) != s_name
            throw(
                ArgumentError(
                    string("Internal Conversion Error: Struct field name mismatch: ", names[i], " != ", s_name)
                )
            )
        end
    end

    for i in 1:K
        s_type = get_struct_child_type(logical_type, i)
        s_type_id = get_type_id(s_type)
        lt::LogicalType = create_logical_type(types_tuple[i])
        lt_id = get_type_id(lt)
        if lt_id != s_type_id
            throw(
                ArgumentError(
                    string("Internal Conversion Error: Struct field type mismatch: ", lt_id, " != ", s_type_id)
                )
            )
        end
    end

    readers = Tuple(
        VecReader(struct_child(vec, k), get_struct_child_type(logical_type, k), fieldtype(T, name), N) for
        (k, name) in enumerate(names)
    )

    data = NamedTuple{names}(readers)
    validity_mask = get_readonly_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, T, _get_index_struct, validity_mask, data)
end

function _get_index_struct(data::NamedTuple{names}, index) where {names}
    return NamedTuple{names}(getindex(reader, index) for reader in data)
end

function _create_vecreader_dict(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {K, V, T <: AbstractDict{K, V}}
    # Internally map vectors are stored as a LIST[STRUCT(key KEY_TYPE, value VALUE_TYPE)].
    child_vec = list_child(vec)
    N_child = list_size(vec)
    key_type = get_map_key_type(logical_type)
    value_type = get_map_value_type(logical_type)
    child_logical_type = LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle)) # struct type

    # TODO REMOVE
    M = get_struct_child_count(child_logical_type)
    for i in 1:M
        s_name = get_struct_child_name(child_logical_type, i)
        s_type = get_type_id(get_struct_child_type(child_logical_type, i))
    end

    lt_key = create_logical_type(K)
    lt_value = create_logical_type(V)
    @assert get_type_id(lt_key) == get_type_id(key_type)
    @assert get_type_id(lt_value) == get_type_id(value_type)

    julia_type_inner = NamedTuple{(:key, :value), Tuple{K, V}}
    validity_mask = get_readonly_validity(vec, N)

    N_total = Int(list_size(vec))
    child_reader = VecReader(child_vec, child_logical_type, julia_type_inner, N_total)
    list_vec = get_array(vec, duckdb_list_entry_t, N)

    data = (julia_type_inner, K, V, N, N_child, list_vec, child_reader)
    return VecReader(vec, Int(N), logical_type, T, _getindex_dict, validity_mask, data)
end

function _getindex_dict(data, index)
    (julia_type_inner, K, V, N, N_child, list_vec, child_reader) = data
    list_entry::duckdb_list_entry_t = list_vec[index]
    N = list_entry.length
    offset = list_entry.offset

    d = Dict{K, V}()
    sizehint!(d, Int(N))
    for i in 1:N
        key_value = child_reader[offset + i]
        key = convert(K, key_value.key)
        value = convert(V, key_value.value)
        d[key] = value
    end
    return d
end



function _create_vecreader_union(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    K = get_union_member_count(logical_type)
    #names = [get_union_member_name(logical_type, i) for i in 1:K]
    types_julia = DataType[]
    types_logical = LogicalType[]
    for i in 1:K
        #name_u = names[i]
        type_u = get_union_member_type(logical_type, i)
        push!(types_logical, type_u)
        julia_type_u::DataType = duckdb_type_to_julia_type(type_u)
        push!(types_julia, julia_type_u)
        #println(name_u, " ", get_type_id(type_u), " ", julia_type_u)
    end

    # Ks = get_struct_child_count(logical_type)
    # println("Struct Child Count: ", Ks)
    # for i in 1:Ks
    #     s_name = get_struct_child_name(logical_type, i)
    #     s_type = get_struct_child_type(logical_type, i)
    #     println("name='", s_name, "', type='", get_type_id(s_type), "'")
    # end

    julia_type = Union{types_julia...}
    #println("Union Type Julia: ", julia_type)

    T_index = UInt8 # Tag/Index of union type is a UTINYINT and is always the first struct child
    index_reader = VecReader(struct_child(vec, 1), create_logical_type(T_index), T_index, N)

    # The rest of the struct children are the actual values
    julia_type_actual = Tuple(t for t in types_julia)
    internal_readers = [VecReader(struct_child(vec, i + 1), types_logical[i], julia_type_actual[i], N) for i in 1:K]
    data = (index_reader, internal_readers, julia_type_actual)

    validity_mask = get_readonly_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, julia_type, _getindex_union, validity_mask, data)
end


function _getindex_union(data, index)
    (index_reader, internal_readers, julia_type_actual) = data
    tag = index_reader[index] # TAG is zero based
    tag += 1 # convert to 1-based index
    if tag < 1 || tag > length(internal_readers)
        throw(BoundsError(tag))
    end

    T_output = julia_type_actual[tag] # Actual type
    value = internal_readers[tag][index]
    return convert(T_output, value)
end


# %% --------------------------------------------------------
#        Writer Interface
#------------------------------------------------------------

"""
    VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T}

Create a new VecWriter for the given vector and logical type. The writer is used to write data to the vector.
The type T is the type of the data that will be written to the vector. The size N is the number of elements in the vector.
"""
mutable struct VecWriter{T, D, F <: Function}
    vec::Vec
    N::Int
    logical_type::LogicalType
    julia_type::Type{T}
    setindex_func::F
    propagate_invalid::Function
    validity_mask::ValidityMask
    data::D # Internal data -> e.g. array of vector data or other writers for complex types
end

VecWriter(vec::Vec, logical_type::LogicalType, ::Type{Union{Missing, T}}, N) where {T} =
    VecWriter(vec, logical_type, T, N)

VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Integer} =
    _create_vecwriter_simple(vec, logical_type, T, N)
VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: AbstractFloat} =
    _create_vecwriter_simple(vec, logical_type, T, N)
VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Date} =
    _create_vecwriter_simple(vec, logical_type, T, N)
VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Time} =
    _create_vecwriter_simple(vec, logical_type, T, N)
VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: DateTime} =
    _create_vecwriter_simple(vec, logical_type, T, N)
VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Union{Period, Dates.CompoundPeriod}} =
    _create_vecwriter_simple(vec, logical_type, T, N)
VecWriter(vec::Vec, logical_type::LogicalType, ::Type{UUID}, N) = _create_vecwriter_simple(vec, logical_type, UUID, N)

VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: FixedDecimal} =
    _create_vecwriter_decimal(vec, logical_type, T, N)

function VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T}
    type_id = get_type_id(logical_type)
    if type_id === DUCKDB_TYPE_UNION
        return _create_vecwriter_union(vec, logical_type, T, N)
    end
    throw(NotImplementedException(string("VecWriter not implemented for type ", T)))
end

function VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: AbstractString}
    return _create_vecwriter_string(vec, logical_type, T, N)
end

function VecWriter(
    vec::Vec,
    logical_type::LogicalType,
    ::Type{T},
    N = VECTOR_SIZE
) where {T <: Base.CodeUnits{UInt8, String}}
    return _create_vecwriter_string(vec, logical_type, T, N)
end

function VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: AbstractArray}
    return _create_vecwriter_list(vec, logical_type, T, N)
end

function VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: NamedTuple}
    return _create_vecwriter_struct(vec, logical_type, T, N)
end

function VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: AbstractDict}
    return _create_vecwriter_map(vec, logical_type, T, N)
end

function VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T <: Tuple}
    return _create_vecwriter_array(vec, logical_type, T, N)
end

Base.length(writer::VecWriter) = writer.N

function Base.setindex!(writer::VecWriter{T}, value, index::Integer) where {T}
    if index < 1 || index > writer.N
        throw(BoundsError(writer, index))
    end
    if ismissing(value)
        setinvalid(writer.validity_mask, index)
        # Necessary to also mark all child elements as invalid
        writer.propagate_invalid(writer, index)
    else
        writer.setindex_func(writer, value, index)
    end
    return nothing
end

function write_to_vector(writer::VecWriter, data)
    i = 1
    for xi in data
        writer[i] = xi
        i += 1
    end
end

# %% --- primitive types ------------------------------------------ #

function _create_vecwriter_simple(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    internal_type_static = julia_to_duck_type(T)
    data = get_array(vec, internal_type_static, N)
    setindex_func = setindex_simple!
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, setindex_func, _propagate_invalid_simple, validity_mask, data)
end

function _create_vecwriter_string(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    type_id = get_type_id(logical_type)
    string_array = get_array(vec, duckdb_string_t, N)
    data = (vec, string_array)
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, setindex_string!, _propagate_invalid_string, validity_mask, data)
end

function _create_vecwriter_blob(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T <: Base.CodeUnits{UInt8}}
    data = vec
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, _setindex_blob!, _propagate_invalid_simple, validity_mask, data)
end

function _create_vecwriter_decimal(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    internal_type_static = julia_to_duck_type(T)
    data = get_array(vec, internal_type_static, N)
    setindex_func = setindex_fixed_decimal!
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, setindex_func, _propagate_invalid_simple, validity_mask, data)
end

function setindex_simple!(w::VecWriter, v, i)
    w.data[i] = v
    return nothing
end

function _propagate_invalid_simple(w::VecWriter, i)
    setinvalid(w.validity_mask, i)
    return nothing
end


function _propagate_invalid_string(w::VecWriter, i)
    setinvalid(w.validity_mask, i)
    _, string_array = w.data
    string_array[i] = duckdb_string_t_zero
    return nothing
end

function setindex_string!(w::VecWriter, v, i)
    vec, _ = w.data
    return assign_string_element(vec, i, v)
end

function _setindex_blob!(w::VecWriter, v, i)
    return assign_blob_element(w.data, i, v)
end

function setindex_fixed_decimal!(w::VecWriter, x::FixedDecimal{T, S}, i) where {T, S}
    #val = Base.reinterpret(T, x)
    #Tout = eltype(w.data)
    #@show T Tout val x
    #w.data[i] = Base.reinterpret(T, x) # reinterpret the FixedDecimal to the internal type to avoid Inexact errors
    w.data[i] = x
    return nothing
end

# %% --- Arrays ------------------------------------------ #

function _create_vecwriter_array(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    child_vec = array_child(vec)
    child_type = get_array_child_type(logical_type)
    child_julia_type = duckdb_type_to_julia_type(child_type)
    child_julia_type_static = eltype(T)
    parent_type = T
    N_array = get_array_child_size(logical_type)
    N_child = N * N_array
    child_writer = VecWriter(child_vec, child_type, child_julia_type_static, N_child)
    data = (N_array, child_writer, parent_type) # TODO make struct
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, _setindex_array, _propagate_invalid_array, validity_mask, data)
end

function _setindex_array(writer, value, index)
    N_array, child_writer, T_parent = writer.data
    offset = (index - 1) * N_array + 1
    i = 0
    for ix in eachindex(value)
        child_writer[offset + i] = value[ix]
        i += 1

        if i > N_array
            throw(ArgumentError(string("Array size mismatch: ", i, " > ", N_array, " at index ", index)))
        end
    end
end

function _propagate_invalid_array(writer, index)
    N_array, child_writer, T_parent = writer.data
    offset = (index - 1) * N_array + 1
    for i in 0:N_array
        child_writer.propagate_invalid(child_writer, offset + i)
    end
end

# %% --- Struct ------------------------------------------ #

function _check_struct_type(::Type{T}, logical_type::LogicalType) where {T}
    type_id = get_type_id(logical_type)
    if type_id != DUCKDB_TYPE_STRUCT
        throw(ArgumentError(string("Internal Conversion Error: Type mismatch: ", type_id, " != ", DUCKDB_TYPE_STRUCT)))
    end

    K = get_struct_child_count(logical_type)
    K = get_struct_child_count(logical_type)
    names = fieldnames(T)
    Nf = length(names)
    if K != Nf
        throw(ArgumentError(string("Internal Conversion Error: Struct field count mismatch: ", K, " != ", Nf)))
    end

    for i in 1:K
        s_name = get_struct_child_name(logical_type, i)
        if string(names[i]) != s_name
            throw(
                ArgumentError(
                    string("Internal Conversion Error: Struct field name mismatch: ", names[i], " != ", s_name)
                )
            )
        end
    end

    for i in 1:K
        s_type = get_struct_child_type(logical_type, i)
        s_type_id = get_type_id(s_type)
        lt::LogicalType = create_logical_type(fieldtype(T, names[i]))
        lt_id = get_type_id(lt)
        if lt_id != s_type_id
            throw(
                ArgumentError(
                    string("Internal Conversion Error: Struct field type mismatch: ", lt_id, " != ", s_type_id)
                )
            )
        end
    end
end

function _create_vecwriter_struct(
    vec::Vec,
    logical_type::LogicalType,
    ::Type{T},
    N
) where {names, T <: NamedTuple{names}}
    _check_struct_type(T, logical_type)
    types_tuple = fieldtypes(T)
    Nf = _tuple_count(names)
    writers = NTuple{Nf, VecWriter}(
        VecWriter(struct_child(vec, k), get_struct_child_type(logical_type, k), types_tuple[k], N) for k in 1:Nf
    )
    writer_types_ = typeof.(writers)
    #writer_types = NTuple{Nf,VecWriter}
    writer_types = Tuple{writer_types_...}
    data = NamedTuple{names, writer_types}(writers)
    setindex_func = setindex_struct!
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, setindex_func, _propagate_invalid_struct, validity_mask, data)
end

_namedtuple_names(::Type{NamedTuple{names}}) where {names} = names
_namedtuple_names(::NamedTuple{names}) where {names} = names
function setindex_struct!(_writer::VecWriter, value, index)
    data = _writer.data
    names = _namedtuple_names(data)
    for name in names
        writer = getindex(data, name)
        writer[index] = getproperty(value, name)
    end
end

function _propagate_invalid_struct(_writer::VecWriter, index)
    for writer in _writer.data
        writer.propagate_invalid(writer, index)
    end
end

# %% ----- List ------------------------------
function _create_vecwriter_list(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    # Reserve space for the list entries
    duckdb_list_vector_reserve(vec.handle, N)
    child_vec = list_child(vec)
    child_logical_type = LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle))
    child_logical_type_static = eltype(T)
    child_julia_type = duckdb_type_to_julia_type(child_logical_type)
    validity_mask = get_validity(vec, N)
    data = (vec, nothing, N, child_logical_type_static)
    return VecWriter(vec, Int(N), logical_type, T, _setindex_list, _propagate_invalid_list, validity_mask, data)
end

function _setindex_list(_writer, value, index)
    vec, _entries_old, _, child_julia_type = _writer.data
    N = _writer.N # TODO cleanup
    entries = get_array(vec, duckdb_list_entry_t, N)

    N_entry = length(value)
    N_total = list_size(vec)
    N_total_new = N_total + N_entry
    state = duckdb_list_vector_reserve(vec.handle, N_total_new)
    if state != DuckDBSuccess
        throw(QueryException("Failed to reserve list size"))
    end
    state = duckdb_list_vector_set_size(vec.handle, N_total_new)
    if state != DuckDBSuccess
        throw(QueryException("Failed to set list size"))
    end


    child_vec = list_child(vec)
    child_logical_type = LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle))

    offset = N_total
    entry = DuckDB.duckdb_list_entry_t(offset, N_entry)
    entries[index] = entry
    child_writer = VecWriter(child_vec, child_logical_type, child_julia_type, N_total_new)

    j = offset + 1
    for ix in eachindex(value)
        child_writer[j] = value[ix]
        j += 1
    end
end

function _propagate_invalid_list(_writer::VecWriter, index)
    vec, _entries_old, _, child_julia_type = _writer.data
    #child_vec = list_child(vec)
    #N_total = list_size(vec)
    #child_logical_type = LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle))
    setinvalid(_writer.validity_mask, index)
    entries = get_array(vec, duckdb_list_entry_t, _writer.N)
    entries[index] = duckdb_list_entry_t(0, 0) # set zero length
    #child_writer = VecWriter(child_vec, child_logical_type, child_julia_type, N_total)
    #return child_writer.propagate_invalid(child_writer, index)
    return
end

# %% ----- Dict ------------------------------

function _create_vecwriter_map(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {K, V, T <: AbstractDict{K, V}}
    # Internally map vectors are stored as a LIST[STRUCT(key KEY_TYPE, value VALUE_TYPE)].
    child_vec = list_child(vec)
    child_logical_type = LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle))

    N_child = list_size(vec)
    key_type = get_map_key_type(logical_type)
    value_type = get_map_value_type(logical_type)
    child_logical_type = LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle)) # struct type

    M = get_struct_child_count(child_logical_type)
    for i in 1:M
        s_name = get_struct_child_name(child_logical_type, i)
        s_type = get_type_id(get_struct_child_type(child_logical_type, i))
    end

    lt_key = create_logical_type(K)
    lt_value = create_logical_type(V)
    @assert get_type_id(lt_key) == get_type_id(key_type)
    @assert get_type_id(lt_value) == get_type_id(value_type)

    #julia_type_inner = Vector{NamedTuple{(:key, :value), Tuple{K,V}}}
    julia_type_inner = NamedTuple{(:key, :value), Tuple{K, V}}
    #child_writer = VecWriter(child_vec, child_logical_type, julia_type_inner, N_child)
    validity_mask = get_validity(vec, N)

    data = (child_logical_type, julia_type_inner, N)
    return VecWriter(vec, Int(N), logical_type, T, _setindex_dict, _propagate_invalid_map, validity_mask, data)
end


function _setindex_dict(_writer, value, index)
    #child_writer = _writer.data
    (child_logical_type, child_julia_type, N) = _writer.data
    entries = get_array(_writer.vec, duckdb_list_entry_t, _writer.N)

    N_entry = length(value)
    N_total = list_size(_writer.vec)
    N_total_new = N_total + N_entry

    state = duckdb_list_vector_reserve(_writer.vec.handle, N_total_new)
    if state != DuckDBSuccess
        throw(QueryException("Failed to reserve list size"))
    end
    state = duckdb_list_vector_set_size(_writer.vec.handle, N_total_new)
    if state != DuckDBSuccess
        throw(QueryException("Failed to set list size"))
    end

    child_vec = list_child(_writer.vec)
    child_writer = VecWriter(child_vec, child_logical_type, child_julia_type, N_total_new)

    offset = N_total
    entry = DuckDB.duckdb_list_entry_t(offset, N_entry)
    entries[index] = entry

    i = 1
    for (k, v) in pairs(value)
        j = offset + i
        child_writer[j] = (key = k, value = v)
        i += 1
    end
end

function _propagate_invalid_map(_writer::VecWriter, index)
    (child_logical_type, child_julia_type, N) = _writer.data
    child_vec = list_child(_writer.vec)
    N_total = list_size(_writer.vec)
    child_writer = VecWriter(child_vec, child_logical_type, child_julia_type, N_total)
    entries = get_array(_writer.vec, duckdb_list_entry_t, _writer.N)
    entries[index] = duckdb_list_entry_t(0, 0) # set zero length
    setinvalid(_writer.validity_mask, index)
    return nothing
end

# %% ----- Union ------------------------------

function _create_vecwriter_union(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    # Internally UNION utilizes the same structure as a STRUCT. The first “child” is always occupied by the Tag Vector of the UNION, which records for each row which of the UNION's types apply to that row.

    # Julia Type: Union{T1, T2, T3}
    # Internal Type: STRUCT(''::UINT64, '1'::T1, '2'::T2, '3'::T3)

    K = get_union_member_count(logical_type)
    types_julia = []
    types_logical = LogicalType[]
    for i in 1:K
        type_u = get_union_member_type(logical_type, i)
        push!(types_logical, type_u)
        julia_type_u = duckdb_type_to_julia_type(type_u)
        push!(types_julia, julia_type_u)
    end

    # Ks = get_struct_child_count(logical_type)
    # println("Struct Child Count: ", Ks)
    # for i in 1:Ks
    #     s_name = get_struct_child_name(logical_type, i)
    #     s_type = get_struct_child_type(logical_type, i)
    #     println("name='", s_name, "', type='", get_type_id(s_type), "'")
    # end

    julia_type = Union{types_julia...}
    T_index = UInt8 # Tag/Index of union type is a UTINYINT and is always the first struct child
    index_writer = VecWriter(struct_child(vec, 1), create_logical_type(T_index), T_index, N)

    # The rest of the struct children are the actual values
    julia_type_actual = Tuple(t for t in types_julia)
    internal_writers = [VecWriter(struct_child(vec, i + 1), types_logical[i], julia_type_actual[i], N) for i in 1:K]
    data = (index_writer, internal_writers, julia_type_actual)

    validity_mask = get_validity(vec, N)
    return VecWriter(
        vec,
        Int(N),
        logical_type,
        julia_type,
        _setindex_union,
        _propagate_invalid_union,
        validity_mask,
        data
    )
end

function _setindex_union(_writer::VecWriter, value::T, index) where {T}
    index_writer, internal_writers, julia_type_actual = _writer.data
    T_actual = typeof(value)
    tag = findfirst(x -> x == T_actual, julia_type_actual) # find the correct type

    if tag === nothing
        throw(QueryException("Invalid type for Union $julia_type_actual: $T_actual"))
    end

    element_writer = internal_writers[tag]

    index_writer[index] = tag - 1 # convert to 0-based index
    return element_writer[index] = value
end

function _propagate_invalid_union(_writer::VecWriter, index)
    index_writer, internal_writers, _ = _writer.data
    setinvalid(_writer.validity_mask, index)
    setinvalid(index_writer.validity_mask, index)
    index_writer.propagate_invalid(index_writer, index)
    for writer in internal_writers
        writer.propagate_invalid(writer, index)
    end
end

# %% --- Enum ------------------------------------------ #

function _create_vecwriter_enum(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    throw(NotImplementedException("Enum not implemented"))
end


# %% --- Writer Util ------------------------------------------ #


function Base.sizehint!(writer::VecWriter, N; kwargs...)
    type_id = get_type_id(writer.logical_type)
    if type_id != DUCKDB_TYPE_LIST
        return writer
    end
    N_current = list_size(writer.vec)
    if N_current >= N
        return writer
    end
    state = duckdb_list_vector_reserve(writer.vec.handle, N)
    if state != DuckDBSuccess
        throw(QueryException("Failed to reserve list size"))
    end
    return writer
end


function Base.empty!(writer::VecWriter)
    type_id = get_type_id(writer.logical_type)
    if type_id != DUCKDB_TYPE_LIST
        return writer
    end
    # TODO - perform the cleanup also for child vectors
    entries = get_array(writer.vec, duckdb_list_entry_t, writer.N)
    for i in 1:(writer.N)
        entries[i] = duckdb_list_entry_t(0, 0)
    end

    state = duckdb_list_vector_set_size(writer.vec.handle, 0)
    if state != DuckDBSuccess
        throw(QueryException("Failed to set list size"))
    end

    state = duckdb_list_vector_reserve(writer.vec.handle, 0)
    if state != DuckDBSuccess
        throw(QueryException("Failed to reserve list size"))
    end

    return writer
end





# %% --------------------------------------------------------
#        Write to Vector
#------------------------------------------------------------



function write_to_vector(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}

    type_id = get_type_id(logical_type)

    if type_id === DUCKDB_TYPE_STRUCT
        write_to_vector_struct(vec, N_internal, logical_type, data)
    elseif type_id === DUCKDB_TYPE_LIST
        write_to_vector_list(vec, N_internal, logical_type, data)
    elseif type_id === DUCKDB_TYPE_MAP
        write_to_vector_map(vec, N_internal, logical_type, data)
    elseif type_id === DUCKDB_TYPE_UNION
        write_to_vector_union(vec, N_internal, logical_type, data)
        #throw(NotImplementedException("Union not implemented"))
    elseif type_id === DUCKDB_TYPE_ENUM
        write_to_vector_enum(vec, N_internal, logical_type, data)
    elseif type_id === DUCKDB_TYPE_ARRAY
        write_to_vector_array(vec, N_internal, logical_type, data)
    elseif type_id === DUCKDB_TYPE_VARCHAR || type_id === DUCKDB_TYPE_BLOB || type_id === DUCKDB_TYPE_BIT
        write_to_vector_string(vec, N_internal, logical_type, data)
    elseif type_id === DUCKDB_TYPE_DECIMAL
        write_to_vector_decimal(vec, N_internal, logical_type, data)
    else
        write_to_vector_simple(vec, N_internal, logical_type, data)
    end
end

function write_to_vector_enum(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}
    throw(NotImplementedException("ENUM not supported yet"))

    internal_type_id = get_internal_type_id(logical_type)
    T_internal = duckdb_type_to_internal_type(internal_type_id)

    dict_keys = get_enum_dictionary(logical_type)
    N_enum = length(dict_keys)

    arr = get_array(vec, T_internal, N_internal)

    if has_missing(data)
        validity_mask = get_validity(vec, N_internal)
        for i in 1:N_internal
            x = data[i] # TODO Use categorical arrays
            if ismissing(x)
                setinvalid(validity_mask, i)
                arr[i] = 0  # set save value
            else
                ix = findfirst(s -> s == x, dict_keys)
                if ix === nothing
                    throw(QueryException("Key $x not in enum dictionary $dict_keys"))
                end
                if ix > N_enum
                    throw(QueryException("Key $x matched index $ix but enum has only $N_enum values"))
                end
                arr[i] = ix - 1 # 0-based index
            end
        end
    else
        for i in 1:N_internal
            x = data[i] # TODO Use categorical arrays
            ix = findfirst(s -> s == x, dict_keys)
            if ix === nothing
                throw(QueryException("Key $x not in enum dictionary $dict_keys"))
            end
            arr[i] = ix - 1 # 0-based index
        end
    end

    return nothing
end

function write_to_vector_union(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}
    K = get_union_member_count(logical_type)

    union_types = [get_union_member_type(logical_type, i) for i in 1:K]
    julia_types = duckdb_type_to_julia_type.(union_types)

    vec_index = struct_child(vec, 1)
    struct_children = [struct_child(vec, i + 1) for i in 1:K]
    data_index = get_array(vec_index, UInt8, N_internal)


    if has_missing(data)
        validity_mask = get_validity(vec)
        for i in 1:N_internal
            x = data[i]
            if ismissing(x) || isnothing(x)
                setinvalid(validity_mask, i)
            end
            Ti = typeof(x)
            tag = findfirst(x -> x == Ti, julia_types)
            if tag === nothing
                throw(QueryException("Cannot find type $Ti in Union $julia_types"))
            end
            data_index[i] = tag - 1 # set the tag
        end

        # Write the actual values
        data_arrays = [[x == Ti ? x : missing for x in data] for Ti in julia_types]
        for i in 1:K
            write_to_vector(struct_children[i], N_internal, union_types[i], data_arrays[i])
        end
    else
        for i in 1:N_internal
            x = data[i]
            Ti = typeof(x)
            tag = findfirst(x -> x == Ti, julia_types)
            if tag === nothing
                throw(QueryException("Cannot find type $Ti in Union $julia_types"))
            end
            data_index[i] = tag - 1 # set the tag
        end

        # Write the actual values
        data_arrays = [[x == Ti ? x : missing for x in data] for Ti in julia_types]
        for i in 1:K
            write_to_vector(struct_children[i], N_internal, union_types[i], data_arrays[i])
        end
    end
end

function write_to_vector_list(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}

    child_vec = list_child(vec)
    child_logical_type = get_list_child_type(logical_type)

    entries = get_array(vec, duckdb_list_entry_t, N_internal)

    if has_missing(data)
        offset = 0
        N_total = 0
        validity_mask = get_validity(vec)
        for i in 1:N_internal
            x = data[i]
            if ismissing(x) || isnothing(x)
                entries[i] = duckdb_list_entry_t(0, 0)
                setinvalid(validity_mask, i)
            else
                N_entries = length(x)
                entries[i] = duckdb_list_entry_t(offset, N_entries)
                offset += N_entries
                N_total += N_entries
            end
        end

        duckdb_list_vector_reserve(vec.handle, N_total)
        duckdb_list_vector_set_size(vec.handle, N_total)

        #data_child = [xi for x in data for xi in x if !ismissing(xi)]
        data_child = flatten_and_skip_missing(data)
        write_to_vector(child_vec, N_total, child_logical_type, data_child)
    else
        offset = 0
        N_total = 0
        for i in 1:N_internal
            x = data[i]
            N_entries = length(x)
            entries[i] = duckdb_list_entry_t(offset, N_entries)
            offset += N_entries
            N_total += N_entries
        end

        duckdb_list_vector_reserve(vec.handle, N_total)
        duckdb_list_vector_set_size(vec.handle, N_total)

        data_child = [xi for x in data for xi in x]
        write_to_vector(child_vec, N_total, child_logical_type, data_child)
    end

end

function write_to_vector_map(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}

    key_type = get_map_key_type(logical_type)
    value_type = get_map_value_type(logical_type)

    child_vec = list_child(vec)
    child_logical_type = LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle)) # struct type
    K = get_struct_child_count(child_logical_type)

    if K != 2
        throw(ArgumentError("Internal Conversion Error: Map struct child count mismatch"))
    end

    entries = get_array(vec, duckdb_list_entry_t, N_internal)

    if has_missing(data)
        offset = 0
        N_total = sum(ismissing(x) ? 0 : length(x) for x in data; init = 0)

        duckdb_list_vector_reserve(vec.handle, N_total)
        duckdb_list_vector_set_size(vec.handle, N_total)
        validity_mask = get_validity(vec, N_internal)

        for i in 1:N_internal
            x = data[i]
            if ismissing(x) || isnothing(data[i])
                entries[i] = duckdb_list_entry_t(0, 0)
                setinvalid(validity_mask, i)
            else
                N_entries = length(x)
                entries[i] = duckdb_list_entry_t(offset, N_entries)
                offset += N_entries
            end
        end

        T_Key = keytype(base_type(T))
        T_Value = valtype(base_type(T))
        data_keys = Vector{T_Key}(undef, N_total)
        data_values = Vector{T_Value}(undef, N_total)
        k = 1
        for i in 1:N_internal
            x = data[i]
            if ismissing(x)
                continue
            end
            for kv in x
                data_keys[k] = kv[1]
                data_values[k] = kv[2]
                k += 1
            end
        end
        # store the actual data
        write_to_vector(struct_child(child_vec, 1), N_total, key_type, data_keys)
        write_to_vector(struct_child(child_vec, 2), N_total, value_type, data_values)
    else
        offset = 0
        #data_key_values = [pairs(data_i) for data_i in data]
        N_total = sum(length(x) for x in data; init = 0)

        duckdb_list_vector_reserve(vec.handle, N_total)
        duckdb_list_vector_set_size(vec.handle, N_total)

        # set the list entries
        for i in 1:N_internal
            x = data[i]
            N_entries = length(x)
            entries[i] = duckdb_list_entry_t(offset, N_entries)
            offset += N_entries
        end

        # store the actual data
        data_keys = [k for data_i in data for (k, v) in data_i]
        data_values = [v for data_i in data for (k, v) in data_i]
        write_to_vector(struct_child(child_vec, 1), N_total, key_type, data_keys)
        write_to_vector(struct_child(child_vec, 2), N_total, value_type, data_values)
    end
end

function write_to_vector_array(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}
    child_logical_type = get_array_child_type(logical_type)
    child_size = get_array_child_size(logical_type)

    N_real = N_internal * child_size # real size of the array
    if has_missing(data)
        validity_mask = get_validity(vec, N_internal)
        for i in 1:N_internal
            if ismissing(data[i]) || isnothing(data[i])
                setinvalid(validity_mask, i)
            end
        end
        write_to_vector(vec, N_real, child_logical_type, data)
    else

        # FIXME this could be more performant
        #T_inner = eltype(eltype(data))
        #data_flat = Base.reinterpret(T_inner, data)
        data_flat = [xi for x in data for xi in x]
        write_to_vector(vec, N_real, child_logical_type, data_flat)
    end
end

function write_to_vector_struct(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}
    K = get_struct_child_count(logical_type)
    if has_missing(data)
        validity_mask = get_validity(vec, N_internal)

        # Set validity for main vec
        for i in 1:N_internal
            if ismissing(data[i]) || isnothing(data[i])
                setinvalid(validity_mask, i)
            end
        end

        # Process each struct child and also set validity
        for k in 1:K
            fieldname = get_struct_child_name(logical_type, k)
            fieldname_s = Symbol(fieldname)
            field_data = [ismissing(data_i) ? missing : getproperty(data_i, fieldname_s) for data_i in data]
            write_to_vector(struct_child(vec, k), N_internal, get_struct_child_type(logical_type, k), field_data)
        end
    else
        for k in 1:K
            fieldname = get_struct_child_name(logical_type, k)
            fieldname_s = Symbol(fieldname)
            field_data = [getproperty(data_i, fieldname_s) for data_i in data] # get the field data
            write_to_vector(struct_child(vec, k), N_internal, get_struct_child_type(logical_type, k), field_data)
        end
    end
end

function write_to_vector_simple(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}
    type_id = get_type_id(logical_type)
    T_internal = duckdb_type_to_internal_type(type_id)
    array = get_array(vec, T_internal, N_internal)
    if has_missing(data)
        validity_mask = get_validity(vec, N_internal)
        for i in 1:N_internal
            if ismissing(data[i]) || isnothing(data[i])
                setinvalid(validity_mask, i)
            else
                array[i] = data[i]
            end
        end
    else
        for i in 1:N_internal
            array[i] = data[i]
        end
    end
end

function write_to_vector_string(vec::Vec, N_internal::Int, logical_type::LogicalType, data::Vector{T}) where {T}
    if has_missing(data)
        validity_mask = get_validity(vec, N_internal)
        for i in 1:N_internal
            if ismissing(data[i]) || isnothing(data[i])
                setinvalid(validity_mask, i)
            else
                assign_string_element(vec, i, data[i])
            end
        end
    else
        for i in 1:N_internal
            assign_string_element(vec, i, data[i])
        end
    end
end

function write_to_vector_decimal(
    vec::Vec,
    N_internal::Int,
    logical_type::LogicalType,
    data::Vector{R}
) where {T, S, R <: Union{Missing, FixedDecimal{T, S}}}
    #type_id = get_type_id(logical_type)
    #T_internal = duckdb_type_to_internal_type(type_id)
    array = get_array(vec, FixedDecimal{T, S}, N_internal)
    if has_missing(data)
        validity_mask = get_validity(vec, N_internal)
        for i in 1:N_internal
            if ismissing(data[i]) || isnothing(data[i])
                setinvalid(validity_mask, i)
            else
                array[i] = data[i]
            end
        end
    else
        for i in 1:N_internal
            array[i] = data[i]
        end
    end
end
