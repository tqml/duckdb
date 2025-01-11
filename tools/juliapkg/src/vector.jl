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

function assign_string_element(vector::Vec, index, str::String)
    return duckdb_vector_assign_string_element_len(vector.handle, index, str, sizeof(str))
end

function assign_string_element(vector::Vec, index, str::AbstractString)
    return duckdb_vector_assign_string_element_len(vector.handle, index, str, sizeof(str))
end


# %% --------------------------------------------------------
#        Reader Interface
#------------------------------------------------------------

mutable struct VecReader{T, D}
    vec::Vec
    N::Int
    logical_type::LogicalType
    julia_type::T
    conversion_func::Function
    getindex_func::Function
    validity_mask::ValidityMask
    data::D
end

Base.length(reader::VecReader) = reader.N
Base.iterate(reader::VecReader, state = 1) = state > length(reader) ? nothing : (reader[state], state + 1)
Base.eltype(reader::VecReader) = reader.julia_type
Base.HasEltype(::Type{VecReader}) = true
Base.HasLength(::Type{VecReader}) = true

function VecReader(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T}
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
    elseif type_id == DUCKDB_TYPE_BLOB || type_id == DUCKDB_TYPE_BIT
        reader = _create_vecreader_simple(vec, logical_type, T, N)
        reader.conversion_func = x -> Base.codeunits(convert(T, x))
        return reader
    elseif is_complex_type(logical_type)
        throw(NotImplementedException("Complex types are not supported"))
    else
        return _create_vecreader_simple(vec, logical_type, T, N)
    end
end


function _create_vecreader_simple(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    type_id = get_type_id(logical_type)
    internal_type = duckdb_type_to_internal_type(type_id)
    data = get_array(vec, internal_type, N)
    conversion_func = x -> convert(T, x)
    getindex_func = getindex
    validity_mask = get_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, T, conversion_func, getindex_func, validity_mask, data)
end

function _create_vecreader_list(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    child_vec = list_child(vec)
    child_type = get_list_child_type(logical_type)
    julia_type = duckdb_type_to_julia_type(child_type)
    list_vec = get_array(vec, duckdb_list_entry_t, N)
    N_total = Int(list_size(vec))
    child_reader = VecReader(child_vec, child_type, julia_type, N_total)
    data = (list_vec, child_reader) # TODO make struct
    validity_mask = get_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, T, identity, _getindex_list, validity_mask, data)
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
    N_array = get_array_child_size(logical_type)
    N_child = N * N_array
    child_reader = VecReader(child_vec, child_type, child_julia_type, N_child)
    data = (N, N_array, N_child, child_reader) # TODO make struct
    validity_mask = get_readonly_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, T, identity, _getindex_array, validity_mask, data)
end

function _getindex_array(data, index)
    N, N_array, N_child, child_reader = data
    offset = (index - 1) * N_array + 1
    return Tuple(child_reader[i] for i in offset:(offset + N_array - 1))
end

function _create_vecreader_struct(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    julia_type = duckdb_type_to_julia_type(logical_type)
    K = get_struct_child_count(logical_type)
    names = Vector{Symbol}()
    readers = Vector{VecReader}()
    for k in 1:K
        child_type = get_struct_child_type(logical_type, k)
        child_vec = struct_child(vec, k)
        child_type_julia = duckdb_type_to_julia_type(child_type)
        child_reader = VecReader(child_vec, child_type, child_type_julia, N)
        push!(readers, child_reader)
        child_name = Symbol(get_struct_child_name(logical_type, k))
        push!(names, child_name)
    end
    names_tuple = Tuple(name for name in names)
    data = NamedTuple{names_tuple}(readers)
    validity_mask = get_readonly_validity(vec, N)
    return VecReader(vec, Int(N), logical_type, T, identity, _get_index_struct, validity_mask, data)
end

function _get_index_struct(data::NamedTuple{names}, index) where {names}
    return NamedTuple{names}(getindex(reader, index) for reader in data)
end

function _create_vecreader_dict(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    # Internally map vectors are stored as a LIST[STRUCT(key KEY_TYPE, value VALUE_TYPE)].
    child_vec = list_child(vec)
    N_child = list_size(vec)
    key_type = get_map_key_type(logical_type)
    value_type = get_map_value_type(logical_type)
    validity_mask = get_validity(vec, N)
    child_reader = VecReader(child_vec, value_type, T, N_child)
    data = (N, N_child, key_type, value_type, child_reader)
    return VecReader(vec, Int(N), logical_type, T, identity, _getindex_dict, validity_mask, data)
end

function _getindex_dict(data, index)
    (N, N_child, key_type, value_type, child_reader) = data
    s = child_reader[index]
    dict_keys = s[1]
    dict_values = s[2]
    K = dict_keys .=> dict_values
    return Dict(K)
end

function Base.getindex(reader::VecReader, index)
    if index < 1 || index > reader.N
        throw(BoundsError(reader, index))
    end

    if all_valid(reader.validity_mask)
        x = reader.getindex_func(reader.data, index)
        return reader.conversion_func(x)
    else
        if isvalid(reader.validity_mask, index)
            x = reader.getindex_func(reader.data, index)
            return reader.conversion_func(x)
        else
            return missing
        end
    end
end

function julia_eltype(r::VecReader)
    return duckdb_type_to_julia_type(r.logical_type, all_valid(r.validity_mask))
end

# %% --------------------------------------------------------
#        Writer Interface
#------------------------------------------------------------

mutable struct VecWriter{T, D}
    vec::Vec
    N::Int
    logical_type::LogicalType
    julia_type::T
    setindex_func::Function
    validity_mask::ValidityMask
    data::D
end


function VecWriter(vec::Vec, logical_type::LogicalType, ::Type{T}, N = VECTOR_SIZE) where {T}
    type_id = get_type_id(logical_type)
    if type_id == DUCKDB_TYPE_VARCHAR
        return _create_vecwriter_string(vec, logical_type, T, N)
    elseif type_id == DUCKDB_TYPE_STRUCT
        return _create_vecwriter_struct(vec, logical_type, T, N)
    elseif type_id == DUCKDB_TYPE_LIST
        return _create_vecwriter_list(vec, logical_type, T, N)
    elseif type_id == DUCKDB_TYPE_ARRAY
        return _create_vecwriter_array(vec, logical_type, T, N)
    elseif is_complex_type(logical_type)
        throw(NotImplementedException("Complex types are not supported"))
    else
        return _create_vecwriter_simple(vec, logical_type, T, N)
    end
end

Base.length(writer::VecWriter) = writer.N
function Base.setindex!(writer::VecWriter, value, index)
    if index < 1 || index > writer.N
        throw(BoundsError(writer, index))
    end
    if ismissing(value)
        setinvalid(writer.validity_mask, index)
    else
        return writer.setindex_func(writer.data, value, index)
    end
end

# %% --- primitive types ------------------------------------------ #
function _create_vecwriter_simple(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    type_id = get_type_id(logical_type)
    internal_type = duckdb_type_to_internal_type(type_id)
    data = get_array(vec, internal_type, N)
    setindex_func = setindex!
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, setindex_func, validity_mask, data)
end


setindex_string!(v::Vec, s::AbstractString, i) = assign_string_element(v, i, s)
function _create_vecwriter_string(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    data = vec
    setindex_func = setindex_string!
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, setindex_func, validity_mask, data)
end


# %% --- Arrays ------------------------------------------ #

function _create_vecwriter_array(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    child_vec = array_child(vec)
    child_type = get_array_child_type(logical_type)
    child_julia_type = duckdb_type_to_julia_type(child_type)
    N_array = get_array_child_size(logical_type)
    N_child = N * N_array
    child_writer = VecWriter(child_vec, child_type, child_julia_type, N_child)
    data = (N, N_array, N_child, child_writer) # TODO make struct
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, N, logical_type, T, _setindex_array, validity_mask, data)
end

function _setindex_array(data, value, index)
    N, N_array, N_child, child_writer = data
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

# %% --- Struct ------------------------------------------ #

function _create_vecwriter_struct(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    type_id = get_type_id(logical_type)
    K = get_struct_child_count(logical_type)
    names = Vector{Symbol}()
    writers = Vector{VecWriter}()
    for k in 1:K
        child_type = get_struct_child_type(logical_type, k)
        child_vec = struct_child(vec, k)
        child_writer = VecWriter(child_vec, child_type, T, N)
        push!(writers, child_writer)

        child_name = Symbol(get_struct_child_name(logical_type, k))
        push!(names, child_name)
    end
    names_tuple = Tuple(name for name in names)
    data = NamedTuple{names_tuple}(writers)
    setindex_func = setindex_struct!
    validity_mask = get_validity(vec, N)
    return VecWriter(vec, Int(N), logical_type, T, setindex_func, validity_mask, data)
end

function setindex_struct!(data::NamedTuple{names}, value, index) where {names}
    for name in names
        writer = getindex(data, name)
        writer[index] = getproperty(value, name)
    end
end

# %% ----- List ------------------------------
function _create_vecwriter_list(vec::Vec, logical_type::LogicalType, ::Type{T}, N) where {T}
    # Reserve space for the list entries
    duckdb_list_vector_reserve(vec.handle, N)
    child_vec = list_child(vec)
    child_logical_type = LogicalType(DuckDB.duckdb_vector_get_column_type(child_vec.handle))
    child_julia_type = duckdb_type_to_julia_type(child_logical_type)
    validity_mask = get_validity(vec, N)
    #data = (vec, N, child_logical_type, child_julia_type)
    #entries = get_array(vec, duckdb_list_entry_t, N)
    data = (vec, nothing, N, child_julia_type)
    return VecWriter(vec, Int(N), logical_type, T, _setindex_list, validity_mask, data)
end

function _setindex_list(data, value, index)
    vec, _entries_old, N, child_julia_type = data
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
    for i in 1:N_entry
        j = offset + i
        child_writer[j] = value[i]
    end
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
