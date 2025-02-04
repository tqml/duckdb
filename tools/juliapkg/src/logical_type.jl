"""
DuckDB type
"""
mutable struct LogicalType
    handle::duckdb_logical_type

    function LogicalType(type::DUCKDB_TYPE)
        handle = duckdb_create_logical_type(type)
        result = new(handle)
        finalizer(_destroy_type, result)
        return result
    end
    function LogicalType(handle::duckdb_logical_type)
        result = new(handle)
        finalizer(_destroy_type, result)
        return result
    end
end

function _destroy_type(type::LogicalType)
    if type.handle != C_NULL
        duckdb_destroy_logical_type(type.handle)
    end
    type.handle = C_NULL
    return
end

function name(lt::LogicalType)
    alias_ptr = duckdb_logical_type_get_alias(lt.handle)
    if alias_ptr == C_NULL
        return ""
    end
    alias = unsafe_string(alias_ptr)
    duckdb_free(alias_ptr)
    return alias
end

alias(lt::LogicalType) = name(lt)
set_alias!(lt::LogicalType, alias::AbstractString) = duckdb_logical_type_set_alias(lt.handle, alias)


is_struct_type(lt::LogicalType) = get_type_id(lt) == DUCKDB_TYPE_STRUCT
is_union_type(lt::LogicalType) = get_type_id(lt) == DUCKDB_TYPE_UNION
is_list_type(lt::LogicalType) = get_type_id(lt) == DUCKDB_TYPE_LIST
is_array_type(lt::LogicalType) = get_type_id(lt) == DUCKDB_TYPE_ARRAY
is_map_type(lt::LogicalType) = get_type_id(lt) == DUCKDB_TYPE_MAP

is_complex_type(lt::LogicalType) = is_complex_type(get_type_id(lt))
is_complex_type(handle::duckdb_logical_type) = is_complex_type(duckdb_get_type_id(handle)::DUCKDB_TYPE)
function is_complex_type(type::DUCKDB_TYPE)
    return type in (
        DUCKDB_TYPE_ENUM,
        DUCKDB_TYPE_LIST,
        DUCKDB_TYPE_STRUCT,
        DUCKDB_TYPE_MAP,
        DUCKDB_TYPE_ARRAY,
        DUCKDB_TYPE_UNION
    )
end


# Missing, Any
create_logical_type(::Type{T}) where {T} =
    throw(NotImplementedException(string("Unsupported type for create_logical_type: ", T)))
create_logical_type(::Type{Union{Missing, T}}) where {T} = create_logical_type(T)
create_logical_type(::Type{Any}) = throw(NotImplementedException("Unsupported type for create_logical_type: Any"))

# Primitive / value types
create_logical_type(::Type{T}) where {T <: Bool} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BOOLEAN)
create_logical_type(::Type{T}) where {T <: Int8} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TINYINT)
create_logical_type(::Type{T}) where {T <: Int16} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_SMALLINT)
create_logical_type(::Type{T}) where {T <: Int32} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_INTEGER)
create_logical_type(::Type{T}) where {T <: Int64} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BIGINT)
create_logical_type(::Type{T}) where {T <: Int128} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_HUGEINT)
create_logical_type(::Type{T}) where {T <: UInt8} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UTINYINT)
create_logical_type(::Type{T}) where {T <: UInt16} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_USMALLINT)
create_logical_type(::Type{T}) where {T <: UInt32} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UINTEGER)
create_logical_type(::Type{T}) where {T <: UInt64} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UBIGINT)
create_logical_type(::Type{T}) where {T <: UInt128} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UHUGEINT)
create_logical_type(::Type{T}) where {T <: Float32} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_FLOAT)
create_logical_type(::Type{T}) where {T <: Float64} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_DOUBLE)
create_logical_type(::Type{T}) where {T <: Date} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_DATE)
create_logical_type(::Type{T}) where {T <: Time} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TIME)
create_logical_type(::Type{T}) where {T <: DateTime} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TIMESTAMP)
create_logical_type(::Type{T}) where {T <: Period} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_INTERVAL)
create_logical_type(::Type{T}) where {T <: Dates.CompoundPeriod} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_INTERVAL)
create_logical_type(::Type{T}) where {T <: AbstractString} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_VARCHAR)
create_logical_type(::Type{UUID}) = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UUID)

function create_logical_type(::Type{T}) where {T <: FixedDecimal}
    int_type = T.parameters[1]
    width = 0
    scale = T.parameters[2]
    if int_type == Int16
        width = 4
    elseif int_type == Int32
        width = 9
    elseif int_type == Int64
        width = 18
    elseif int_type == Int128
        width = 38
    else
        throw(NotImplementedException("Unsupported internal type for decimal"))
    end
    return DuckDB.LogicalType(duckdb_create_decimal_type(width, scale))
end

function get_type_id(type::LogicalType)::DUCKDB_TYPE
    return duckdb_get_type_id(type.handle)
end

function get_internal_type_id(type::LogicalType)
    type_id = get_type_id(type)
    if type_id == DUCKDB_TYPE_DECIMAL
        type_id = duckdb_decimal_internal_type(type.handle)
    elseif type_id == DUCKDB_TYPE_ENUM
        type_id = duckdb_enum_internal_type(type.handle)
    end
    return type_id
end

function get_decimal_scale(type::LogicalType)
    return duckdb_decimal_scale(type.handle)
end

function get_enum_dictionary(type::LogicalType)
    dict::Vector{String} = Vector{String}()
    dict_size = duckdb_enum_dictionary_size(type.handle)
    for i in 1:dict_size
        val = duckdb_enum_dictionary_value(type.handle, i)
        str_val = String(unsafe_string(val))
        push!(dict, str_val)
        duckdb_free(val)
    end
    return dict
end

function get_list_child_type(type::LogicalType)
    return LogicalType(duckdb_list_type_child_type(type.handle))
end

##===--------------------------------------------------------------------===##
## Dict methods
##===--------------------------------------------------------------------===##

function get_map_key_type(type::LogicalType)
    return LogicalType(duckdb_map_type_key_type(type.handle))
end

function get_map_value_type(type::LogicalType)
    return LogicalType(duckdb_map_type_value_type(type.handle))
end


##===--------------------------------------------------------------------===##
## Struct methods
##===--------------------------------------------------------------------===##

function get_struct_child_count(type::LogicalType)
    return Int(duckdb_struct_type_child_count(type.handle))
end


function get_struct_child_name(type::LogicalType, index)
    val = duckdb_struct_type_child_name(type.handle, index)
    result = unsafe_string(val)
    duckdb_free(val)
    return result
end

function get_struct_child_type(type::LogicalType, index)
    return LogicalType(duckdb_struct_type_child_type(type.handle, index))
end

# function create_logical_type(::Type{T}) where {T}
#     if !isstructtype(T)
#         throw(NotImplementedException("Unsupported type for create_logical_type: $T"))
#     end

#     names = fieldnames(T)
#     types = fieldtypes(T)
#     N = length(names)
#     member_names = [string(name) for name in names]
#     member_types = [create_logical_type(type) for type in types]
#     Base.GC.@preserve member_types begin
#         member_type_ptrs = [member.handle for member in member_types]
#         struct_handle = duckdb_create_struct_type(member_type_ptrs, member_names, N)
#         struct_type_name = "JL" * string(nameof(T))
#         duckdb_logical_type_set_alias(struct_handle, struct_type_name)
#         return DuckDB.LogicalType(struct_handle)
#     end
# end

function create_logical_type(::Type{NamedTuple{names, T}}) where {names, T <: Tuple}
    n = length(names)
    member_names = [string(name) for name in names]
    member_types = [create_logical_type(type) for type in T.types]
    Base.GC.@preserve member_types begin
        member_type_ptrs = [member.handle for member in member_types]
        return DuckDB.LogicalType(duckdb_create_struct_type(member_type_ptrs, member_names, n))
    end
end

##===--------------------------------------------------------------------===##
## Union methods
##===--------------------------------------------------------------------===##

function get_union_member_count(type::LogicalType)
    return duckdb_union_type_member_count(type.handle)
end

function get_union_member_name(type::LogicalType, index::UInt64)
    val = duckdb_union_type_member_name(type.handle, index)
    result = unsafe_string(val)
    duckdb_free(val)
    return result
end

function get_union_member_type(type::LogicalType, index::UInt64)
    return LogicalType(duckdb_union_type_member_type(type.handle, index))
end


function create_union_type(types)
    member_types = [create_logical_type(t) for t in types]
    member_names = [string(i) for i in 1:length(types)]
    K = length(types)
    GC.@preserve member_types member_names begin
        member_types_handles = [member.handle for member in member_types]
        println("Create Union Type")
        union_type = LogicalType(duckdb_create_union_type(member_types_handles, member_names, K))
        println("Union Type created")
        if union_type.handle == C_NULL
            throw(ArgumentError("Failed to create union type"))
        end
        return union_type
    end
end

##===--------------------------------------------------------------------===##
## List & Array methods
##===--------------------------------------------------------------------===##

function create_logical_type(::Type{T}) where {T <: AbstractArray}
    X = eltype(T)
    child_type = create_logical_type(X)
    list_type = duckdb_create_list_type(child_type.handle)
    return DuckDB.LogicalType(list_type)
end

function create_logical_type(::Type{NTuple{N, U}}) where {N, U}
    if N == 0
        throw(NotImplementedException("create_logical_type not implemented for Tuple{}"))
    end
    child_type = create_logical_type(U)
    array_type_handle = duckdb_create_array_type(child_type.handle, N)
    return DuckDB.LogicalType(array_type_handle)
end

function create_logical_type(::Type{T}) where {K, V, T <: AbstractDict{K, V}}
    key_type = create_logical_type(K)
    value_type = create_logical_type(V)
    GC.@preserve key_type value_type begin
        map_type = duckdb_create_map_type(key_type.handle, value_type.handle)
        return LogicalType(map_type)
    end
end

function get_array_child_type(type::LogicalType)
    return LogicalType(duckdb_array_type_child_type(type.handle))
end

function get_array_child_size(type::LogicalType)
    return Int(duckdb_array_type_array_size(type.handle))
end

# %% --- Inverse Operations ------------------------------------------ #

function to_internal_julia_type(lt::LogicalType)
    return to_internal_julia_type(get_type_id(lt))
end

function to_julia_type(lt::LogicalType)
    return duckdb_type_to_julia_type(lt.handle)
end
