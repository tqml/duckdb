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

get_alias(lt::LogicalType) = name(lt)
set_alias!(lt::LogicalType, alias::AbstractString) = duckdb_logical_type_set_alias(lt.handle, alias)

is_struct_type(lt::LogicalType) = get_type_id(lt) == DUCKDB_TYPE_STRUCT
is_union_type(lt::LogicalType)  = get_type_id(lt) == DUCKDB_TYPE_UNION
is_list_type(lt::LogicalType)   = get_type_id(lt) == DUCKDB_TYPE_LIST
is_array_type(lt::LogicalType)  = get_type_id(lt) == DUCKDB_TYPE_ARRAY
is_map_type(lt::LogicalType)    = get_type_id(lt) == DUCKDB_TYPE_MAP

# Missing, Any
create_logical_type(::Type{T}) where {T} =
    throw(NotImplementedException(string("Unsupported type for create_logical_type: ", T)))
create_logical_type(::Type{Union{Missing, T}}) where {T} = create_logical_type(T)
create_logical_type(::Type{Any}) = throw(NotImplementedException("Unsupported type for create_logical_type: Any"))

# Primitive / value types
create_logical_type(::Type{Bool})    = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BOOLEAN)
create_logical_type(::Type{Int8})    = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TINYINT)
create_logical_type(::Type{Int16})   = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_SMALLINT)
create_logical_type(::Type{Int32})   = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_INTEGER)
create_logical_type(::Type{Int64})   = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BIGINT)
create_logical_type(::Type{Int128})  = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_HUGEINT)
create_logical_type(::Type{UInt8})   = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UTINYINT)
create_logical_type(::Type{UInt16})  = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_USMALLINT)
create_logical_type(::Type{UInt32})  = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UINTEGER)
create_logical_type(::Type{UInt64})  = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UBIGINT)
create_logical_type(::Type{UInt128}) = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UHUGEINT)
create_logical_type(::Type{Float32}) = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_FLOAT)
create_logical_type(::Type{Float64}) = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_DOUBLE)
create_logical_type(::Type{UUID})    = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UUID)

create_logical_type(::Type{Date})                                = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_DATE)
create_logical_type(::Type{Time})                                = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TIME)
create_logical_type(::Type{DateTime})                            = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TIMESTAMP)
create_logical_type(::Type{T}) where {T <: Period}               = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_INTERVAL)
create_logical_type(::Type{T}) where {T <: Dates.CompoundPeriod} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_INTERVAL)

create_logical_type(::Type{T}) where {T <: AbstractString} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_VARCHAR)
create_logical_type(::Type{Base.CodeUnits{UInt8, String}}) = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BLOB)



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

function create_logical_type(::Type{T}) where {K, V, T <: AbstractDict{K, V}}
    key_type = create_logical_type(K)
    value_type = create_logical_type(V)
    GC.@preserve key_type value_type begin
        map_type = duckdb_create_map_type(key_type.handle, value_type.handle)
        return LogicalType(map_type)
    end
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

function get_union_member_name(type::LogicalType, index)
    val = duckdb_union_type_member_name(type.handle, index)
    result = unsafe_string(val)
    duckdb_free(val)
    return result
end

function get_union_member_type(type::LogicalType, index)
    return LogicalType(duckdb_union_type_member_type(type.handle, index))
end


function create_union_type(types)
    member_types = [create_logical_type(t) for t in types]
    member_names = [string(i) for i in 1:length(types)]
    K = length(types)
    GC.@preserve member_types member_names begin
        member_types_handles = [member.handle for member in member_types]
        union_type = LogicalType(duckdb_create_union_type(member_types_handles, member_names, K))
        if union_type.handle == C_NULL
            throw(ArgumentError("Failed to create union type"))
        end
        return union_type
    end
end


function create_union_type(type::Union)
    # Union{T1,T2,T3,...}
    return create_union_type(_union_types(type))
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

function get_array_child_type(type::LogicalType)
    return LogicalType(duckdb_array_type_child_type(type.handle))
end

function get_array_child_size(type::LogicalType)
    return Int(duckdb_array_type_array_size(type.handle))
end


# %% --- Debugging ------------------------------------------ #


function _show_type_tree(lt::LogicalType, indent = 0)
    type_id = get_type_id(lt)
    if type_id == DUCKDB_TYPE_STRUCT
        println("$(repeat(' ', indent))Struct: $(name(lt))")
        for i in 1:get_struct_child_count(lt)
            child_name = get_struct_child_name(lt, i)
            child_type = get_struct_child_type(lt, i)
            child_type_id = get_type_id(child_type)
            println("$(repeat(' ', indent + 2))$child_name: $(child_type_id)")
            _show_type_tree(child_type, indent + 4)
        end
    elseif type_id == DUCKDB_TYPE_UNION
        println("$(repeat(' ', indent))Union: $(name(lt))")
        for i in 1:get_union_member_count(lt)
            member_name = get_union_member_name(lt, i)
            member_type = get_union_member_type(lt, i)
            member_type_id = get_type_id(member_type)
            println("$(repeat(' ', indent + 2))$member_name: $(member_type_id)")
            _show_type_tree(member_type, indent + 4)
        end
    elseif type_id == DUCKDB_TYPE_LIST
        println("$(repeat(' ', indent))List: $(name(lt))")
        child_type = get_list_child_type(lt)
        _show_type_tree(child_type, indent + 2)
    elseif type_id == DUCKDB_TYPE_ARRAY
        println("$(repeat(' ', indent))Array: $(name(lt))")
        child_type = get_array_child_type(lt)
        println("$(repeat(' ', indent + 2))Size: $(get_array_child_size(lt))")
        _show_type_tree(child_type, indent + 2)
    elseif type_id == DUCKDB_TYPE_MAP
        println("$(repeat(' ', indent))Map: $(name(lt))")
        key_type = get_map_key_type(lt)
        value_type = get_map_value_type(lt)
        println("$(repeat(' ', indent + 2))Key: $(name(key_type))")
        _show_type_tree(key_type, indent + 4)
        println("$(repeat(' ', indent + 2))Value: $(name(value_type))")
        _show_type_tree(value_type, indent + 4)
    else
        println("$(repeat(' ', indent))$(type_id)")
    end
end
