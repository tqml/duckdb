module DuckDBStaticArraysExt

using StaticArrays
using DuckDB

# %% --------------------------------------------------------
#        Logical Types
#------------------------------------------------------------

"""
    DuckDB.create_logical_type(::Type{V}) where {V <: StaticArrays.SArray}

Creates a logical type for a StaticArray type `V` where `V` is a subtype of `StaticArrays.SArray`.
"""
function DuckDB.create_logical_type(::Type{V}) where {V <: StaticArrays.SArray}
    # TODO Make this recursive -> Array of Array of Array of ...
    S = StaticArrays.Size(V)
    T = eltype(V)
    N = prod(S) # Number of elements in the static array
    child_type = DuckDB.create_logical_type(T)
    GC.@preserve child_type begin
        array_type_handle = DuckDB.duckdb_create_array_type(child_type.handle, N)
        return DuckDB.LogicalType(array_type_handle)
    end
end

# %% --------------------------------------------------------
#        VecReader and VecWriter
#------------------------------------------------------------

"""
    DuckDB.VecWriter(vec, logical_type, ::Type{V}, N) where {V <: StaticArrays.SArray}

Creates a VecWriter that consumes elements of type `V` where `V` is a subtype of `StaticArrays.SArray` and
stores them in a DuckDB Vec. The logical type of the Vec is `logical_type` and the number of elements in the
"""
function DuckDB.VecWriter(
    vec::DuckDB.Vec,
    logical_type::DuckDB.LogicalType,
    ::Type{V},
    N
) where {V <: StaticArrays.SArray}
    S = StaticArrays.Size(V)
    T = eltype(V)

    type_id = DuckDB.get_type_id(logical_type)
    if type_id != DuckDB.DUCKDB_TYPE_ARRAY
        throw(DuckDB.Exception("DuckDB Logical Type is not an array type"))
    end

    child_type_id = DuckDB.get_type_id(DuckDB.get_array_child_type(logical_type))
    # if child_type_id != DuckDB.get_type_id(T)
    #     throw(DuckDB.Exception("DuckDB Logical Type does not match the type of the StaticArray"))
    # end

    N_array = DuckDB.get_array_child_size(logical_type)
    N_array_2 = prod(S)
    if N_array != N_array_2
        throw(DuckDB.Exception("DuckDB Logical Type defines array of size $N_array but got array of size $N_array_2"))
    end

    return DuckDB._create_vecwriter_array(vec, logical_type, V, N)
end


function DuckDB.VecReader(
    vec::DuckDB.Vec,
    logical_type::DuckDB.LogicalType,
    ::Type{V},
    N
) where {V <: StaticArrays.SArray}
    S = StaticArrays.Size(V)
    T = eltype(V)

    N_array = DuckDB.get_array_child_size(logical_type)
    N_array_2 = prod(S)
    if N_array != N_array_2
        throw(DuckDB.Exception("DuckDB Logical Type defines array of size $N_array but got array of size $N_array_2"))
    end

    return DuckDB._create_vecreader_array(vec, logical_type, V, N)
end

end