module DuckDBStaticArraysExt

using StaticArrays
using DuckDB




function DuckDB.create_logical_type(::Type{StaticArray{T, N}}) where {T, N}
    if N == 0
        throw(DuckDB.NotImplementedException("create_logical_type not implemented for StaticArray{0}"))
    end
    child_type = DuckDB.create_logical_type(T)
    array_type_handle = DuckDB.duckdb_create_array_type(child_type.handle, N)
    return DuckDB.LogicalType(array_type_handle, [child_type])
end


end