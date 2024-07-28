#=
//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
=#

_is_vector_expression(x::Expr) = x.head == :vect
_is_vector_expression(x) = false


macro _wrap_scalar_function(func_expr, return_type::Symbol, parameter_types::Expr)
    _is_vector_expression(parameter_types) || throw(ArgumentError("parameter_types must be a vector expression"))
    vector_elements = parameter_types.args
    return _wrap_scalar_function(func_expr, return_type, vector_elements)
end

function _wrap_scalar_function(func, return_type, parameter_types)
    # Create a scalar function with fixed return and parameter types
    args = [Symbol("a$i") for i in 1:length(parameter_types)]
    args_with_types = [Expr(:(::), Symbol("a$i"), parameter_types[i]) for i in 1:length(parameter_types)]
    args_escaped = esc.(args)
    args_with_types_escaped = esc.(args_with_types)

    return quote
        local _f = $(esc(func))
        return function ($(args_with_types_escaped...))
            local ret::$(esc(return_type)) = _f($(args_escaped...))
            return ret
        end
    end
end



macro _create_duckdb_scalar_function_wrapper(parameter_types, return_type)
    local vector_elements = esc.(parameter_types.args)
    local data_access_var = [Symbol("data$(i)") for i in 1:length(vector_elements)]
    local data_access_assign_expr =
        [:($(data_access_var[i]) = get_array(data_chunk, $i, $(vector_elements[i]))) for i in 1:length(vector_elements)]
    local true_return_type = esc(return_type)

    return quote
        (info::duckdb_function_info, chunk::duckdb_data_chunk, output::duckdb_vector) -> begin
            println("Start calling the wrapper function")
            scalar_function::ScalarFunction = unsafe_pointer_to_objref(duckdb_scalar_function_get_extra_info(info))
            data_chunk = DataChunk(chunk, false) # create a data chunk object, that does not own the data
            data_chunk_size = get_size(data_chunk) # get the size of the columns in the data chunk
            output_vec = Vec(output)

            param_types::Vector{LogicalType} = scalar_function.parameters
            logical_return_type::LogicalType = scalar_function.return_type
            n_cols = get_column_count(data_chunk)
            result_array = get_array(output_vec, $(true_return_type))

            $(data_access_assign_expr...)

            try
                for i in 1:data_chunk_size
                    result::$true_return_type =
                        scalar_function.func($([:($(data_access_var[j])[i]) for j in 1:length(vector_elements)]...))
                    result_array[i] = result
                end
            catch
                return duckdb_function_set_error(info, "ScalarFuncExc - " * get_exception_info())
            end
            return nothing
        end
    end

end



# typedef void (*duckdb_scalar_function_t)(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);
function _scalar_function(info::duckdb_function_info, chunk::duckdb_data_chunk, output::duckdb_vector)
    scalar_function::ScalarFunction = unsafe_pointer_to_objref(duckdb_scalar_function_get_extra_info(info))
    #function_info::ScalarFunctionInfo = ScalarFunctionInfo(info, scalar_function)

    data_chunk = DataChunk(chunk, false)
    output_vec = Vec(output)
    param_types::Vector{LogicalType} = scalar_function.parameters
    return_type::LogicalType = scalar_function.return_type
    data_chunk_size = get_size(data_chunk) # get the size of the columns in the data chunk

    try
        n_cols = duckdb_data_chunk_get_column_count(chunk)
        # #@info "n_cols: $n_cols"
        #data = [get_array(data_chunk, Int64(i), Int64) for i in 1:(n_cols)]
        # #@info "output_size: $output_size"
        result_array = DuckDB.get_array(output_vec, Int64)
        #output_size = duckdb_vector_size(v.handle)
        n = VECTOR_SIZE
        println("n_cols: $n_cols")
        #println("output_size: $output_size")
        println("VECTOR_SIZE: $n")
        println("data_chunk_size: $data_chunk_size")

        data = (get_array(data_chunk, i, Int64) for i in 1:n_cols)
        d = get_array(data_chunk, 1, Int64)
        #data_validity = get_validity(data_chunk, 1)
        #println("data: ", data)
        #println("validty: ", data_validity)


        # # #count = 0
        for i in 1:data_chunk_size

            # if n_cols == 1
            #     result = scalar_function.func(data[1][i])
            #     result_array[i] = result
            # else
            #     result = scalar_function.func(data[1][i], data[2][i])
            #     result_array[i] = result
            # end
            #inputs = (d[i]::Int64 for d in data)
            result = scalar_function.func(d[i])
            #result = scalar_function.func(inputs...)
            result_array[i] = result
            #@info "inputs" inputs result

        end

    catch
        duckdb_function_set_error(info, "ScalarFuncExc - " * get_exception_info())
    end

    return
end

struct ScalarFunctionInfo
    handle::duckdb_function_info
    main_function::Any

    function ScalarFunctionInfo(handle::duckdb_function_info, main_function)
        result = new(handle, main_function)
        return result
    end
end


#=
//===--------------------------------------------------------------------===//
// Scalar Function
//===--------------------------------------------------------------------===//
=#

mutable struct ScalarFunction
    handle::duckdb_scalar_function
    name::AbstractString
    parameters::Vector{LogicalType}
    return_type::LogicalType
    func::Function
    wrapper::Union{Nothing, Function} # the wrapper function to hold a reference to it to prevent GC

    function ScalarFunction(
        name::AbstractString,
        parameters::Vector{LogicalType},
        return_type::LogicalType,
        func::Function,
        wrapper::Union{Nothing, Function} = nothing
    )
        handle = duckdb_create_scalar_function()
        duckdb_scalar_function_set_name(handle, name)
        for param in parameters
            duckdb_scalar_function_add_parameter(handle, param.handle)
        end
        duckdb_scalar_function_set_return_type(handle, return_type.handle)
        result = new(handle, name, parameters, return_type, func, wrapper)
        finalizer(_destroy_scalar_function, result)
        duckdb_scalar_function_set_extra_info(handle, pointer_from_objref(result))

        # Use the generic wrapper if no custom wrapper is provided
        if wrapper === nothing
            # typedef void (*duckdb_scalar_function_t)(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);
            duckdb_scalar_function_set_function(
                handle,
                @cfunction(_scalar_function, Cvoid, (duckdb_function_info, duckdb_data_chunk, duckdb_vector))
            )
        end
        return result
    end

end

function _destroy_scalar_function(func::ScalarFunction)
    # disconnect from DB
    if func.handle != C_NULL
        duckdb_destroy_scalar_function(func.handle)
    end
    func.handle = C_NULL
    return
end

# function create_scalar_function(
#     con::Connection,
#     name::AbstractString,
#     parameter_types::Vector{LogicalType},
#     return_type::LogicalType,
#     func::Function
# )

#     fun = ScalarFunction(name, parameter_types, return_type, func)
#     result = duckdb_register_scalar_function(con.handle, fun.handle)
#     if result != DuckDBSuccess
#         throw(QueryException(string("Failed to register scalar function \"", name, "\"")))
#     end
#     push!(con.db.functions, fun) # TODO should this be a different list?
#     return
# end

"""
Create a scalar function in the database.

# Arguments
- `db::DB`: The database object.
- `name::AbstractString`: The name of the function.
- `parameters::Vector{DataType}`: The types of the parameters.
- `return_type::DataType`: The return type of the function.
- `func::Function`: The function to call.

# Example
```jldoctest
using DuckDB

db = DuckDB.DB(":memory:")
function add(a::Int64, b::Int64)
    return a + b
end

DuckDB.create_scalar_function(db, "add", [Int64, Int64], Int64, add)
```

# Note
- The function must be a Julia function that takes the same number of arguments as the length of `parameters`.
- The function must return a value of the same type as `return_type`.


"""
function create_scalar_function(
    con::Connection,
    name::AbstractString,
    parameters::Vector{DataType},
    return_type::DataType,
    func::Function
)

    parameter_types::Vector{LogicalType} = Vector()
    for parameter_type in parameters
        push!(parameter_types, create_logical_type(parameter_type))
    end

    # Define and register the scalar function
    fun = ScalarFunction(name, parameter_types, create_logical_type(return_type), func, nothing)

    # define the wrapper for the function
    duckdb_scalar_function_set_function(
        fun.handle,
        @cfunction(_scalar_function, Cvoid, (duckdb_function_info, duckdb_data_chunk, duckdb_vector))
    )

    result = duckdb_register_scalar_function(con.handle, fun.handle)
    if result != DuckDBSuccess
        throw(QueryException(string("Failed to register scalar function \"", name, "\"")))
    end

    push!(con.db.functions, fun) # TODO should this be a different list?
    return
end



create_scalar_function(db::DB, name, parameters, return_type, func) =
    create_scalar_function(db.main_connection, name, parameters, return_type, func)



"""
    @create_scalar_function(db, name, parameter, return_type, func)

Create a scalar function in the database.

# Example

```jldoctest
@create_scalar_function "my_add" [Int64, Int64] Int64 (a,b)->a+b
```

# Arguments
- `db::DB`: The database object.
- `name::AbstractString`: The name of the function.
- `parameter::DataType`: The type of the parameter.
- `return_type::DataType`: The return type of the function.
- `func::Function`: The function to call.
"""
macro create_scalar_function(name, parameters, return_type, func)
    # generate a wrapper for the function
    func_wrapper = eval(:(@_create_duckdb_scalar_function_wrapper($parameters, $return_type)))
    func_pointer = :(@cfunction($func_wrapper, Core.Cvoid, (duckdb_function_info, duckdb_data_chunk, duckdb_vector)))
    # create the scalar function
    quote

        #local func_wrapper = @_create_duckdb_scalar_function_wrapper($parameters, $return_type)
        local parameter_types = Vector{LogicalType}()
        for parameter_type in $(esc(parameters))
            Base.push!(parameter_types, create_logical_type(parameter_type))
        end

        fun = ScalarFunction($name, parameter_types, create_logical_type($return_type), $(esc(func)), $func_wrapper) # do not use generic wrapper
        duckdb_scalar_function_set_function(fun.handle, $func_pointer)
        fun
    end
end







function register_scalar_function(con::Connection, fun::ScalarFunction)
    result = duckdb_register_scalar_function(con.handle, fun.handle)
    if result != DuckDBSuccess
        throw(QueryException(string("Failed to register scalar function \"", fun.name, "\"")))
    end
    push!(con.db.functions, fun) # TODO should this be a different list?
    return
end

register_scalar_function(db::DB, fun::ScalarFunction) = register_scalar_function(db.main_connection, fun)

function unregister_scalar_function(con::Connection, fun::ScalarFunction)
    # remove the function from the list
    fun_ix = findfirst(fun1 -> fun1 === fun, con.db.functions)
    if fun_ix === nothing
        throw(QueryException(string("Scalar function \"", fun.name, "\" not found")))
    end
    deleteat!(con.db.functions, fun_ix)
    return duckdb_destroy_scalar_function(fun.handle)
end

function unregister_scalar_function(con::Connection, name::AbstractString)
    fun_ix = findfirst(fun -> typeof(fun) == ScalarFunction && fun.name == name, con.db.functions)
    if fun_ix === nothing
        throw(QueryException(string("Scalar function \"", name, "\" not found")))
    end
    fun = con.db.functions[fun_ix]
    return unregister_scalar_function(con, fun)
end

unregister_scalar_function(db::DB, fun::ScalarFunction) = unregister_scalar_function(db.main_connection, fun)
unregister_scalar_function(db::DB, name::AbstractString) = unregister_scalar_function(db.main_connection, name)