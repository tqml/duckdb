
mutable struct ProfilingInfo
    handle::duckdb_profiling_info

    function ProfilingInfo(info::duckdb_profiling_info)
        con = new(info)
        if con == C_NULL
            throw(DuckDB.ConnectionException("Profiling is not enabled"))
        end
        return con
    end
end
