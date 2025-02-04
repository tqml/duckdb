




function queryAsync(con, args...; kwargs...)
    task = @task begin
        DuckDB.query(con, args...; kwargs...)
    end
    schedule(task)
    return task
end
