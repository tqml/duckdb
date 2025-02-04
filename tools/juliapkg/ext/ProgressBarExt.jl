module ProgressBarExt
using ProgressBars

function progress()
    pbar = ProgressBar(total = 100, unit = "%")
    p = duckdb_query_progress(con)
    return update(pbar, round(Int, p * 100))
end

end