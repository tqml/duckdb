using Pkg
#Pkg.instantiate()Pkg.develop(PackageSpec(path=dirname(@__DIR__)))
Pkg.develop(PackageSpec(path=dirname(@__DIR__)))
using PkgBenchmark, DuckDB


results = benchmarkpkg("DuckDB")
export_markdown("benchmark.md", results)
writeresults("DuckDB_benchmark.json", results)


# results.benchmarkgroup["ColumnConversion"]
# m1 = results.benchmarkgroup["ColumnConversion"]["read_Float64_ColumnConversion"]
# m2 = results.benchmarkgroup["ColumnConversion"]["read_Float64_VecReader"]
# judge(median(m1), median(m2))
# judge(ratio(m1), ratio(m2))

# JUDGEMENTS = BenchmarkGroup()
# for pair in column_conversion_judgement_pairs
#     m1 = results.benchmarkgroup["ColumnConversion"][pair[1]]
#     m2 = results.benchmarkgroup["ColumnConversion"][pair[2]]
#     judge(median(m1), median(m2))
#     judge(ratio(m1), ratio(m2))
# end

#=

shell> cd benchmark
(@v1.9) pkg> activate .
  Activating project at `~/.julia/dev/DuckDB/benchmark`
julia-1.9> include("run-pkgbenchmark.jl")

=#
