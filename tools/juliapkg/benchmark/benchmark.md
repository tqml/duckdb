# Benchmark Report for *DuckDB*

## Job Properties
* Time of benchmark: 13 Jan 2025 - 13:28
* Package commit: non gi
* Julia commit: 5e9a32
* Julia command flags: None
* Environment variables: None

## Results
Below is a table of this job's results, obtained by running the benchmarks.
The values listed in the `ID` column have the structure `[parent_group, child_group, ..., key]`, and can be used to
index into the BaseBenchmarks suite to retrieve the corresponding benchmarks.
The percentages accompanying time and memory values in the below table are noise tolerances. The "true"
time/memory value for a given benchmark is expected to fall within this percentage of the reported value.
An empty cell means that the value was zero.

| ID                                                                              | time            | GC time    | memory          | allocations |
|---------------------------------------------------------------------------------|----------------:|-----------:|----------------:|------------:|
| `["ColumnConversion", "read_Bool_ColumnConversion"]`                            |  14.416 μs (5%) |            |  21.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Bool_VecReader"]`                                   |  53.167 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_DateTime_ColumnConversion"]`                        |  22.250 μs (5%) |            | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_DateTime_VecReader"]`                               |  55.750 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Date_ColumnConversion"]`                            |  16.208 μs (5%) |            | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Date_VecReader"]`                                   |  53.333 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Dates.CompoundPeriod_ColumnConversion"]`            |   5.166 ms (5%) |            |   2.66 MiB (1%) |      102475 |
| `["ColumnConversion", "read_Dates.CompoundPeriod_VecReader"]`                   |   5.162 ms (5%) |            |   2.50 MiB (1%) |      102452 |
| `["ColumnConversion", "read_Dict{String, Int64}_ColumnConversion"]`             |    3.147 s (5%) | 528.439 ms |   1.80 GiB (1%) |    52614919 |
| `["ColumnConversion", "read_Dict{String, Int64}_VecReader"]`                    | 119.882 ms (5%) |            |  69.29 MiB (1%) |     1956622 |
| `["ColumnConversion", "read_Dict{String, String}_ColumnConversion"]`            | 104.631 ms (5%) |            |  70.70 MiB (1%) |     1798817 |
| `["ColumnConversion", "read_Dict{String, String}_VecReader"]`                   |  74.963 ms (5%) |            |  54.36 MiB (1%) |     1342202 |
| `["ColumnConversion", "read_Dict{String, Vector{Int64}}_ColumnConversion"]`     |    1.711 s (5%) | 274.210 ms |   1.26 GiB (1%) |    30399083 |
| `["ColumnConversion", "read_Dict{String, Vector{Int64}}_VecReader"]`            |  89.368 ms (5%) |            |  58.36 MiB (1%) |     1444702 |
| `["ColumnConversion", "read_Float32_ColumnConversion"]`                         |  15.375 μs (5%) |            |  81.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Float32_VecReader"]`                                |  52.084 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Float64_ColumnConversion"]`                         |  14.458 μs (5%) |            | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Float64_VecReader"]`                                |  52.458 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int128_ColumnConversion"]`                          |  16.333 μs (5%) |            | 321.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int128_VecReader"]`                                 |  54.917 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int16_ColumnConversion"]`                           |  14.292 μs (5%) |            |  41.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int16_VecReader"]`                                  |  52.166 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int32_ColumnConversion"]`                           |  14.416 μs (5%) |            |  81.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int32_VecReader"]`                                  |  51.291 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int64_ColumnConversion"]`                           |  14.625 μs (5%) |            | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int64_VecReader"]`                                  |  52.875 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int8_ColumnConversion"]`                            |  13.875 μs (5%) |            |  21.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int8_VecReader"]`                                   |  52.541 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_NTuple{10, Int64}_VecReader"]`                      |   4.281 ms (5%) |            |   2.74 MiB (1%) |       56472 |
| `["ColumnConversion", "read_SArray{Tuple{2, 2, 2}, Int64, 3}_VecReader"]`       |   7.823 ms (5%) |            |   6.49 MiB (1%) |      240792 |
| `["ColumnConversion", "read_SMatrix{2, 2, Int64}_VecReader"]`                   |   5.781 ms (5%) |            |   3.99 MiB (1%) |      158872 |
| `["ColumnConversion", "read_SVector{10, Int64}_VecReader"]`                     |  11.079 ms (5%) |            |   7.74 MiB (1%) |      281752 |
| `["ColumnConversion", "read_String_ColumnConversion"]`                          | 289.792 μs (5%) |            | 940.83 KiB (1%) |       20535 |
| `["ColumnConversion", "read_String_VecReader"]`                                 | 401.083 μs (5%) |            | 781.52 KiB (1%) |       20532 |
| `["ColumnConversion", "read_Time_ColumnConversion"]`                            | 166.667 μs (5%) |            | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Time_VecReader"]`                                   | 203.667 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt128_ColumnConversion"]`                         |  16.291 μs (5%) |            | 321.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt128_VecReader"]`                                |  55.416 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt16_ColumnConversion"]`                          |  14.458 μs (5%) |            |  41.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt16_VecReader"]`                                 |  52.000 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt32_ColumnConversion"]`                          |  14.709 μs (5%) |            |  81.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt32_VecReader"]`                                 |  52.833 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt64_ColumnConversion"]`                          |  14.584 μs (5%) |            | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt64_VecReader"]`                                 |  52.375 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt8_ColumnConversion"]`                           |  14.041 μs (5%) |            |  21.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt8_VecReader"]`                                  |  52.750 μs (5%) |            |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Vector{Int64}_ColumnConversion"]`                   |  15.807 ms (5%) |            |  74.47 MiB (1%) |       81649 |
| `["ColumnConversion", "read_Vector{Int64}_VecReader"]`                          |   1.813 ms (5%) |            |   2.11 MiB (1%) |       56462 |
| `["ColumnConversion", "read_Vector{Vector{Int64}}_ColumnConversion"]`           | 251.484 ms (5%) |  23.470 ms | 335.80 MiB (1%) |     6632495 |
| `["ColumnConversion", "read_Vector{Vector{Int64}}_VecReader"]`                  |  14.019 ms (5%) |            |  12.74 MiB (1%) |      322782 |
| `["ColumnConversion", "read_Vector{Vector{Vector{Float64}}}_ColumnConversion"]` | 394.123 ms (5%) |  57.238 ms | 478.31 MiB (1%) |     9904383 |
| `["ColumnConversion", "read_Vector{Vector{Vector{Float64}}}_VecReader"]`        |  62.456 ms (5%) |            |  65.87 MiB (1%) |     1654062 |
| `["ColumnConversion", "read_Vector{Vector{Vector{Int64}}}_ColumnConversion"]`   | 395.574 ms (5%) |  54.026 ms | 478.36 MiB (1%) |     9905583 |
| `["ColumnConversion", "read_Vector{Vector{Vector{Int64}}}_VecReader"]`          |  75.197 ms (5%) |            |  65.81 MiB (1%) |     1649962 |
| `["ColumnConversion", "write_Bool_VecWriter"]`                                  |  14.291 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_DateTime_VecWriter"]`                              |  16.125 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Date_VecWriter"]`                                  |  18.417 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Dates.CompoundPeriod_VecWriter"]`                  |   4.370 ms (5%) |            |   1.88 MiB (1%) |       61553 |
| `["ColumnConversion", "write_Dict{String, Int64}_VecWriter"]`                   |  89.836 ms (5%) |            |  37.82 MiB (1%) |     1331333 |
| `["ColumnConversion", "write_Dict{String, String}_VecWriter"]`                  |  81.974 ms (5%) |            |  27.19 MiB (1%) |      942213 |
| `["ColumnConversion", "write_Dict{String, Vector{Int64}}_VecWriter"]`           | 265.033 ms (5%) |            |  75.00 MiB (1%) |     3072143 |
| `["ColumnConversion", "write_Float32_VecWriter"]`                               |  14.333 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Float64_VecWriter"]`                               |  14.291 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int128_VecWriter"]`                                |  19.125 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int16_VecWriter"]`                                 |  14.041 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int32_VecWriter"]`                                 |  14.334 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int64_VecWriter"]`                                 |  14.458 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int8_VecWriter"]`                                  |  14.375 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_NTuple{10, Int64}_VecWriter"]`                     | 226.250 μs (5%) |            |   3.48 KiB (1%) |          93 |
| `["ColumnConversion", "write_SArray{Tuple{2, 2, 2}, Int64, 3}_VecWriter"]`      | 195.000 μs (5%) |            |   3.64 KiB (1%) |         103 |
| `["ColumnConversion", "write_SMatrix{2, 2, Int64}_VecWriter"]`                  | 121.042 μs (5%) |            |   3.64 KiB (1%) |         103 |
| `["ColumnConversion", "write_SVector{10, Int64}_VecWriter"]`                    | 233.541 μs (5%) |            |   3.64 KiB (1%) |         103 |
| `["ColumnConversion", "write_String_VecWriter"]`                                | 710.916 μs (5%) |            |   1.30 KiB (1%) |          33 |
| `["ColumnConversion", "write_Time_VecWriter"]`                                  |  17.583 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt128_VecWriter"]`                               |  19.208 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt16_VecWriter"]`                                |  14.208 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt32_VecWriter"]`                                |  14.334 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt64_VecWriter"]`                                |  14.084 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt8_VecWriter"]`                                 |  14.334 μs (5%) |            |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Vector{Int64}_VecWriter"]`                         |  18.457 ms (5%) |            |   9.06 MiB (1%) |      409643 |
| `["ColumnConversion", "write_Vector{Vector{Int64}}_VecWriter"]`                 | 164.638 ms (5%) |            |  52.19 MiB (1%) |     2334783 |
| `["ColumnConversion", "write_Vector{Vector{Vector{Float64}}}_VecWriter"]`       |    2.427 s (5%) |  32.601 ms | 268.75 MiB (1%) |    12001363 |
| `["ColumnConversion", "write_Vector{Vector{Vector{Int64}}}_VecWriter"]`         |    2.439 s (5%) |  33.168 ms | 268.75 MiB (1%) |    12001363 |
| `["UDF", "float_add"]`                                                          |  40.419 ms (5%) |            |  46.05 MiB (1%) |     3008814 |

## Benchmark Group List
Here's a list of all the benchmark groups executed by this job:

- `["ColumnConversion"]`
- `["UDF"]`

## Julia versioninfo
```
Julia Version 1.11.2
Commit 5e9a32e7af2 (2024-12-01 20:02 UTC)
Build Info:
  Official https://julialang.org/ release
Platform Info:
  OS: macOS (arm64-apple-darwin24.0.0)
  uname: Darwin 24.2.0 Darwin Kernel Version 24.2.0: Fri Dec  6 19:01:59 PST 2024; root:xnu-11215.61.5~2/RELEASE_ARM64_T6000 arm64 arm
  CPU: Apple M1 Max: 
                 speed         user         nice          sys         idle          irq
       #1-10  2400 MHz    9106554 s          0 s   19172429 s   59301135 s          0 s
  Memory: 32.0 GB (147.5625 MB free)
  Uptime: 1.471116e6 sec
  Load Avg:  4.89208984375  5.0458984375  5.62158203125
  WORD_SIZE: 64
  LLVM: libLLVM-16.0.6 (ORCJIT, apple-m1)
Threads: 1 default, 0 interactive, 1 GC (on 8 virtual cores)
```