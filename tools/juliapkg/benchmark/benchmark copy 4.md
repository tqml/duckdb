# Benchmark Report for *DuckDB*

## Job Properties
* Time of benchmark: 11 Jan 2025 - 16:33
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

| ID                                                                   | time            | GC time | memory          | allocations |
|----------------------------------------------------------------------|----------------:|--------:|----------------:|------------:|
| `["ColumnConversion", "read_Bool_ColumnConversion"]`                 |  14.208 μs (5%) |         |  21.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Bool_VecReader"]`                        |  52.125 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_DateTime_ColumnConversion"]`             |  21.917 μs (5%) |         | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_DateTime_VecReader"]`                    |  54.250 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Date_ColumnConversion"]`                 |  16.458 μs (5%) |         | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Date_VecReader"]`                        |  52.166 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Dates.CompoundPeriod_ColumnConversion"]` |   3.770 ms (5%) |         |   2.66 MiB (1%) |      102475 |
| `["ColumnConversion", "read_Dates.CompoundPeriod_VecReader"]`        |   3.396 ms (5%) |         |   2.50 MiB (1%) |      102452 |
| `["ColumnConversion", "read_Float32_ColumnConversion"]`              |  15.333 μs (5%) |         |  81.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Float32_VecReader"]`                     |  51.083 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Float64_ColumnConversion"]`              |  13.833 μs (5%) |         | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Float64_VecReader"]`                     |  51.458 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int128_ColumnConversion"]`               |  16.375 μs (5%) |         | 321.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int128_VecReader"]`                      |  53.916 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int16_ColumnConversion"]`                |  13.666 μs (5%) |         |  41.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int16_VecReader"]`                       |  51.041 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int32_ColumnConversion"]`                |  13.833 μs (5%) |         |  81.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int32_VecReader"]`                       |  51.083 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int64_ColumnConversion"]`                |  15.542 μs (5%) |         | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int64_VecReader"]`                       |  51.458 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Int8_ColumnConversion"]`                 |  14.041 μs (5%) |         |  21.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Int8_VecReader"]`                        |  52.125 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_NTuple{10, Int64}_VecReader"]`           |   8.689 ms (5%) |         |   8.29 MiB (1%) |      297272 |
| `["ColumnConversion", "read_String_ColumnConversion"]`               | 284.167 μs (5%) |         | 939.81 KiB (1%) |       20535 |
| `["ColumnConversion", "read_String_VecReader"]`                      | 386.125 μs (5%) |         | 780.50 KiB (1%) |       20532 |
| `["ColumnConversion", "read_Time_ColumnConversion"]`                 | 163.458 μs (5%) |         | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_Time_VecReader"]`                        | 198.541 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt128_ColumnConversion"]`              |  16.167 μs (5%) |         | 321.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt128_VecReader"]`                     |  53.875 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt16_ColumnConversion"]`               |  14.416 μs (5%) |         |  41.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt16_VecReader"]`                      |  51.334 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt32_ColumnConversion"]`               |  14.333 μs (5%) |         |  81.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt32_VecReader"]`                      |  51.375 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt64_ColumnConversion"]`               |  14.500 μs (5%) |         | 161.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt64_VecReader"]`                      |  51.416 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_UInt8_ColumnConversion"]`                |  13.459 μs (5%) |         |  21.84 KiB (1%) |          75 |
| `["ColumnConversion", "read_UInt8_VecReader"]`                       |  51.292 μs (5%) |         |   1.91 KiB (1%) |          52 |
| `["ColumnConversion", "read_Vector{Int64}_ColumnConversion"]`        |  14.964 ms (5%) |         |  77.11 MiB (1%) |       81649 |
| `["ColumnConversion", "read_Vector{Int64}_VecReader"]`               |   1.709 ms (5%) |         |   2.11 MiB (1%) |       56462 |
| `["ColumnConversion", "write_Bool_VecWriter"]`                       |  13.791 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_DateTime_VecWriter"]`                   |  15.500 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Date_VecWriter"]`                       |  17.958 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Dates.CompoundPeriod_VecWriter"]`       |  14.228 ms (5%) |         |   8.99 MiB (1%) |      323853 |
| `["ColumnConversion", "write_Float32_VecWriter"]`                    |  13.791 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Float64_VecWriter"]`                    |  14.000 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int128_VecWriter"]`                     |  18.541 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int16_VecWriter"]`                      |  14.000 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int32_VecWriter"]`                      |  14.000 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int64_VecWriter"]`                      |  14.000 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Int8_VecWriter"]`                       |  13.791 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_NTuple{10, Int64}_VecWriter"]`          | 664.042 μs (5%) |         |   2.12 MiB (1%) |       36083 |
| `["ColumnConversion", "write_String_VecWriter"]`                     | 353.375 μs (5%) |         |   1.30 KiB (1%) |          33 |
| `["ColumnConversion", "write_Time_VecWriter"]`                       |  17.166 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt128_VecWriter"]`                    |  18.750 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt16_VecWriter"]`                     |  13.791 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt32_VecWriter"]`                     |  14.000 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt64_VecWriter"]`                     |  14.000 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_UInt8_VecWriter"]`                      |  13.959 μs (5%) |         |   1.92 KiB (1%) |          53 |
| `["ColumnConversion", "write_Vector{Int64}_VecWriter"]`              |  13.271 ms (5%) |         |   9.30 MiB (1%) |      425143 |
| `["UDF", "float_add"]`                                               |  39.260 ms (5%) |         |  46.05 MiB (1%) |     3008814 |

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
       #1-10  2400 MHz    8068462 s          0 s   16471957 s   51893648 s          0 s
  Memory: 32.0 GB (67.359375 MB free)
  Uptime: 1.309403e6 sec
  Load Avg:  2.97314453125  3.1904296875  3.0078125
  WORD_SIZE: 64
  LLVM: libLLVM-16.0.6 (ORCJIT, apple-m1)
Threads: 1 default, 0 interactive, 1 GC (on 8 virtual cores)
```