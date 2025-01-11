# Benchmark Report for *DuckDB*

## Job Properties
* Time of benchmark: 11 Jan 2025 - 15:3
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

| ID                                                                   | time            | GC time  | memory          | allocations |
|----------------------------------------------------------------------|----------------:|---------:|----------------:|------------:|
| `["ColumnConversion", "read_Bool_ColumnConversion"]`                 |   1.708 μs (5%) |          |   2.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Bool_VecReader"]`                        | 242.792 μs (5%) |          |  50.70 KiB (1%) |        3096 |
| `["ColumnConversion", "read_DateTime_ColumnConversion"]`             |   2.416 μs (5%) |          |  16.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_DateTime_VecReader"]`                    | 292.209 μs (5%) |          |  96.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Date_ColumnConversion"]`                 |   1.958 μs (5%) |          |  16.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Date_VecReader"]`                        | 299.334 μs (5%) |          |  96.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Dates.CompoundPeriod_ColumnConversion"]` | 476.791 μs (5%) |          | 272.30 KiB (1%) |       10252 |
| `["ColumnConversion", "read_Dates.CompoundPeriod_VecReader"]`        | 796.666 μs (5%) |          | 384.70 KiB (1%) |       15384 |
| `["ColumnConversion", "read_Float32_ColumnConversion"]`              |   1.916 μs (5%) |          |   8.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Float32_VecReader"]`                     | 249.375 μs (5%) |          |  88.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Float64_ColumnConversion"]`              |   1.708 μs (5%) |          |  16.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Float64_VecReader"]`                     | 249.125 μs (5%) |          |  96.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Int128_ColumnConversion"]`               |   1.875 μs (5%) |          |  32.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int128_VecReader"]`                      | 297.542 μs (5%) |          | 144.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Int16_ColumnConversion"]`                |   1.666 μs (5%) |          |   4.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int16_VecReader"]`                       | 246.500 μs (5%) |          |  84.30 KiB (1%) |        5118 |
| `["ColumnConversion", "read_Int32_ColumnConversion"]`                |   1.708 μs (5%) |          |   8.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int32_VecReader"]`                       | 242.125 μs (5%) |          |  88.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Int64_ColumnConversion"]`                |   1.833 μs (5%) |          |  16.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int64_VecReader"]`                       | 226.750 μs (5%) |          |  96.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Int8_ColumnConversion"]`                 |   1.625 μs (5%) |          |   2.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int8_VecReader"]`                        | 244.417 μs (5%) |          |  50.70 KiB (1%) |        3096 |
| `["ColumnConversion", "read_NTuple{10, Int64}_VecReader"]`           |   4.626 ms (5%) |          |   1.82 MiB (1%) |       78332 |
| `["ColumnConversion", "read_String_ColumnConversion"]`               |  28.292 μs (5%) |          |  94.41 KiB (1%) |        2058 |
| `["ColumnConversion", "read_String_VecReader"]`                      | 334.292 μs (5%) |          | 206.88 KiB (1%) |        7192 |
| `["ColumnConversion", "read_Time_ColumnConversion"]`                 |  16.583 μs (5%) |          |  16.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Time_VecReader"]`                        | 333.708 μs (5%) |          |  96.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_UInt128_ColumnConversion"]`              |   1.875 μs (5%) |          |  32.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_UInt128_VecReader"]`                     | 302.125 μs (5%) |          | 144.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_UInt16_ColumnConversion"]`               |   1.708 μs (5%) |          |   4.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_UInt16_VecReader"]`                      | 253.667 μs (5%) |          |  84.22 KiB (1%) |        5113 |
| `["ColumnConversion", "read_UInt32_ColumnConversion"]`               |   1.666 μs (5%) |          |   8.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_UInt32_VecReader"]`                      | 229.167 μs (5%) |          |  88.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_UInt64_ColumnConversion"]`               |   1.791 μs (5%) |          |  16.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_UInt64_VecReader"]`                      | 250.833 μs (5%) |          |  96.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_UInt8_ColumnConversion"]`                |   1.666 μs (5%) |          |   2.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_UInt8_VecReader"]`                       | 241.041 μs (5%) |          |  50.70 KiB (1%) |        3096 |
| `["ColumnConversion", "read_Vector{Int64}_ColumnConversion"]`        |   3.956 ms (5%) |          |  25.63 MiB (1%) |        7714 |
| `["ColumnConversion", "read_Vector{Int64}_VecReader"]`               | 151.026 ms (5%) |          | 769.36 KiB (1%) |       35884 |
| `["ColumnConversion", "write_Bool_VecWriter"]`                       |   1.541 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_DateTime_VecWriter"]`                   |   1.708 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Date_VecWriter"]`                       |   1.958 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Dates.CompoundPeriod_VecWriter"]`       |   1.655 ms (5%) |          | 921.11 KiB (1%) |       32476 |
| `["ColumnConversion", "write_Float32_VecWriter"]`                    |   1.500 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Float64_VecWriter"]`                    |   1.500 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Int128_VecWriter"]`                     |   2.000 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Int16_VecWriter"]`                      |   1.541 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Int32_VecWriter"]`                      |   1.500 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Int64_VecWriter"]`                      |   1.500 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Int8_VecWriter"]`                       |   1.500 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_NTuple{10, Int64}_VecWriter"]`          | 127.625 μs (5%) |          | 216.84 KiB (1%) |        3611 |
| `["ColumnConversion", "write_String_VecWriter"]`                     |  59.125 μs (5%) |          |  176 bytes (1%) |           6 |
| `["ColumnConversion", "write_Time_VecWriter"]`                       |   1.833 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_UInt128_VecWriter"]`                    |   2.000 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_UInt16_VecWriter"]`                     |   1.500 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_UInt32_VecWriter"]`                     |   1.541 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_UInt64_VecWriter"]`                     |   1.500 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_UInt8_VecWriter"]`                      |   1.541 μs (5%) |          |  240 bytes (1%) |           8 |
| `["ColumnConversion", "write_Vector{Int64}_VecWriter"]`              |   1.681 ms (5%) |          | 952.61 KiB (1%) |       42517 |
| `["UDF", "float_add"]`                                               |  40.490 ms (5%) | 1.593 ms |  46.05 MiB (1%) |     3008814 |

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
       #1-10  2400 MHz    8016224 s          0 s   16393178 s   51486296 s          0 s
  Memory: 32.0 GB (132.96875 MB free)
  Uptime: 1.304006e6 sec
  Load Avg:  3.2763671875  3.0791015625  2.8642578125
  WORD_SIZE: 64
  LLVM: libLLVM-16.0.6 (ORCJIT, apple-m1)
Threads: 1 default, 0 interactive, 1 GC (on 8 virtual cores)
```