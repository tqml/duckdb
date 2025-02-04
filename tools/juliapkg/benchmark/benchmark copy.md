# Benchmark Report for *DuckDB*

## Job Properties
* Time of benchmark: 11 Jan 2025 - 13:27
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

| ID                                                    | time            | GC time  | memory         | allocations |
|-------------------------------------------------------|----------------:|---------:|---------------:|------------:|
| `["ColumnConversion", "read_Bool_ColumnConversion"]`  |   1.666 μs (5%) |          |  2.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Bool_VecReader"]`         | 158.625 μs (5%) |          | 50.70 KiB (1%) |        3096 |
| `["ColumnConversion", "read_Int16_ColumnConversion"]` |   1.583 μs (5%) |          |  4.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int16_VecReader"]`        | 225.375 μs (5%) |          | 84.19 KiB (1%) |        5111 |
| `["ColumnConversion", "read_Int32_ColumnConversion"]` |   1.667 μs (5%) |          |  8.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int32_VecReader"]`        | 223.000 μs (5%) |          | 88.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Int64_ColumnConversion"]` |   1.708 μs (5%) |          | 16.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int64_VecReader"]`        | 221.167 μs (5%) |          | 96.70 KiB (1%) |        5144 |
| `["ColumnConversion", "read_Int8_ColumnConversion"]`  |   1.625 μs (5%) |          |  2.30 KiB (1%) |          12 |
| `["ColumnConversion", "read_Int8_VecReader"]`         | 219.583 μs (5%) |          | 50.70 KiB (1%) |        3096 |
| `["ColumnConversion", "write_Bool_VecWriter"]`        |  30.583 μs (5%) |          | 24.30 KiB (1%) |        1547 |
| `["ColumnConversion", "write_Int16_VecWriter"]`       |  38.917 μs (5%) |          | 55.78 KiB (1%) |        3562 |
| `["ColumnConversion", "write_Int32_VecWriter"]`       |  38.958 μs (5%) |          | 56.30 KiB (1%) |        3595 |
| `["ColumnConversion", "write_Int64_VecWriter"]`       |  37.000 μs (5%) |          | 56.30 KiB (1%) |        3595 |
| `["ColumnConversion", "write_Int8_VecWriter"]`        |  28.584 μs (5%) |          | 24.30 KiB (1%) |        1547 |
| `["UDF", "float_add"]`                                |  62.558 ms (5%) | 1.472 ms | 72.78 MiB (1%) |     4759913 |

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
       #1-10  2400 MHz    7970102 s          0 s   16362975 s   50988126 s          0 s
  Memory: 32.0 GB (118.46875 MB free)
  Uptime: 1.298246e6 sec
  Load Avg:  2.65673828125  3.00927734375  5.10107421875
  WORD_SIZE: 64
  LLVM: libLLVM-16.0.6 (ORCJIT, apple-m1)
Threads: 1 default, 0 interactive, 1 GC (on 8 virtual cores)
```