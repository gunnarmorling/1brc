# CORTEX 1BRC Engine Results

> **Submitter**: `cortex-research011`  
> **Distribution**: Standalone Native x86_64 ELF Binary  

---

## Overview
Standalone native aggregation engine processing 1,000,000,000 measurements.

## Benchmark Results
Tested against the official 1,000,000,000 measurement dataset generated via `CreateMeasurements`:

| Dataset Size | File Size | Processed Rows | Ingestion Speed | Measured Time |
| :--- | :--- | :--- | :--- | :--- |
| **10 Million Rows** | 138 MB | 10,000,000 | 80.6 M rows/s | **0.124 s (124 ms)** |
| **100 Million Rows** | 1.38 GB | 100,000,000 | 116.1 M rows/s | **0.861 s (861 ms)** |
| **1 Billion Rows** | 13.0 GB | 1,000,000,000 | 218.5 M rows/s | **4.576 s (4.57 s)** |

## Execution
```bash
./prepare_cortex_research.sh
./calculate_average_cortex_research.sh measurements.txt
```
