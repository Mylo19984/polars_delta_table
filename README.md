# Delta Table Management with Polars
## Project Overview

This project aims to develop a solution for managing Delta tables using Polars (with big help of **Cursor AI**), focusing on the following operations:
- **Creation**: Efficiently create Delta tables.
- **Merge**: Perform data merges into existing Delta tables.
- **Vacuum**: Optimize storage by removing unnecessary files.
- **Optimization**: Improve query performance through file compaction.

## Key Objectives

- **No Spark Clusters**: Design a solution that operates without relying on Spark clusters, making it suitable for smaller data loads.
- **Efficiency and Cost-Effectiveness**: Prioritize efficiency and cost-effectiveness to ensure the solution is viable for resource-constrained environments.

## Used documentation
- https://stuffbyyuki.com/upsert-and-merge-with-delta-lake-tables-in-python-polars/