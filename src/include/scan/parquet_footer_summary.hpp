/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <duckdb/common/types.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace sirius::scan {

/// A row group's immutable footprint in the bind-time parquet footer.
struct parquet_row_group_summary {
  std::uint64_t first_row{0};
  std::uint64_t num_rows{0};
  std::uint64_t compressed_bytes{0};
  std::uint64_t uncompressed_bytes{0};
};

/// Null-count evidence for one top-level output column.
///
/// `complete` is true only for a scalar column whose one parquet leaf supplied
/// a null_count in every row group. Nested columns deliberately remain
/// incomplete: a leaf null count is not a top-level null count for LIST/MAP/
/// STRUCT values.
struct parquet_column_null_summary {
  bool complete{false};
  std::uint64_t null_count{0};
};

/// Exact Parquet PLAIN-encoded bounds for one scalar column, one entry per
/// row group. The bytes remain in footer representation until the optimizer
/// callback proves that the output logical type can decode them losslessly.
struct parquet_column_minmax_summary {
  bool complete{false};
  std::vector<std::vector<std::uint8_t>> min_values;
  std::vector<std::vector<std::uint8_t>> max_values;
};

/// Footer facts retained independently of the parsed cuDF footer object.
///
/// This is the only footer-derived payload optimizer callbacks may inspect.
/// The parsed footer remains owned by the bound scan solely for physical
/// execution and is never re-fetched by a Sirius-owned scan.
struct parquet_footer_summary {
  std::vector<std::string> names;
  std::vector<duckdb::LogicalType> types;
  std::vector<parquet_row_group_summary> row_groups;
  std::vector<parquet_column_null_summary> column_null_counts;
  std::vector<parquet_column_minmax_summary> column_minmax;
  std::uint64_t total_num_rows{0};
};

}  // namespace sirius::scan
