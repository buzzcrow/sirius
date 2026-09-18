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
#include <cstddef>
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
enum class parquet_minmax_encoding : std::uint8_t {
  /// Parquet's native fixed-width scalar representation (little-endian).
  little_endian_scalar,
  /// Signed two's-complement decimal BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY.
  /// This value is set only after bind verified the leaf's DECIMAL annotation.
  big_endian_decimal,
};

/// Unit carried by a temporal Parquet statistic before conversion to DuckDB's
/// logical carrier. `none` means the footer supplied no trustworthy unit.
enum class parquet_stat_time_unit : std::uint8_t { none, millis, micros, nanos };

struct parquet_column_minmax_summary {
  bool complete{false};
  std::vector<std::vector<std::uint8_t>> min_values;
  std::vector<std::vector<std::uint8_t>> max_values;
  // Keep these after the historical aggregate fields so existing summary
  // fixtures retain their default scalar/no-unit interpretation.
  parquet_minmax_encoding encoding{parquet_minmax_encoding::little_endian_scalar};
  parquet_stat_time_unit time_unit{parquet_stat_time_unit::none};
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

/// A conservative bind-time I/O and decode estimate for a projected row
/// group.  The footer records totals for all leaves, so a projection is
/// charged its rounded-up share.  Execution may refine this with the parsed
/// per-column footer, but must never drop below this bound.
struct parquet_projected_row_group_estimate {
  std::uint64_t compressed_read_bytes{0};
  std::uint64_t decoded_working_bytes{0};

  parquet_projected_row_group_estimate() = default;
  parquet_projected_row_group_estimate(std::uint64_t compressed, std::uint64_t decoded)
    : compressed_read_bytes(compressed), decoded_working_bytes(decoded)
  {
  }
};

[[nodiscard]] inline parquet_projected_row_group_estimate estimate_projected_row_group(
  parquet_footer_summary const& summary,
  std::size_t row_group_index,
  std::size_t projected_leaf_count,
  std::size_t total_leaf_count) noexcept
{
  if (row_group_index >= summary.row_groups.size() || projected_leaf_count == 0 ||
      total_leaf_count == 0) {
    return {};
  }
  auto const& row_group = summary.row_groups[row_group_index];
  if (projected_leaf_count >= total_leaf_count) {
    return parquet_projected_row_group_estimate{row_group.compressed_bytes,
                                                row_group.uncompressed_bytes};
  }
  auto const scale_up = [projected_leaf_count, total_leaf_count](std::uint64_t bytes) {
    // __int128 keeps a malformed-but-large footer from wrapping this estimate.
    auto const numerator = static_cast<unsigned __int128>(bytes) * projected_leaf_count;
    return static_cast<std::uint64_t>((numerator + total_leaf_count - 1) / total_leaf_count);
  };
  return parquet_projected_row_group_estimate{scale_up(row_group.compressed_bytes),
                                              scale_up(row_group.uncompressed_bytes)};
}

}  // namespace sirius::scan
