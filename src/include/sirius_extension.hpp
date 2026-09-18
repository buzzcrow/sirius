/*
 * Copyright 2025, Sirius Contributors.
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

#include "duckdb.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "io/file_version.hpp"
#include "scan/parquet_footer_summary.hpp"

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace sirius {
struct sirius_config;
}  // namespace sirius

namespace cudf::io::parquet {
struct FileMetaData;
}

namespace duckdb {
class GPUBufferManager;
struct DBConfig;

/// Immutable evidence captured at bind for one Sirius-owned Parquet input.
struct SiriusParquetFileBindData {
  SiriusParquetFileBindData() = default;
  SiriusParquetFileBindData(std::string uri,
                            std::shared_ptr<cudf::io::parquet::FileMetaData const> file_metadata,
                            std::size_t object_size,
                            std::string validation_etag,
                            sirius::io::local_file_version local_version,
                            std::shared_ptr<sirius::scan::parquet_footer_summary const> footer_summary = nullptr)
    : uri(std::move(uri)),
      file_metadata(std::move(file_metadata)),
      object_size(object_size),
      validation_etag(std::move(validation_etag)),
      local_version(std::move(local_version)),
      footer_summary(std::move(footer_summary))
  {
  }

  std::string uri;
  std::shared_ptr<cudf::io::parquet::FileMetaData const> file_metadata;
  std::size_t object_size{0};
  std::string validation_etag;
  sirius::io::local_file_version local_version;
  std::shared_ptr<sirius::scan::parquet_footer_summary const> footer_summary;

  bool operator==(SiriusParquetFileBindData const& other) const
  {
    return uri == other.uri && file_metadata == other.file_metadata &&
           object_size == other.object_size && validation_etag == other.validation_etag &&
           local_version == other.local_version && footer_summary == other.footer_summary;
  }
};

/// Per immutable bind counters. They deliberately live with the bound scan,
/// rather than in connection-global state, so plan copies and deserialized
/// references report one coherent view without extending object lifetime.
struct SiriusParquetMeteringData {
  uint64_t binds_created{0};
  uint64_t bound_files{0};
  uint64_t footer_summaries{0};
  uint64_t footer_row_groups{0};
  uint64_t footer_compressed_bytes{0};
  uint64_t footer_uncompressed_bytes{0};
  uint64_t null_count_covered_columns{0};
  uint64_t minmax_covered_columns{0};
  uint64_t cardinality_requests{0};
  uint64_t statistics_requests{0};
  uint64_t statistics_with_minmax{0};
  uint64_t statistics_nullability_only{0};
  uint64_t statistics_unavailable{0};
  uint64_t planned_row_groups{0};
  uint64_t planned_compressed_read_bytes{0};
  uint64_t planned_decode_working_bytes{0};
  uint64_t materialization_attempts{0};
  uint64_t materialization_successes{0};
  uint64_t materialization_failures{0};
  uint64_t materialized_row_groups{0};
  uint64_t materialized_compressed_read_budget_bytes{0};
  uint64_t materialized_decode_working_budget_bytes{0};
  uint64_t materialized_rows{0};
  uint64_t pipeline_task_attempts{0};
  uint64_t pipeline_task_successes{0};
  uint64_t pipeline_task_failures{0};
  uint64_t pipeline_task_input_basis_bytes{0};
  uint64_t pipeline_task_reservation_bytes{0};
  uint64_t pipeline_task_peak_operator_bytes{0};
  uint64_t pipeline_task_output_bytes{0};
};

class SiriusParquetMetering {
 public:
  void record_bind(std::vector<SiriusParquetFileBindData> const& files)
  {
    binds_created.fetch_add(1, std::memory_order_relaxed);
    bound_files.fetch_add(files.size(), std::memory_order_relaxed);
    for (auto const& file : files) {
      if (!file.footer_summary) { continue; }
      footer_summaries.fetch_add(1, std::memory_order_relaxed);
      for (auto const& row_group : file.footer_summary->row_groups) {
        footer_row_groups.fetch_add(1, std::memory_order_relaxed);
        footer_compressed_bytes.fetch_add(row_group.compressed_bytes, std::memory_order_relaxed);
        footer_uncompressed_bytes.fetch_add(row_group.uncompressed_bytes, std::memory_order_relaxed);
      }
      for (auto const& summary : file.footer_summary->column_null_counts) {
        if (summary.complete) { null_count_covered_columns.fetch_add(1, std::memory_order_relaxed); }
      }
      for (auto const& summary : file.footer_summary->column_minmax) {
        if (summary.complete) { minmax_covered_columns.fetch_add(1, std::memory_order_relaxed); }
      }
    }
  }

  [[nodiscard]] SiriusParquetMeteringData snapshot() const noexcept
  {
    SiriusParquetMeteringData result;
    result.binds_created              = binds_created.load(std::memory_order_relaxed);
    result.bound_files                = bound_files.load(std::memory_order_relaxed);
    result.footer_summaries           = footer_summaries.load(std::memory_order_relaxed);
    result.footer_row_groups          = footer_row_groups.load(std::memory_order_relaxed);
    result.footer_compressed_bytes    = footer_compressed_bytes.load(std::memory_order_relaxed);
    result.footer_uncompressed_bytes  = footer_uncompressed_bytes.load(std::memory_order_relaxed);
    result.null_count_covered_columns = null_count_covered_columns.load(std::memory_order_relaxed);
    result.minmax_covered_columns     = minmax_covered_columns.load(std::memory_order_relaxed);
    result.cardinality_requests       = cardinality_requests.load(std::memory_order_relaxed);
    result.statistics_requests        = statistics_requests.load(std::memory_order_relaxed);
    result.statistics_with_minmax     = statistics_with_minmax.load(std::memory_order_relaxed);
    result.statistics_nullability_only =
      statistics_nullability_only.load(std::memory_order_relaxed);
    result.statistics_unavailable = statistics_unavailable.load(std::memory_order_relaxed);
    result.planned_row_groups = planned_row_groups.load(std::memory_order_relaxed);
    result.planned_compressed_read_bytes =
      planned_compressed_read_bytes.load(std::memory_order_relaxed);
    result.planned_decode_working_bytes =
      planned_decode_working_bytes.load(std::memory_order_relaxed);
    result.materialization_attempts = materialization_attempts.load(std::memory_order_relaxed);
    result.materialization_successes = materialization_successes.load(std::memory_order_relaxed);
    result.materialization_failures = materialization_failures.load(std::memory_order_relaxed);
    result.materialized_row_groups = materialized_row_groups.load(std::memory_order_relaxed);
    result.materialized_compressed_read_budget_bytes =
      materialized_compressed_read_budget_bytes.load(std::memory_order_relaxed);
    result.materialized_decode_working_budget_bytes =
      materialized_decode_working_budget_bytes.load(std::memory_order_relaxed);
    result.materialized_rows = materialized_rows.load(std::memory_order_relaxed);
    result.pipeline_task_attempts = pipeline_task_attempts.load(std::memory_order_relaxed);
    result.pipeline_task_successes = pipeline_task_successes.load(std::memory_order_relaxed);
    result.pipeline_task_failures = pipeline_task_failures.load(std::memory_order_relaxed);
    result.pipeline_task_input_basis_bytes =
      pipeline_task_input_basis_bytes.load(std::memory_order_relaxed);
    result.pipeline_task_reservation_bytes =
      pipeline_task_reservation_bytes.load(std::memory_order_relaxed);
    result.pipeline_task_peak_operator_bytes =
      pipeline_task_peak_operator_bytes.load(std::memory_order_relaxed);
    result.pipeline_task_output_bytes = pipeline_task_output_bytes.load(std::memory_order_relaxed);
    return result;
  }

  /// Runtime scan observation. The byte values are the split's conservative
  /// reader budgets, not reactor-reported physical I/O bytes: cache and
  /// aligned backend reads make the latter backend-specific.
  void record_materialization_attempt(uint64_t row_groups,
                                      uint64_t compressed_read_budget_bytes,
                                      uint64_t decode_working_budget_bytes) const noexcept
  {
    materialization_attempts.fetch_add(1, std::memory_order_relaxed);
    materialized_row_groups.fetch_add(row_groups, std::memory_order_relaxed);
    materialized_compressed_read_budget_bytes.fetch_add(
      compressed_read_budget_bytes, std::memory_order_relaxed);
    materialized_decode_working_budget_bytes.fetch_add(
      decode_working_budget_bytes, std::memory_order_relaxed);
  }

  void record_materialization_success(uint64_t rows) const noexcept
  {
    materialization_successes.fetch_add(1, std::memory_order_relaxed);
    materialized_rows.fetch_add(rows, std::memory_order_relaxed);
  }

  void record_materialization_failure() const noexcept
  {
    materialization_failures.fetch_add(1, std::memory_order_relaxed);
  }

  /// Task-level observation is attributed only when the task input carries
  /// this immutable BoundScan. @p peak_operator_bytes uses the same
  /// materialization-excluded definition as pipeline_memory_history; it is
  /// not a process-wide GPU usage sample and never influences reservation.
  void record_pipeline_task_attempt(uint64_t input_basis_bytes, uint64_t reservation_bytes) const noexcept
  {
    pipeline_task_attempts.fetch_add(1, std::memory_order_relaxed);
    pipeline_task_input_basis_bytes.fetch_add(input_basis_bytes, std::memory_order_relaxed);
    pipeline_task_reservation_bytes.fetch_add(reservation_bytes, std::memory_order_relaxed);
  }

  void record_pipeline_task_success(uint64_t peak_operator_bytes, uint64_t output_bytes) const noexcept
  {
    pipeline_task_successes.fetch_add(1, std::memory_order_relaxed);
    pipeline_task_peak_operator_bytes.fetch_add(peak_operator_bytes, std::memory_order_relaxed);
    pipeline_task_output_bytes.fetch_add(output_bytes, std::memory_order_relaxed);
  }

  void record_pipeline_task_failure() const noexcept
  {
    pipeline_task_failures.fetch_add(1, std::memory_order_relaxed);
  }

  mutable std::atomic<uint64_t> binds_created{0};
  mutable std::atomic<uint64_t> bound_files{0};
  mutable std::atomic<uint64_t> footer_summaries{0};
  mutable std::atomic<uint64_t> footer_row_groups{0};
  mutable std::atomic<uint64_t> footer_compressed_bytes{0};
  mutable std::atomic<uint64_t> footer_uncompressed_bytes{0};
  mutable std::atomic<uint64_t> null_count_covered_columns{0};
  mutable std::atomic<uint64_t> minmax_covered_columns{0};
  mutable std::atomic<uint64_t> cardinality_requests{0};
  mutable std::atomic<uint64_t> statistics_requests{0};
  mutable std::atomic<uint64_t> statistics_with_minmax{0};
  mutable std::atomic<uint64_t> statistics_nullability_only{0};
  mutable std::atomic<uint64_t> statistics_unavailable{0};
  /// Scan-plan facts after footer/statistics pruning. These are observational:
  /// they never feed admission, split selection, or the optimizer.
  mutable std::atomic<uint64_t> planned_row_groups{0};
  mutable std::atomic<uint64_t> planned_compressed_read_bytes{0};
  mutable std::atomic<uint64_t> planned_decode_working_bytes{0};
  mutable std::atomic<uint64_t> materialization_attempts{0};
  mutable std::atomic<uint64_t> materialization_successes{0};
  mutable std::atomic<uint64_t> materialization_failures{0};
  mutable std::atomic<uint64_t> materialized_row_groups{0};
  mutable std::atomic<uint64_t> materialized_compressed_read_budget_bytes{0};
  mutable std::atomic<uint64_t> materialized_decode_working_budget_bytes{0};
  mutable std::atomic<uint64_t> materialized_rows{0};
  mutable std::atomic<uint64_t> pipeline_task_attempts{0};
  mutable std::atomic<uint64_t> pipeline_task_successes{0};
  mutable std::atomic<uint64_t> pipeline_task_failures{0};
  mutable std::atomic<uint64_t> pipeline_task_input_basis_bytes{0};
  mutable std::atomic<uint64_t> pipeline_task_reservation_bytes{0};
  mutable std::atomic<uint64_t> pipeline_task_peak_operator_bytes{0};
  mutable std::atomic<uint64_t> pipeline_task_output_bytes{0};
};

/// Immutable, connection-generation-local result of a Sirius-owned Parquet
/// bind.  A logical-plan copy must retain this object rather than copy its
/// parsed footer payload or bind the URI again.
struct SiriusParquetBoundScan {
  SiriusParquetBoundScan() = default;
  SiriusParquetBoundScan(std::vector<SiriusParquetFileBindData> files, std::size_t total_num_rows)
    : files(std::move(files)), total_num_rows(total_num_rows)
  {
    metering.record_bind(this->files);
  }

  uint64_t generation{0};
  uint64_t scan_instance_id{0};
  uint64_t fingerprint{0};
  std::vector<SiriusParquetFileBindData> files;
  std::size_t total_num_rows{0};
  mutable SiriusParquetMetering metering;
};

// Lightweight table-function payload for a Sirius-owned Parquet scan. The
// shared BoundScan carries the canonical ordered file list and parsed footers;
// Copy() and table-function deserialization retain this same object.
struct SiriusReadParquetBindData : public FunctionData {
  SiriusReadParquetBindData(
    std::string uri,
    std::size_t total_num_rows,
    std::shared_ptr<cudf::io::parquet::FileMetaData const> file_metadata = nullptr,
    std::size_t object_size                                              = 0,
    std::string validation_etag                                          = {},
    sirius::io::local_file_version local_version                         = {},
    std::shared_ptr<sirius::scan::parquet_footer_summary const> footer_summary = nullptr)
    : SiriusReadParquetBindData(std::vector<SiriusParquetFileBindData>{{std::move(uri),
                                                                        std::move(file_metadata),
                                                                        object_size,
                                                                        std::move(validation_etag),
                                                                        std::move(local_version),
                                                                        std::move(footer_summary)}},
                                total_num_rows)
  {
  }

  SiriusReadParquetBindData(std::vector<SiriusParquetFileBindData> files,
                            std::size_t total_num_rows)
    : bound_scan(std::make_shared<SiriusParquetBoundScan>(std::move(files), total_num_rows))
  {
  }

  explicit SiriusReadParquetBindData(std::shared_ptr<SiriusParquetBoundScan const> bound_scan)
    : bound_scan(std::move(bound_scan))
  {
  }

  [[nodiscard]] SiriusParquetBoundScan const& scan() const { return *bound_scan; }
  [[nodiscard]] std::vector<SiriusParquetFileBindData> const& files() const { return scan().files; }
  [[nodiscard]] std::size_t total_num_rows() const { return scan().total_num_rows; }
  [[nodiscard]] uint64_t generation() const { return scan().generation; }
  [[nodiscard]] uint64_t scan_instance_id() const { return scan().scan_instance_id; }
  [[nodiscard]] uint64_t fingerprint() const { return scan().fingerprint; }
  [[nodiscard]] SiriusParquetMeteringData metering() const { return scan().metering.snapshot(); }

  // One-file accessors preserve the old compatibility surface without making a
  // second copy of the footer evidence. Callers for multi-file scans use files().
  [[nodiscard]] std::string const& uri() const { return files().front().uri; }
  [[nodiscard]] std::shared_ptr<cudf::io::parquet::FileMetaData const> const& file_metadata() const
  {
    return files().front().file_metadata;
  }
  [[nodiscard]] std::size_t object_size() const { return files().front().object_size; }
  [[nodiscard]] std::string const& validation_etag() const { return files().front().validation_etag; }
  [[nodiscard]] sirius::io::local_file_version const& local_version() const
  {
    return files().front().local_version;
  }

  std::shared_ptr<SiriusParquetBoundScan const> bound_scan;

  unique_ptr<FunctionData> Copy() const override
  {
    return make_uniq<SiriusReadParquetBindData>(bound_scan);
  }

  bool Equals(FunctionData const& other_p) const override
  {
    auto const& other = other_p.Cast<SiriusReadParquetBindData>();
    return bound_scan == other.bound_scan ||
           (scan().generation == other.scan().generation &&
            scan().scan_instance_id == other.scan().scan_instance_id &&
            scan().fingerprint == other.scan().fingerprint && files() == other.files() &&
            total_num_rows() == other.total_num_rows());
  }
};

// Cardinality callback for sirius_read_parquet. Returns the footer row count
// (exact value, doubles as both estimated and max cardinality). Returns
// nullptr on a null or wrong-type FunctionData so DuckDB falls back to its
// default behavior.
unique_ptr<NodeStatistics> SiriusReadParquetCardinality(ClientContext& context,
                                                        FunctionData const* bind_data);

/// Return only footer statistics whose coverage is complete across every
/// bound file. Currently this is exact null-free evidence for flat scalar
/// columns; min/max requires lossless conversion for every Parquet encoding.
unique_ptr<BaseStatistics> SiriusReadParquetStatistics(ClientContext& context,
                                                        FunctionData const* bind_data,
                                                        column_t column_index);

class SiriusExtension : public Extension {
 public:
  void Load(ExtensionLoader& loader) override;
  std::string Name() override;
  std::string Version() const override;
  /// Register Sirius's extension options. @p defaults supplies the registered default for every
  /// option DuckDB stores per connection, so a sirius.yaml value reaches those connections as
  /// their inherited starting point instead of being shadowed by the compiled default.
  static void InitialGPUConfigs(DBConfig& db, const sirius::sirius_config& defaults);
  static void RegisterGPUFunctions(DatabaseInstance& catalog);
#ifdef SIRIUS_ENABLE_LEGACY
  static void GPUProcessingSubstraitFunction(ClientContext& context,
                                             TableFunctionInput& data_p,
                                             DataChunk& output);
  static void GPUProcessingFunction(ClientContext& context,
                                    TableFunctionInput& data_p,
                                    DataChunk& output);
  static unique_ptr<FunctionData> GPUProcessingSubstraitBind(ClientContext& context,
                                                             TableFunctionBindInput& input,
                                                             vector<LogicalType>& return_types,
                                                             vector<string>& names);
  static unique_ptr<FunctionData> GPUProcessingBind(ClientContext& context,
                                                    TableFunctionBindInput& input,
                                                    vector<LogicalType>& return_types,
                                                    vector<string>& names);
#endif
  static void GPUExecutionFunction(ClientContext& context,
                                   TableFunctionInput& data_p,
                                   DataChunk& output);
#ifdef SIRIUS_ENABLE_LEGACY
  static void GPUBufferInitFunction(ClientContext& context,
                                    TableFunctionInput& data_p,
                                    DataChunk& output);
  static unique_ptr<FunctionData> GPUBufferInitBind(ClientContext& context,
                                                    TableFunctionBindInput& input,
                                                    vector<LogicalType>& return_types,
                                                    vector<string>& names);
#endif
  static unique_ptr<FunctionData> GPUExecutionBind(ClientContext& context,
                                                   TableFunctionBindInput& input,
                                                   vector<LogicalType>& return_types,
                                                   vector<string>& names);
  /// Per-execution state factory for gpu_execution(): a reusable prepared
  /// statement gets fresh execution state (result/connection/interface) on
  /// every execute instead of reusing bind-held state.
  static unique_ptr<GlobalTableFunctionState> GPUExecutionInitGlobal(ClientContext& context,
                                                                     TableFunctionInitInput& input);

  static void PinTableFunction(ClientContext& context,
                               TableFunctionInput& data_p,
                               DataChunk& output);
  static unique_ptr<FunctionData> PinTableBind(ClientContext& context,
                                               TableFunctionBindInput& input,
                                               vector<LogicalType>& return_types,
                                               vector<string>& names);

  static void UnpinTableFunction(ClientContext& context,
                                 TableFunctionInput& data_p,
                                 DataChunk& output);
  static unique_ptr<FunctionData> UnpinTableBind(ClientContext& context,
                                                 TableFunctionBindInput& input,
                                                 vector<LogicalType>& return_types,
                                                 vector<string>& names);

#ifdef SIRIUS_ENABLE_LEGACY
  static bool buffer_is_initialized;
#endif
};

}  // namespace duckdb
