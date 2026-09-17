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

/// Immutable, connection-generation-local result of a Sirius-owned Parquet
/// bind.  A logical-plan copy must retain this object rather than copy its
/// parsed footer payload or bind the URI again.
struct SiriusParquetBoundScan {
  SiriusParquetBoundScan() = default;
  SiriusParquetBoundScan(std::vector<SiriusParquetFileBindData> files, std::size_t total_num_rows)
    : files(std::move(files)), total_num_rows(total_num_rows)
  {
  }

  uint64_t generation{0};
  uint64_t scan_instance_id{0};
  uint64_t fingerprint{0};
  std::vector<SiriusParquetFileBindData> files;
  std::size_t total_num_rows{0};
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
