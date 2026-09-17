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

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

namespace sirius {
struct sirius_config;
}  // namespace sirius

namespace cudf::io::parquet {
struct FileMetaData;
}

namespace duckdb {
class GPUBufferManager;
struct DBConfig;

// Bind-time payload for the sirius_read_parquet table function. Carries the
// canonical URI and parquet footer row count. The physical planner consumes
// this URI and footer directly, while DuckDB's optimizer sees a real
// cardinality estimate via the registered callback instead of falling back to
// "unknown table function output".
struct SiriusReadParquetBindData : public FunctionData {
  SiriusReadParquetBindData(std::string uri,
                            std::size_t total_num_rows,
                            std::shared_ptr<cudf::io::parquet::FileMetaData const> file_metadata = nullptr,
                            std::size_t object_size = 0,
                            std::string validation_etag = {},
                            sirius::io::local_file_version local_version = {})
    : uri(std::move(uri)),
      total_num_rows(total_num_rows),
      file_metadata(std::move(file_metadata)),
      object_size(object_size),
      validation_etag(std::move(validation_etag)),
      local_version(std::move(local_version))
  {
  }

  std::string uri;
  std::size_t total_num_rows{0};
  std::shared_ptr<cudf::io::parquet::FileMetaData const> file_metadata;
  /// Size observed while reading the bound footer. Reused to open object-store
  /// data reads without a second size-discovery HEAD request.
  std::size_t object_size{0};
  /// Footer Range GET ETag for S3, empty when unavailable.
  std::string validation_etag;
  /// Bind-time local size/mtime evidence; unavailable for remote paths.
  sirius::io::local_file_version local_version;

  unique_ptr<FunctionData> Copy() const override
  {
    return make_uniq<SiriusReadParquetBindData>(
      uri, total_num_rows, file_metadata, object_size, validation_etag, local_version);
  }

  bool Equals(FunctionData const& other_p) const override
  {
    auto const& other = other_p.Cast<SiriusReadParquetBindData>();
    return uri == other.uri && total_num_rows == other.total_num_rows &&
           file_metadata == other.file_metadata && object_size == other.object_size &&
           validation_etag == other.validation_etag && local_version == other.local_version;
  }
};

// Cardinality callback for sirius_read_parquet. Returns the footer row count
// (exact value, doubles as both estimated and max cardinality). Returns
// nullptr on a null or wrong-type FunctionData so DuckDB falls back to its
// default behavior.
unique_ptr<NodeStatistics> SiriusReadParquetCardinality(ClientContext& context,
                                                        FunctionData const* bind_data);

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
