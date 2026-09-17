/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "sirius_extension.hpp"

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace sirius::scan {

/// Per-connection, generation-scoped ownership for Sirius-owned file binds.
/// It exists solely to let LogicalGet serialization recover the exact bind
/// object during an in-process plan copy; it is not a persistent plan format.
class file_scan_bind_catalog final : public duckdb::ClientContextState {
 public:
  static constexpr char const* kStateKey = "sirius_file_scan_bind_catalog";

  std::shared_ptr<duckdb::SiriusParquetBoundScan const> register_parquet_scan(
    uint64_t generation,
    std::vector<duckdb::SiriusParquetFileBindData> files,
    std::size_t total_num_rows);

  /// Resolves exactly one previously registered binding. This never probes a
  /// datasource, expands a glob, or substitutes a newer path-version entry.
  [[nodiscard]] std::shared_ptr<duckdb::SiriusParquetBoundScan const> resolve_parquet_scan(
    uint64_t generation,
    uint64_t scan_instance_id,
    uint64_t fingerprint) const;

 private:
  mutable std::mutex mutex_;
  uint64_t active_generation_{0};
  uint64_t next_scan_instance_id_{0};
  uint64_t next_fingerprint_{0};
  std::unordered_map<uint64_t, std::shared_ptr<duckdb::SiriusParquetBoundScan const>> entries_;
};

[[nodiscard]] duckdb::shared_ptr<file_scan_bind_catalog> file_scan_catalog_for(
  duckdb::ClientContext& context);

}  // namespace sirius::scan
