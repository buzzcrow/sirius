/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "scan/file_scan_bind_catalog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

#include <utility>

namespace sirius::scan {

std::shared_ptr<duckdb::SiriusParquetBoundScan const>
file_scan_bind_catalog::register_parquet_scan(
  uint64_t generation,
  std::vector<duckdb::SiriusParquetFileBindData> files,
  std::size_t total_num_rows)
{
  std::lock_guard lock(mutex_);
  if (active_generation_ != generation) {
    // The registry drops only its own references. Existing logical/physical
    // consumers keep their immutable shared binding alive until they finish.
    catalog_references_released_ += entries_.size();
    entries_.clear();
    active_generation_ = generation;
  }
  auto mutable_bound = std::make_shared<duckdb::SiriusParquetBoundScan>(
    std::move(files), total_num_rows);
  mutable_bound->generation       = generation;
  mutable_bound->scan_instance_id = ++next_scan_instance_id_;
  mutable_bound->fingerprint      = ++next_fingerprint_;
  std::shared_ptr<duckdb::SiriusParquetBoundScan const> bound = std::move(mutable_bound);
  entries_.emplace(bound->scan_instance_id, bound);
  ++bindings_registered_;
  return bound;
}

file_scan_bind_lifecycle_data file_scan_bind_catalog::lifecycle_data() const
{
  std::lock_guard lock(mutex_);
  return {active_generation_, entries_.size(), bindings_registered_, catalog_references_released_};
}

std::shared_ptr<duckdb::SiriusParquetBoundScan const>
file_scan_bind_catalog::resolve_parquet_scan(uint64_t generation,
                                             uint64_t scan_instance_id,
                                             uint64_t fingerprint) const
{
  std::lock_guard lock(mutex_);
  if (generation != active_generation_) {
    throw duckdb::SerializationException(
      "Sirius file scan binding generation mismatch during plan deserialization");
  }
  auto const it = entries_.find(scan_instance_id);
  if (it == entries_.end()) {
    throw duckdb::SerializationException(
      "Sirius file scan binding is no longer available during plan deserialization");
  }
  if (it->second->fingerprint != fingerprint) {
    throw duckdb::SerializationException(
      "Sirius file scan binding fingerprint mismatch during plan deserialization");
  }
  return it->second;
}

duckdb::shared_ptr<file_scan_bind_catalog> file_scan_catalog_for(duckdb::ClientContext& context)
{
  auto catalog = context.registered_state->Get<file_scan_bind_catalog>(file_scan_bind_catalog::kStateKey);
  if (!catalog) {
    throw duckdb::SerializationException(
      "no Sirius file scan binding catalog on this connection during plan deserialization");
  }
  return catalog;
}

}  // namespace sirius::scan
