/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "catch.hpp"
#include "scan/file_scan_bind_catalog.hpp"
#include "sirius_extension.hpp"

#include <cudf/io/parquet_schema.hpp>
#include <duckdb.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/node_statistics.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>

#include <cstring>
#include <string>
#include <memory>

namespace {

constexpr duckdb::idx_t orders_row_count = 150000;
constexpr std::size_t orders_object_size  = 42'000'000;
constexpr char orders_etag[]              = "\"orders-v1\"";
constexpr sirius::io::local_file_version orders_local_version{true, orders_object_size, 123456789};

std::shared_ptr<sirius::scan::parquet_footer_summary> footer_summary_with_null_evidence(
  bool complete,
  std::uint64_t null_count)
{
  auto summary = std::make_shared<sirius::scan::parquet_footer_summary>();
  summary->names = {"orderkey"};
  summary->types = {duckdb::LogicalType::INTEGER};
  summary->row_groups = {{0, orders_row_count, 100, 200}};
  summary->column_null_counts = {{complete, null_count}};
  summary->total_num_rows = orders_row_count;
  return summary;
}

std::vector<std::uint8_t> parquet_int32_bytes(std::int32_t value)
{
  std::vector<std::uint8_t> bytes(sizeof(value));
  std::memcpy(bytes.data(), &value, sizeof(value));
  return bytes;
}

struct unrelated_function_data : public duckdb::FunctionData {
  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override
  {
    return duckdb::unique_ptr<duckdb::FunctionData>(new unrelated_function_data());
  }

  bool Equals(duckdb::FunctionData const& other) const override { return this == &other; }
};

}  // namespace

TEST_CASE("SiriusReadParquetBindData preserves URI and row-count planner metadata",
          "[planner-metadata][sirius_read_parquet]")
{
  auto footer = std::make_shared<cudf::io::parquet::FileMetaData const>();
  duckdb::SiriusReadParquetBindData bind_data{"s3://bucket/orders.parquet", orders_row_count,
                                             footer, orders_object_size, orders_etag};

  CHECK(bind_data.uri() == "s3://bucket/orders.parquet");
  CHECK(bind_data.total_num_rows() == orders_row_count);
  CHECK(bind_data.object_size() == orders_object_size);
  CHECK(bind_data.validation_etag() == orders_etag);
  CHECK_FALSE(bind_data.local_version().available);

  auto copy = bind_data.Copy();
  REQUIRE(copy != nullptr);
  auto* typed_copy = dynamic_cast<duckdb::SiriusReadParquetBindData*>(copy.get());
  REQUIRE(typed_copy != nullptr);
  CHECK(typed_copy->bound_scan == bind_data.bound_scan);
  CHECK(typed_copy->uri() == bind_data.uri());
  CHECK(typed_copy->total_num_rows() == bind_data.total_num_rows());
  CHECK(typed_copy->file_metadata() == footer);
  CHECK(typed_copy->object_size() == orders_object_size);
  CHECK(typed_copy->validation_etag() == orders_etag);
  CHECK_FALSE(typed_copy->local_version().available);
  CHECK(bind_data.Equals(*copy));

  duckdb::SiriusReadParquetBindData different_uri{"s3://bucket/lineitem.parquet", orders_row_count};
  CHECK_FALSE(bind_data.Equals(different_uri));

  duckdb::SiriusReadParquetBindData different_rows{"s3://bucket/orders.parquet",
                                                   orders_row_count + 1};
  CHECK_FALSE(bind_data.Equals(different_rows));

  duckdb::SiriusReadParquetBindData different_footer{
    "s3://bucket/orders.parquet", orders_row_count,
    std::make_shared<cudf::io::parquet::FileMetaData const>(), orders_object_size, orders_etag};
  CHECK_FALSE(bind_data.Equals(different_footer));

  duckdb::SiriusReadParquetBindData different_size{
    "s3://bucket/orders.parquet", orders_row_count, footer, orders_object_size + 1};
  CHECK_FALSE(bind_data.Equals(different_size));

  duckdb::SiriusReadParquetBindData different_etag{
    "s3://bucket/orders.parquet", orders_row_count, footer, orders_object_size, "\"orders-v2\""};
  CHECK_FALSE(bind_data.Equals(different_etag));

  duckdb::SiriusReadParquetBindData local_bind{"/data/orders.parquet",
                                                orders_row_count,
                                                footer,
                                                orders_object_size,
                                                {},
                                                orders_local_version};
  auto local_copy = local_bind.Copy();
  REQUIRE(local_copy != nullptr);
  auto* typed_local_copy = dynamic_cast<duckdb::SiriusReadParquetBindData*>(local_copy.get());
  REQUIRE(typed_local_copy != nullptr);
  CHECK(typed_local_copy->local_version() == orders_local_version);
  CHECK(local_bind.Equals(*local_copy));

  duckdb::SiriusReadParquetBindData different_local_version{
    "/data/orders.parquet",
    orders_row_count,
    footer,
    orders_object_size,
    {},
    sirius::io::local_file_version{true, orders_object_size, orders_local_version.mtime_ns + 1}};
  CHECK_FALSE(local_bind.Equals(different_local_version));
  CHECK(sirius::io::local_file_cache_id("/data/orders.parquet", orders_local_version) ==
        sirius::io::local_file_cache_id("/data/orders.parquet", orders_local_version));
  CHECK(sirius::io::local_file_cache_id("/data/orders.parquet", orders_local_version) !=
        sirius::io::local_file_cache_id(
          "/data/orders.parquet",
          sirius::io::local_file_version{true, orders_object_size, orders_local_version.mtime_ns + 1}));
}

TEST_CASE("SiriusReadParquetCardinality returns exact DuckDB node statistics",
          "[planner-metadata][sirius_read_parquet]")
{
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  duckdb::SiriusReadParquetBindData bind_data{"s3://bucket/orders.parquet", orders_row_count};

  auto stats = duckdb::SiriusReadParquetCardinality(*con.context, &bind_data);

  REQUIRE(stats != nullptr);
  CHECK(stats->has_estimated_cardinality);
  CHECK(stats->estimated_cardinality == orders_row_count);
  CHECK(stats->has_max_cardinality);
  CHECK(stats->max_cardinality == orders_row_count);
}

TEST_CASE("Sirius file scan binding catalog resolves the same immutable payload",
          "[planner-metadata][sirius_read_parquet]")
{
  sirius::scan::file_scan_bind_catalog catalog;
  auto footer = std::make_shared<cudf::io::parquet::FileMetaData const>();
  auto bound  = catalog.register_parquet_scan(
    /*generation=*/7,
    {{"s3://bucket/orders.parquet", footer, orders_object_size, orders_etag, {}}},
    orders_row_count);

  REQUIRE(bound != nullptr);
  CHECK(bound->generation == 7);
  CHECK(bound->files.size() == 1);
  CHECK(bound->files.front().file_metadata == footer);
  CHECK(catalog.resolve_parquet_scan(bound->generation, bound->scan_instance_id, bound->fingerprint) ==
        bound);

  // A new generation releases the catalog's old reference and must never make
  // an old handle resolve to a newer path/version binding.
  auto newer = catalog.register_parquet_scan(
    /*generation=*/8,
    {{"s3://bucket/orders.parquet", footer, orders_object_size, "\"orders-v2\"", {}}},
    orders_row_count);
  REQUIRE(newer != nullptr);
  CHECK_THROWS_AS(catalog.resolve_parquet_scan(bound->generation,
                                               bound->scan_instance_id,
                                               bound->fingerprint),
                  duckdb::SerializationException);
  CHECK_THROWS_AS(catalog.resolve_parquet_scan(newer->generation,
                                               newer->scan_instance_id,
                                               newer->fingerprint + 1),
                  duckdb::SerializationException);
}

TEST_CASE("SiriusReadParquetCardinality handles absent or wrong bind data defensively",
          "[planner-metadata][sirius_read_parquet]")
{
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  auto missing_stats = duckdb::SiriusReadParquetCardinality(*con.context, nullptr);
  CHECK(missing_stats == nullptr);

  unrelated_function_data unrelated;
  auto wrong_type_stats = duckdb::SiriusReadParquetCardinality(*con.context, &unrelated);
  CHECK(wrong_type_stats == nullptr);
}

TEST_CASE("Sirius parquet footer summary is immutable optimizer evidence",
          "[planner-metadata][sirius_read_parquet]")
{
  auto summary = footer_summary_with_null_evidence(/*complete=*/true, /*null_count=*/0);
  duckdb::SiriusReadParquetBindData bind_data{"s3://bucket/orders.parquet",
                                              orders_row_count,
                                              std::make_shared<cudf::io::parquet::FileMetaData const>(),
                                              orders_object_size,
                                              orders_etag,
                                              {},
                                              summary};

  REQUIRE(bind_data.files().front().footer_summary == summary);
  REQUIRE(summary->row_groups.size() == 1);
  CHECK(summary->row_groups.front().first_row == 0);
  CHECK(summary->row_groups.front().num_rows == orders_row_count);
  CHECK(summary->row_groups.front().compressed_bytes == 100);
  CHECK(summary->row_groups.front().uncompressed_bytes == 200);

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto stats = duckdb::SiriusReadParquetStatistics(*con.context, &bind_data, 0);
  REQUIRE(stats != nullptr);
  CHECK_FALSE(stats->CanHaveNull());
  CHECK(stats->CanHaveNoNull());

  auto incomplete = footer_summary_with_null_evidence(/*complete=*/false, /*null_count=*/0);
  duckdb::SiriusReadParquetBindData incomplete_bind{"s3://bucket/orders.parquet",
                                                     orders_row_count,
                                                     nullptr,
                                                     orders_object_size,
                                                     orders_etag,
                                                     {},
                                                     incomplete};
  CHECK(duckdb::SiriusReadParquetStatistics(*con.context, &incomplete_bind, 0) == nullptr);

  auto has_null = footer_summary_with_null_evidence(/*complete=*/true, /*null_count=*/1);
  duckdb::SiriusReadParquetBindData nullable_bind{"s3://bucket/orders.parquet",
                                                   orders_row_count,
                                                   nullptr,
                                                   orders_object_size,
                                                   orders_etag,
                                                   {},
                                                   has_null};
  CHECK(duckdb::SiriusReadParquetStatistics(*con.context, &nullable_bind, 0) == nullptr);
}

TEST_CASE("Sirius parquet statistics expose only complete exact integer bounds",
          "[planner-metadata][sirius_read_parquet]")
{
  auto summary = footer_summary_with_null_evidence(/*complete=*/true, /*null_count=*/0);
  summary->column_minmax = {{true,
                             {parquet_int32_bytes(10), parquet_int32_bytes(30)},
                             {parquet_int32_bytes(20), parquet_int32_bytes(40)}}};
  duckdb::SiriusReadParquetBindData bind_data{
    "s3://bucket/orders.parquet", orders_row_count, nullptr, orders_object_size, orders_etag, {}, summary};

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto stats = duckdb::SiriusReadParquetStatistics(*con.context, &bind_data, 0);
  REQUIRE(stats != nullptr);
  CHECK(duckdb::NumericStats::Min(*stats).GetValue<std::int32_t>() == 10);
  CHECK(duckdb::NumericStats::Max(*stats).GetValue<std::int32_t>() == 40);

  auto incomplete = footer_summary_with_null_evidence(/*complete=*/false, /*null_count=*/0);
  incomplete->column_minmax = {{false, {}, {}}};
  duckdb::SiriusReadParquetBindData incomplete_bind{
    "s3://bucket/orders.parquet", orders_row_count, nullptr, orders_object_size, orders_etag, {}, incomplete};
  CHECK(duckdb::SiriusReadParquetStatistics(*con.context, &incomplete_bind, 0) == nullptr);
}

TEST_CASE("Sirius multi-file binds retain independent version evidence",
          "[planner-metadata][sirius_read_parquet]")
{
  auto first_summary  = footer_summary_with_null_evidence(/*complete=*/true, /*null_count=*/0);
  auto second_summary = footer_summary_with_null_evidence(/*complete=*/true, /*null_count=*/0);
  duckdb::SiriusReadParquetBindData bind_data{
    {{"s3://bucket/part-000.parquet",
      std::make_shared<cudf::io::parquet::FileMetaData const>(),
      101,
      "\"part-000-v1\"",
      {},
      first_summary},
     {"s3://bucket/part-001.parquet",
      std::make_shared<cudf::io::parquet::FileMetaData const>(),
      202,
      "\"part-001-v9\"",
      {},
      second_summary}},
    orders_row_count * 2};

  REQUIRE(bind_data.files().size() == 2);
  CHECK(bind_data.files()[0].object_size == 101);
  CHECK(bind_data.files()[0].validation_etag == "\"part-000-v1\"");
  CHECK(bind_data.files()[1].object_size == 202);
  CHECK(bind_data.files()[1].validation_etag == "\"part-001-v9\"");
  CHECK(bind_data.files()[0].footer_summary == first_summary);
  CHECK(bind_data.files()[1].footer_summary == second_summary);

  auto copy = bind_data.Copy();
  auto* typed_copy = dynamic_cast<duckdb::SiriusReadParquetBindData*>(copy.get());
  REQUIRE(typed_copy != nullptr);
  CHECK(typed_copy->bound_scan == bind_data.bound_scan);
  CHECK(typed_copy->files()[1].validation_etag == "\"part-001-v9\"");
}
