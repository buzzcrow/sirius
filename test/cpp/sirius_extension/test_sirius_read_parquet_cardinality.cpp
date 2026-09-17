/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "catch.hpp"
#include "sirius_extension.hpp"

#include <cudf/io/parquet_schema.hpp>
#include <duckdb.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/storage/statistics/node_statistics.hpp>

#include <string>
#include <memory>

namespace {

constexpr duckdb::idx_t orders_row_count = 150000;
constexpr std::size_t orders_object_size  = 42'000'000;
constexpr char orders_etag[]              = "\"orders-v1\"";
constexpr sirius::io::local_file_version orders_local_version{true, orders_object_size, 123456789};

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

  CHECK(bind_data.uri == "s3://bucket/orders.parquet");
  CHECK(bind_data.total_num_rows == orders_row_count);
  CHECK(bind_data.object_size == orders_object_size);
  CHECK(bind_data.validation_etag == orders_etag);
  CHECK_FALSE(bind_data.local_version.available);

  auto copy = bind_data.Copy();
  REQUIRE(copy != nullptr);
  auto* typed_copy = dynamic_cast<duckdb::SiriusReadParquetBindData*>(copy.get());
  REQUIRE(typed_copy != nullptr);
  CHECK(typed_copy->uri == bind_data.uri);
  CHECK(typed_copy->total_num_rows == bind_data.total_num_rows);
  CHECK(typed_copy->file_metadata == footer);
  CHECK(typed_copy->object_size == orders_object_size);
  CHECK(typed_copy->validation_etag == orders_etag);
  CHECK_FALSE(typed_copy->local_version.available);
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
  CHECK(typed_local_copy->local_version == orders_local_version);
  CHECK(local_bind.Equals(*local_copy));

  duckdb::SiriusReadParquetBindData different_local_version{
    "/data/orders.parquet",
    orders_row_count,
    footer,
    orders_object_size,
    {},
    sirius::io::local_file_version{true, orders_object_size, orders_local_version.mtime_ns + 1}};
  CHECK_FALSE(local_bind.Equals(different_local_version));
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
