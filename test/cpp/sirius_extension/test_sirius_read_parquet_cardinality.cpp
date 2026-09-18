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
#include <duckdb/common/enums/filter_propagate_result.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/planner/filter/constant_filter.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/node_statistics.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/string_stats.hpp>

#include <cstring>
#include <limits>
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

template <class T>
std::vector<std::uint8_t> parquet_scalar_bytes(T value)
{
  std::vector<std::uint8_t> bytes(sizeof(value));
  std::memcpy(bytes.data(), &value, sizeof(value));
  return bytes;
}

std::shared_ptr<sirius::scan::parquet_footer_summary> footer_summary_with_minmax(
  duckdb::LogicalType type, std::vector<std::uint8_t> min, std::vector<std::uint8_t> max)
{
  auto summary = std::make_shared<sirius::scan::parquet_footer_summary>();
  summary->names = {"value"};
  summary->types = {std::move(type)};
  summary->row_groups = {{0, orders_row_count, 100, 200}};
  summary->column_null_counts = {{true, 0}};
  summary->column_minmax = {{true, {std::move(min)}, {std::move(max)}}};
  summary->total_num_rows = orders_row_count;
  return summary;
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
  CHECK(catalog.lifecycle_data().active_generation == 7);
  CHECK(catalog.lifecycle_data().active_bindings == 1);
  CHECK(catalog.lifecycle_data().bindings_registered == 1);
  CHECK(catalog.lifecycle_data().catalog_references_released == 0);
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
  auto lifecycle = catalog.lifecycle_data();
  CHECK(lifecycle.active_generation == 8);
  CHECK(lifecycle.active_bindings == 1);
  CHECK(lifecycle.bindings_registered == 2);
  CHECK(lifecycle.catalog_references_released == 1);
  // The caller's copy still owns the retired immutable payload even though
  // the catalog has released its reference for generation 7.
  CHECK(bound->files.front().validation_etag == orders_etag);
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

TEST_CASE("Sirius parquet bounds drive optimizer-safe zonemap decisions",
          "[planner-metadata][sirius_read_parquet][optimizer]")
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
  REQUIRE(stats);

  // This is the interface statistics propagation uses: a predicate beyond
  // the aggregate max may remove the get, while an overlapping predicate may not.
  duckdb::ConstantFilter above_max{duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                                   duckdb::Value::INTEGER(100)};
  CHECK(above_max.CheckStatistics(*stats) == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE);
  duckdb::ConstantFilter overlaps{duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                                  duckdb::Value::INTEGER(35)};
  CHECK(overlaps.CheckStatistics(*stats) != duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE);
}

TEST_CASE("Sirius bound footer estimates round projection bytes safely",
          "[planner-metadata][sirius_read_parquet][sizing]")
{
  sirius::scan::parquet_footer_summary summary;
  summary.row_groups = {{0, 10, 101, 1001}};

  auto const one_of_four = sirius::scan::estimate_projected_row_group(summary, 0, 1, 4);
  CHECK(one_of_four.compressed_read_bytes == 26);
  CHECK(one_of_four.decoded_working_bytes == 251);

  auto const all_columns = sirius::scan::estimate_projected_row_group(summary, 0, 4, 4);
  CHECK(all_columns.compressed_read_bytes == 101);
  CHECK(all_columns.decoded_working_bytes == 1001);
  CHECK(sirius::scan::estimate_projected_row_group(summary, 1, 1, 4).compressed_read_bytes == 0);
  CHECK(sirius::scan::estimate_projected_row_group(summary, 0, 0, 4).decoded_working_bytes == 0);
}

TEST_CASE("Sirius bound footer metering reports bind and optimizer coverage",
          "[planner-metadata][sirius_read_parquet][metering]")
{
  auto summary = footer_summary_with_minmax(
    duckdb::LogicalType::INTEGER, parquet_int32_bytes(10), parquet_int32_bytes(20));
  duckdb::SiriusReadParquetBindData bind{
    "s3://bucket/metered.parquet", orders_row_count, nullptr, orders_object_size, orders_etag, {}, summary};
  auto initial = bind.metering();
  CHECK(initial.binds_created == 1);
  CHECK(initial.bound_files == 1);
  CHECK(initial.footer_summaries == 1);
  CHECK(initial.footer_row_groups == 1);
  CHECK(initial.footer_compressed_bytes == 100);
  CHECK(initial.footer_uncompressed_bytes == 200);
  CHECK(initial.null_count_covered_columns == 1);
  CHECK(initial.minmax_covered_columns == 1);

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  REQUIRE(duckdb::SiriusReadParquetCardinality(*con.context, &bind));
  REQUIRE(duckdb::SiriusReadParquetStatistics(*con.context, &bind, 0));
  CHECK(duckdb::SiriusReadParquetStatistics(*con.context, &bind, 1) == nullptr);
  auto final = bind.metering();
  CHECK(final.cardinality_requests == 1);
  CHECK(final.statistics_requests == 2);
  CHECK(final.statistics_with_minmax == 1);
  CHECK(final.statistics_unavailable == 1);
  CHECK(final.statistics_nullability_only == 0);

  bind.scan().metering.record_materialization_attempt(2, 300, 700);
  bind.scan().metering.record_materialization_success(123);
  auto runtime = bind.metering();
  CHECK(runtime.materialization_attempts == 1);
  CHECK(runtime.materialization_successes == 1);
  CHECK(runtime.materialization_failures == 0);
  CHECK(runtime.materialized_row_groups == 2);
  CHECK(runtime.materialized_compressed_read_budget_bytes == 300);
  CHECK(runtime.materialized_decode_working_budget_bytes == 700);
  CHECK(runtime.materialized_rows == 123);

  bind.scan().metering.record_materialization_attempt(1, 50, 80);
  bind.scan().metering.record_materialization_failure();
  runtime = bind.metering();
  CHECK(runtime.materialization_attempts == 2);
  CHECK(runtime.materialization_successes == 1);
  CHECK(runtime.materialization_failures == 1);
  CHECK(runtime.materialized_row_groups == 3);
  CHECK(runtime.materialized_compressed_read_budget_bytes == 350);
  CHECK(runtime.materialized_decode_working_budget_bytes == 780);
  CHECK(runtime.materialized_rows == 123);

  bind.scan().metering.record_pipeline_task_attempt(700, 900);
  bind.scan().metering.record_pipeline_task_success(500, 300);
  runtime = bind.metering();
  CHECK(runtime.pipeline_task_attempts == 1);
  CHECK(runtime.pipeline_task_successes == 1);
  CHECK(runtime.pipeline_task_failures == 0);
  CHECK(runtime.pipeline_task_input_basis_bytes == 700);
  CHECK(runtime.pipeline_task_reservation_bytes == 900);
  CHECK(runtime.pipeline_task_peak_operator_bytes == 500);
  CHECK(runtime.pipeline_task_output_bytes == 300);

  bind.scan().metering.record_pipeline_task_attempt(200, 400);
  bind.scan().metering.record_pipeline_task_failure();
  runtime = bind.metering();
  CHECK(runtime.pipeline_task_attempts == 2);
  CHECK(runtime.pipeline_task_successes == 1);
  CHECK(runtime.pipeline_task_failures == 1);
  CHECK(runtime.pipeline_task_input_basis_bytes == 900);
  CHECK(runtime.pipeline_task_reservation_bytes == 1300);
  CHECK(runtime.pipeline_task_peak_operator_bytes == 500);
  CHECK(runtime.pipeline_task_output_bytes == 300);
}

TEST_CASE("Sirius parquet optimizer statistics require compatible evidence from every file",
          "[planner-metadata][sirius_read_parquet][optimizer]")
{
  auto first = footer_summary_with_minmax(
    duckdb::LogicalType::INTEGER, parquet_int32_bytes(10), parquet_int32_bytes(20));
  auto second = footer_summary_with_minmax(
    duckdb::LogicalType::INTEGER, parquet_int32_bytes(30), parquet_int32_bytes(40));
  duckdb::SiriusReadParquetBindData bind_data{
    {{"s3://bucket/one.parquet", nullptr, 100, {}, {}, first},
     {"s3://bucket/two.parquet", nullptr, 100, {}, {}, second}},
    orders_row_count * 2};
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto stats = duckdb::SiriusReadParquetStatistics(*con.context, &bind_data, 0);
  REQUIRE(stats);
  CHECK(duckdb::NumericStats::Min(*stats).GetValue<std::int32_t>() == 10);
  CHECK(duckdb::NumericStats::Max(*stats).GetValue<std::int32_t>() == 40);

  second->types[0] = duckdb::LogicalType::BIGINT;
  auto incompatible = duckdb::SiriusReadParquetStatistics(*con.context, &bind_data, 0);
  REQUIRE(incompatible);
  CHECK_FALSE(duckdb::NumericStats::HasMinMax(*incompatible));
}

TEST_CASE("Sirius parquet statistics decode safe non-integer bounds",
          "[planner-metadata][sirius_read_parquet]")
{
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto stats_for = [&](duckdb::LogicalType type, auto min, auto max) {
    auto summary = footer_summary_with_minmax(type, std::move(min), std::move(max));
    duckdb::SiriusReadParquetBindData bind{
      "s3://bucket/typed.parquet", orders_row_count, nullptr, orders_object_size, orders_etag, {}, summary};
    return duckdb::SiriusReadParquetStatistics(*con.context, &bind, 0);
  };

  auto float_stats = stats_for(duckdb::LogicalType::FLOAT,
                               parquet_scalar_bytes(1.25F), parquet_scalar_bytes(9.5F));
  REQUIRE(float_stats);
  CHECK(duckdb::NumericStats::Min(*float_stats).GetValue<float>() == 1.25F);
  CHECK(duckdb::NumericStats::Max(*float_stats).GetValue<float>() == 9.5F);

  auto date_stats = stats_for(duckdb::LogicalType::DATE,
                              parquet_scalar_bytes(std::int32_t{18'000}),
                              parquet_scalar_bytes(std::int32_t{18'100}));
  REQUIRE(date_stats);
  CHECK(duckdb::NumericStats::Min(*date_stats) == duckdb::Value::DATE(duckdb::date_t{18'000}));

  auto time_summary = footer_summary_with_minmax(duckdb::LogicalType::TIME,
                                                  parquet_scalar_bytes(std::int32_t{1'000}),
                                                  parquet_scalar_bytes(std::int32_t{2'000}));
  time_summary->column_minmax[0].time_unit = sirius::scan::parquet_stat_time_unit::millis;
  duckdb::SiriusReadParquetBindData time_bind{
    "s3://bucket/time.parquet", orders_row_count, nullptr, orders_object_size, orders_etag, {}, time_summary};
  auto time_stats = duckdb::SiriusReadParquetStatistics(*con.context, &time_bind, 0);
  REQUIRE(time_stats);
  CHECK(duckdb::NumericStats::Min(*time_stats) == duckdb::Value::TIME(duckdb::dtime_t{1'000'000}));

  auto timestamp_summary = footer_summary_with_minmax(duckdb::LogicalType::TIMESTAMP,
                                                       parquet_scalar_bytes(std::int64_t{1'000}),
                                                       parquet_scalar_bytes(std::int64_t{2'000}));
  timestamp_summary->column_minmax[0].time_unit = sirius::scan::parquet_stat_time_unit::millis;
  duckdb::SiriusReadParquetBindData timestamp_bind{"s3://bucket/timestamp.parquet",
                                                    orders_row_count,
                                                    nullptr,
                                                    orders_object_size,
                                                    orders_etag,
                                                    {},
                                                    timestamp_summary};
  auto timestamp_stats = duckdb::SiriusReadParquetStatistics(*con.context, &timestamp_bind, 0);
  REQUIRE(timestamp_stats);
  CHECK(duckdb::NumericStats::Max(*timestamp_stats) ==
        duckdb::Value::TIMESTAMP(duckdb::timestamp_t{2'000'000}));

  auto nanos_summary = footer_summary_with_minmax(duckdb::LogicalType::TIMESTAMP,
                                                   parquet_scalar_bytes(std::int64_t{1}),
                                                   parquet_scalar_bytes(std::int64_t{2}));
  nanos_summary->column_minmax[0].time_unit = sirius::scan::parquet_stat_time_unit::nanos;
  duckdb::SiriusReadParquetBindData nanos_bind{
    "s3://bucket/nanos.parquet", orders_row_count, nullptr, orders_object_size, orders_etag, {}, nanos_summary};
  auto nanos_stats = duckdb::SiriusReadParquetStatistics(*con.context, &nanos_bind, 0);
  REQUIRE(nanos_stats);
  CHECK_FALSE(duckdb::NumericStats::HasMinMax(*nanos_stats));

  auto decimal_type = duckdb::LogicalType::DECIMAL(10, 2);
  auto decimal_stats = stats_for(decimal_type, parquet_scalar_bytes(std::int64_t{-125}),
                                 parquet_scalar_bytes(std::int64_t{325}));
  REQUIRE(decimal_stats);
  CHECK(duckdb::NumericStats::Min(*decimal_stats) == duckdb::Value::DECIMAL(-125, 10, 2));

  // BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY decimal statistics are two's-complement
  // big-endian and only arrive here after the bind-time schema proof.
  auto byte_decimal_type = duckdb::LogicalType::DECIMAL(18, 2);
  auto byte_decimal_summary = footer_summary_with_minmax(
    byte_decimal_type, std::vector<std::uint8_t>{0xff, 0x83}, std::vector<std::uint8_t>{0x01, 0x45});
  byte_decimal_summary->column_minmax[0].encoding =
    sirius::scan::parquet_minmax_encoding::big_endian_decimal;
  duckdb::SiriusReadParquetBindData byte_decimal_bind{
    "s3://bucket/byte-decimal.parquet",
    orders_row_count,
    nullptr,
    orders_object_size,
    orders_etag,
    {},
    byte_decimal_summary};
  auto byte_decimal_stats = duckdb::SiriusReadParquetStatistics(*con.context, &byte_decimal_bind, 0);
  REQUIRE(byte_decimal_stats);
  CHECK(duckdb::NumericStats::Min(*byte_decimal_stats) ==
        duckdb::Value::DECIMAL(-125, 18, 2));
  CHECK(duckdb::NumericStats::Max(*byte_decimal_stats) ==
        duckdb::Value::DECIMAL(325, 18, 2));

  auto wide_decimal_type = duckdb::LogicalType::DECIMAL(38, 2);
  auto wide_decimal_summary = footer_summary_with_minmax(
    wide_decimal_type,
    std::vector<std::uint8_t>{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
    std::vector<std::uint8_t>{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
  wide_decimal_summary->column_minmax[0].encoding = sirius::scan::parquet_minmax_encoding::big_endian_decimal;
  duckdb::SiriusReadParquetBindData wide_decimal_bind{
    "s3://bucket/wide-decimal.parquet",
    orders_row_count,
    nullptr,
    orders_object_size,
    orders_etag,
    {},
    wide_decimal_summary};
  auto wide_decimal = duckdb::SiriusReadParquetStatistics(*con.context, &wide_decimal_bind, 0);
  REQUIRE(wide_decimal);
  CHECK(duckdb::NumericStats::Min(*wide_decimal) == duckdb::Value::DECIMAL(-1, 38, 2));
  CHECK(duckdb::NumericStats::Max(*wide_decimal) ==
        duckdb::Value::DECIMAL(duckdb::hugeint_t{1, 0}, 38, 2));

  // A footer value outside its declared precision remains unknown instead of
  // being truncated to the declared decimal carrier.
  auto invalid_decimal_summary = footer_summary_with_minmax(
    byte_decimal_type, std::vector<std::uint8_t>(9, 0), std::vector<std::uint8_t>(9, 0x7f));
  invalid_decimal_summary->column_minmax[0].encoding =
    sirius::scan::parquet_minmax_encoding::big_endian_decimal;
  duckdb::SiriusReadParquetBindData invalid_decimal_bind{
    "s3://bucket/invalid-decimal.parquet",
    orders_row_count,
    nullptr,
    orders_object_size,
    orders_etag,
    {},
    invalid_decimal_summary};
  auto invalid_decimal = duckdb::SiriusReadParquetStatistics(*con.context, &invalid_decimal_bind, 0);
  REQUIRE(invalid_decimal);
  CHECK_FALSE(duckdb::NumericStats::HasMinMax(*invalid_decimal));

  auto text_stats = stats_for(duckdb::LogicalType::VARCHAR,
                              std::vector<std::uint8_t>{'a'}, std::vector<std::uint8_t>{'z'});
  REQUIRE(text_stats);
  CHECK(duckdb::StringStats::Min(*text_stats) == "a");
  CHECK(duckdb::StringStats::Max(*text_stats) == "z");
}

TEST_CASE("Sirius parquet statistics refuse unsafe floating and text bounds",
          "[planner-metadata][sirius_read_parquet]")
{
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto nan = std::numeric_limits<float>::quiet_NaN();
  auto summary = footer_summary_with_minmax(duckdb::LogicalType::FLOAT,
                                            parquet_scalar_bytes(1.0F), parquet_scalar_bytes(nan));
  duckdb::SiriusReadParquetBindData bind{
    "s3://bucket/unsafe.parquet", orders_row_count, nullptr, orders_object_size, orders_etag, {}, summary};
  auto nan_stats = duckdb::SiriusReadParquetStatistics(*con.context, &bind, 0);
  REQUIRE(nan_stats);
  CHECK_FALSE(duckdb::NumericStats::HasMinMax(*nan_stats));

  auto invalid_utf8 = footer_summary_with_minmax(
    duckdb::LogicalType::VARCHAR, std::vector<std::uint8_t>{0xff}, std::vector<std::uint8_t>{0xff});
  duckdb::SiriusReadParquetBindData invalid_bind{
    "s3://bucket/invalid.parquet", orders_row_count, nullptr, orders_object_size, orders_etag, {}, invalid_utf8};
  auto invalid_stats = duckdb::SiriusReadParquetStatistics(*con.context, &invalid_bind, 0);
  REQUIRE(invalid_stats);
  CHECK(duckdb::StringStats::Min(*invalid_stats).empty());
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
