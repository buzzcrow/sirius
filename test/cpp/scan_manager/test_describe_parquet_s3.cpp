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

#include "catch.hpp"
#include "io/rest/rest_ioctx.hpp"
#include "io/sirius_datasource.hpp"
#include "io/types.hpp"
#include "memory/topology_index.hpp"
#include "op/scan/parquet_metadata.hpp"
#include "scan/parquet_footer_summary.hpp"
#include "scan/test_utils.hpp"
#include "scan_manager/sirius_scan_manager.hpp"
#include "utils/s3_container.hpp"

#include <cucascade/memory/topology_discovery.hpp>
#include <cudf/io/parquet.hpp>
#include <duckdb.hpp>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using sirius::io::io_context_type;
using sirius::io::rest::rest_ioctx;
using sirius::scan_manager::parquet_bind_result;
using sirius::scan_manager::scan_manager_config;
using sirius::scan_manager::sirius_scan_manager;

namespace fs = std::filesystem;

std::string env_or(std::string const& name, std::string fallback = {})
{
  if (auto* value = std::getenv(name.c_str()); value != nullptr) { return value; }
  return fallback;
}

std::string require_env(std::string const& name)
{
  auto value = env_or(name);
  REQUIRE_FALSE(value.empty());
  return value;
}

cucascade::memory::system_topology_info single_gpu_topology()
{
  cucascade::memory::system_topology_info topology;
  topology.num_gpus = 1;
  cucascade::memory::gpu_topology_info gpu;
  gpu.id        = 0;
  gpu.numa_node = 0;
  topology.gpus.push_back(std::move(gpu));
  return topology;
}

std::shared_ptr<const sirius::memory::topology_index> single_gpu_index()
{
  return std::make_shared<sirius::memory::topology_index>(single_gpu_topology(),
                                                          std::vector<int>{0});
}

struct scan_manager_fixture {
  std::unique_ptr<sirius::memory::sirius_memory_reservation_manager> memory =
    initialize_memory_manager(1);
  std::shared_ptr<const sirius::memory::topology_index> topology = single_gpu_index();
};

scan_manager_config make_minio_rest_config(bool perf_instrumentation = false)
{
  scan_manager_config cfg{};
  cfg.use_sirius_datasource     = true;
  cfg.object_store.endpoint     = require_env("SIRIUS_TEST_S3_ENDPOINT");
  cfg.object_store.region       = env_or("SIRIUS_TEST_S3_REGION", "us-east-1");
  cfg.object_store.access_key   = require_env("SIRIUS_TEST_S3_ACCESS_KEY");
  cfg.object_store.secret_key   = require_env("SIRIUS_TEST_S3_SECRET_KEY");
  cfg.object_store.tls_verify   = false;
  cfg.rest.request_timeout_s    = 30;
  cfg.rest.max_connections      = 8;
  cfg.rest.perf_instrumentation = perf_instrumentation;
  cfg.rest_n_reactors           = 1;
  cfg.enable_prefetch_cache     = false;
  return cfg;
}

std::string parquet_uri(std::string const& bucket, std::string const& file_name)
{
  return "s3://" + bucket + "/parquet/" + file_name;
}

std::string sql_quote(std::string_view value)
{
  std::string out{"'"};
  for (char c : value) {
    if (c == '\'') { out.push_back('\''); }
    out.push_back(c);
  }
  out.push_back('\'');
  return out;
}

fs::path parquet_fixture(std::string_view file_name)
{
  return fs::path{SIRIUS_PROJECT_ROOT} / "test" / "cpp" / "integration" / "data" / "parquet" /
         file_name;
}

std::vector<std::uint8_t> read_file_bytes(fs::path const& path)
{
  std::ifstream input(path, std::ios::binary);
  REQUIRE(input.is_open());
  return {std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

struct duckdb_parquet_bind_shape {
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<std::string> names;
};

duckdb_parquet_bind_shape duckdb_read_parquet_shape(fs::path const& path)
{
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto result = con.Query("SELECT * FROM read_parquet(" + sql_quote(path.string()) + ") LIMIT 0");
  REQUIRE(result);
  INFO((result->HasError() ? result->GetError() : ""));
  REQUIRE_FALSE(result->HasError());
  return duckdb_parquet_bind_shape{result->types, result->names};
}

std::vector<std::string> bind_names(parquet_bind_result const& result)
{
  return {result.names.begin(), result.names.end()};
}

std::vector<std::string> bind_type_strings(parquet_bind_result const& result)
{
  std::vector<std::string> out;
  out.reserve(result.return_types.size());
  for (auto const& type : result.return_types) {
    out.push_back(type.ToString());
  }
  return out;
}

void require_same_bind_result(parquet_bind_result const& lhs, parquet_bind_result const& rhs)
{
  REQUIRE(lhs.object_size == rhs.object_size);
  REQUIRE(lhs.total_num_rows == rhs.total_num_rows);
  REQUIRE(bind_names(lhs) == bind_names(rhs));
  REQUIRE(bind_type_strings(lhs) == bind_type_strings(rhs));
}

void check_bind_shape_matches_duckdb(parquet_bind_result const& actual,
                                     duckdb_parquet_bind_shape const& expected)
{
  REQUIRE(actual.names == expected.names);
  REQUIRE(actual.return_types.size() == expected.types.size());
  for (std::size_t i = 0; i < expected.types.size(); ++i) {
    INFO("column=" << expected.names[i] << " actual=" << actual.return_types[i].ToString()
                   << " expected=" << expected.types[i].ToString());
    CHECK(actual.return_types[i] == expected.types[i]);
  }
}

rest_ioctx* require_rest_ioctx(std::shared_ptr<sirius::io::sirius_datasource> const& ds)
{
  REQUIRE(ds != nullptr);
  REQUIRE(ds->io_ctx() != nullptr);
  CHECK(ds->io_ctx()->type() == io_context_type::restful);
  auto* rest_ctx = dynamic_cast<rest_ioctx*>(ds->io_ctx().get());
  REQUIRE(rest_ctx != nullptr);
  return rest_ctx;
}

rest_ioctx* require_rest_ioctx_for(sirius_scan_manager& manager, std::string const& uri)
{
  return require_rest_ioctx(manager.create_datasource(uri));
}

std::uint64_t chunk_get_count(rest_ioctx const& ctx) { return ctx.perf_snapshot().chunk_get_count; }

std::uint64_t saturating_delta(std::uint64_t after, std::uint64_t before)
{
  return after >= before ? after - before : 0;
}

parquet_bind_result describe_with_counter(sirius_scan_manager& manager,
                                          std::string const& uri,
                                          std::uint64_t& delta)
{
  auto* rest_ctx    = require_rest_ioctx_for(manager, uri);
  auto const before = chunk_get_count(*rest_ctx);
  auto result       = manager.describe_parquet(uri);
  auto const after  = chunk_get_count(*rest_ctx);
  REQUIRE(after >= before);
  delta = after - before;
  return result;
}

}  // namespace

TEST_CASE("describe_parquet routes S3 parquet through rest_ioctx and returns nation schema",
          "[s3][integration][describe_parquet]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket = require_env("SIRIUS_TEST_S3_BUCKET");
  auto const uri    = parquet_uri(bucket, "nation.parquet");

  scan_manager_fixture fixture;
  sirius_scan_manager manager{make_minio_rest_config(), *fixture.memory, fixture.topology};

  auto result = manager.describe_parquet(uri);
  require_rest_ioctx_for(manager, uri);

  CHECK(bind_names(result) ==
        std::vector<std::string>{"n_nationkey", "n_name", "n_regionkey", "n_comment"});
  REQUIRE(result.return_types.size() == result.names.size());
  REQUIRE(result.return_types.size() == 4);
  auto const type_strings = bind_type_strings(result);
  INFO("nation type strings: " << type_strings[0] << ", " << type_strings[1] << ", "
                               << type_strings[2] << ", " << type_strings[3]);
  for (auto const& type : type_strings) {
    CHECK_FALSE(type.empty());
  }
  CHECK(result.total_num_rows == 25);

  auto datasource = manager.create_datasource(uri);
  REQUIRE(datasource != nullptr);
  CHECK(result.object_size == datasource->size());
  CHECK(result.object_size > 0);
}

TEST_CASE("S3 bind budgets and physical data bytes remain separately observable",
          "[s3][integration][describe_parquet][metering]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket = require_env("SIRIUS_TEST_S3_BUCKET");
  auto const uri    = parquet_uri(bucket, "nation.parquet");
  scan_manager_fixture fixture;
  sirius_scan_manager manager{
    make_minio_rest_config(/*perf_instrumentation=*/true), *fixture.memory, fixture.topology};
  auto* rest = require_rest_ioctx_for(manager, uri);

  auto const before_bind = rest->perf_snapshot();
  auto bound             = manager.describe_parquet(uri);
  auto const after_bind  = rest->perf_snapshot();
  auto const bind_payload_bytes =
    saturating_delta(after_bind.payload_bytes_read_total, before_bind.payload_bytes_read_total);
  auto const bind_gets = saturating_delta(after_bind.chunk_get_count, before_bind.chunk_get_count);

  REQUIRE(bound.footer_summary != nullptr);
  REQUIRE_FALSE(bound.footer_summary->row_groups.empty());
  CHECK(bind_gets == 1);  // one suffix Range GET supplies bind/version evidence
  CHECK(bind_payload_bytes > 0);
  CHECK(after_bind.chunk_get_p50_ns > 0);
  CHECK(after_bind.chunk_get_p95_ns >= after_bind.chunk_get_p50_ns);
  CHECK(after_bind.active_get_requests == 0);
  CHECK(after_bind.peak_active_get_requests >= 1);

  std::uint64_t projected_budget_bytes = 0;
  for (std::size_t row_group = 0; row_group < bound.footer_summary->row_groups.size(); ++row_group) {
    projected_budget_bytes += sirius::scan::estimate_projected_row_group(
                                *bound.footer_summary,
                                row_group,
                                /*projected_top_level_columns=*/1,
                                bound.names.size())
                                .compressed_read_bytes;
  }
  CHECK(projected_budget_bytes > 0);

  // Read one projected column through the bound size/ETag datasource.  The
  // footer budget is intentionally not asserted equal to transport bytes:
  // page layout, range coalescing and cache state are backend details.  Both
  // values must remain observable, non-zero facts for the same bound object.
  auto source = rest->open_datasource(uri, bound.object_size, bound.validation_etag);
  REQUIRE(source != nullptr);
  std::vector<std::unique_ptr<cudf::io::datasource>> sources;
  sources.push_back(source->duplicate());
  std::vector<cudf::io::parquet::FileMetaData> metadata;
  metadata.push_back(*bound.file_metadata);
  auto options = cudf::io::parquet_reader_options::builder()
                   .column_names({bound.names.front()})
                   .build();

  auto const before_data = rest->perf_snapshot();
  auto [table, ignored_metadata] =
    cudf::io::read_parquet(std::move(sources), std::move(metadata), options);
  (void)ignored_metadata;
  auto const after_data = rest->perf_snapshot();
  auto const data_payload_bytes =
    saturating_delta(after_data.payload_bytes_read_total, before_data.payload_bytes_read_total);
  auto const data_gets = saturating_delta(after_data.chunk_get_count, before_data.chunk_get_count);

  REQUIRE(table != nullptr);
  CHECK(table->num_rows() == static_cast<cudf::size_type>(bound.total_num_rows));
  CHECK(table->num_columns() == 1);
  CHECK(data_gets > 0);
  CHECK(data_payload_bytes > 0);
  INFO("bind_payload_bytes=" << bind_payload_bytes
                              << " projected_budget_bytes=" << projected_budget_bytes
                              << " data_payload_bytes=" << data_payload_bytes
                              << " bind_gets=" << bind_gets << " data_gets=" << data_gets
                              << " bind_p50_ns=" << after_bind.chunk_get_p50_ns
                              << " bind_p95_ns=" << after_bind.chunk_get_p95_ns
                              << " peak_active_gets=" << after_data.peak_active_get_requests);
}

TEST_CASE("describe_parquet reports stable row counts for multiple S3 parquet objects",
          "[s3][integration][describe_parquet]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket     = require_env("SIRIUS_TEST_S3_BUCKET");
  auto const nation_uri = parquet_uri(bucket, "nation.parquet");
  auto const region_uri = parquet_uri(bucket, "region.parquet");

  scan_manager_fixture fixture;
  sirius_scan_manager manager{make_minio_rest_config(), *fixture.memory, fixture.topology};

  auto nation = manager.describe_parquet(nation_uri);
  auto region = manager.describe_parquet(region_uri);

  CHECK(nation.total_num_rows == 25);
  CHECK(region.total_num_rows == 5);
  CHECK(bind_names(region) == std::vector<std::string>{"r_regionkey", "r_name", "r_comment"});
  CHECK(region.object_size > 0);
}

TEST_CASE("describe_parquet maps nested local parquet bind shape like DuckDB CPU read_parquet",
          "[scan_manager][describe_parquet][s3][nested]")
{
  scan_manager_fixture fixture;
  scan_manager_config cfg{};
  cfg.use_sirius_datasource = true;
  sirius_scan_manager manager{std::move(cfg), *fixture.memory, fixture.topology};

  for (auto const fixture_name : {"nested_struct.parquet",
                                  "nested_list.parquet",
                                  "nested_map.parquet",
                                  "nested_deep.parquet"}) {
    auto const path     = parquet_fixture(fixture_name);
    auto const expected = duckdb_read_parquet_shape(path);
    auto const uri      = "file://" + path.string();

    auto bind_info = manager.describe_parquet(uri);

    INFO("fixture=" << fixture_name);
    check_bind_shape_matches_duckdb(bind_info, expected);
    CHECK(bind_info.total_num_rows > 0);
    CHECK(bind_info.object_size > 0);
    REQUIRE(bind_info.local_version.available);
    CHECK(bind_info.local_version.size == bind_info.object_size);
  }
}

TEST_CASE("describe_parquet retains proven local temporal footer units",
          "[scan_manager][describe_parquet][temporal]")
{
  scan_manager_fixture fixture;
  scan_manager_config cfg{};
  cfg.use_sirius_datasource = true;
  sirius_scan_manager manager{std::move(cfg), *fixture.memory, fixture.topology};

  auto const path = fs::temp_directory_path() / "sirius_describe_parquet_temporal_stats.parquet";
  std::error_code ec;
  fs::remove(path, ec);

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto create = con.Query(
    "CREATE TABLE temporal_stats AS SELECT TIME '00:00:01.000001' AS t, "
    "TIMESTAMP '2024-01-01 00:00:01.000001' AS ts UNION ALL "
    "SELECT TIME '00:00:02.000002', TIMESTAMP '2024-01-01 00:00:02.000002'");
  REQUIRE(create);
  REQUIRE_FALSE(create->HasError());
  auto copy = con.Query("COPY temporal_stats TO " + sql_quote(path.string()) + " (FORMAT PARQUET)");
  REQUIRE(copy);
  REQUIRE_FALSE(copy->HasError());

  auto const bind = manager.describe_parquet("file://" + path.string());
  REQUIRE(bind.footer_summary);
  REQUIRE(bind.footer_summary->names.size() == 2);
  REQUIRE(bind.footer_summary->column_minmax.size() == 2);
  for (auto const& summary : bind.footer_summary->column_minmax) {
    CHECK(summary.complete);
    CHECK(summary.time_unit == sirius::scan::parquet_stat_time_unit::micros);
    REQUIRE(summary.min_values.size() == 1);
    REQUIRE(summary.max_values.size() == 1);
    CHECK(summary.min_values.front().size() == sizeof(std::int64_t));
    CHECK(summary.max_values.front().size() == sizeof(std::int64_t));
  }

  fs::remove(path, ec);
}

TEST_CASE("describe_parquet retains decimal128 FLBA statistics only with schema proof",
          "[scan_manager][describe_parquet][decimal]")
{
  scan_manager_fixture fixture;
  scan_manager_config cfg{};
  cfg.use_sirius_datasource = true;
  sirius_scan_manager manager{std::move(cfg), *fixture.memory, fixture.topology};

  auto const path = fs::temp_directory_path() / "sirius_describe_parquet_decimal128_stats.parquet";
  std::error_code ec;
  fs::remove(path, ec);

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto copy = con.Query(
    "COPY (SELECT CAST(-1 AS DECIMAL(38, 4)) AS d "
    "UNION ALL SELECT CAST(18446744073709551616 AS DECIMAL(38, 4))) TO " +
    sql_quote(path.string()) + " (FORMAT PARQUET)");
  REQUIRE(copy);
  REQUIRE_FALSE(copy->HasError());

  auto const bind = manager.describe_parquet("file://" + path.string());
  REQUIRE(bind.footer_summary);
  REQUIRE(bind.footer_summary->types == std::vector<duckdb::LogicalType>{duckdb::LogicalType::DECIMAL(38, 4)});
  REQUIRE(bind.footer_summary->column_minmax.size() == 1);
  auto const& summary = bind.footer_summary->column_minmax.front();
  REQUIRE(summary.complete);
  CHECK(summary.encoding == sirius::scan::parquet_minmax_encoding::big_endian_decimal);
  REQUIRE(summary.min_values.size() == 1);
  REQUIRE(summary.max_values.size() == 1);
  CHECK(summary.min_values.front().size() == 16);
  CHECK(summary.max_values.front().size() == 16);

  fs::remove(path, ec);
}

TEST_CASE("describe_parquet maps nested S3 parquet bind shape like DuckDB CPU read_parquet",
          "[s3][integration][describe_parquet][nested]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket = require_env("SIRIUS_TEST_S3_BUCKET");

  scan_manager_fixture fixture;
  sirius_scan_manager manager{make_minio_rest_config(), *fixture.memory, fixture.topology};

  for (auto const fixture_name : {"nested_struct.parquet",
                                  "nested_list.parquet",
                                  "nested_map.parquet",
                                  "nested_deep.parquet"}) {
    auto const local_path = parquet_fixture(fixture_name);
    auto const expected   = duckdb_read_parquet_shape(local_path);
    auto const uri        = parquet_uri(bucket, fixture_name);

    auto bind_info = manager.describe_parquet(uri);

    INFO("fixture=" << fixture_name);
    check_bind_shape_matches_duckdb(bind_info, expected);
    CHECK(bind_info.total_num_rows > 0);
    CHECK(bind_info.object_size > 0);
  }
}

TEST_CASE("describe_parquet surfaces missing S3 parquet objects from HEAD",
          "[s3][integration][describe_parquet]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket = require_env("SIRIUS_TEST_S3_BUCKET");
  auto const uri    = parquet_uri(bucket, "does-not-exist.parquet");

  scan_manager_fixture fixture;
  sirius_scan_manager manager{make_minio_rest_config(), *fixture.memory, fixture.topology};

  try {
    (void)manager.describe_parquet(uri);
    FAIL("describe_parquet unexpectedly succeeded for a missing S3 object");
  } catch (std::runtime_error const& e) {
    auto const message = std::string{e.what()};
    CHECK(message.find("404") != std::string::npos);
  }
}

TEST_CASE("describe_parquet parks parsed parquet metadata in the rest metadata store",
          "[s3][integration][describe_parquet]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket = require_env("SIRIUS_TEST_S3_BUCKET");
  auto const uri    = parquet_uri(bucket, "nation.parquet");

  scan_manager_fixture fixture;
  sirius_scan_manager manager{make_minio_rest_config(), *fixture.memory, fixture.topology};

  auto result = manager.describe_parquet(uri);

  auto datasource = manager.create_datasource(uri);
  REQUIRE(datasource != nullptr);
  auto metadata = datasource->metadata();
  REQUIRE(metadata != nullptr);
  auto parquet_metadata =
    std::dynamic_pointer_cast<sirius::op::scan::parquet_metadata>(std::move(metadata));
  REQUIRE(parquet_metadata != nullptr);
  REQUIRE(parquet_metadata->file_metadata() != nullptr);
  CHECK(result.file_metadata == parquet_metadata->file_metadata());
  CHECK(static_cast<std::size_t>(parquet_metadata->file_metadata()->num_rows) ==
        result.total_num_rows);
  CHECK(result.total_num_rows == 25);
  CHECK(parquet_metadata->footer_byte_len() > 0);
}

TEST_CASE("describe_parquet reuses the metadata store on repeated S3 binds",
          "[s3][integration][describe_parquet]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket = require_env("SIRIUS_TEST_S3_BUCKET");
  auto const uri    = parquet_uri(bucket, "nation.parquet");

  scan_manager_fixture fixture;
  sirius_scan_manager manager{
    make_minio_rest_config(/*perf_instrumentation=*/true), *fixture.memory, fixture.topology};

  std::uint64_t cold_gets = 0;
  auto cold               = describe_with_counter(manager, uri, cold_gets);
  CHECK(cold_gets >= 1);

  std::uint64_t warm_gets = 0;
  auto warm               = describe_with_counter(manager, uri, warm_gets);
  // Each bind obtains one footer Range GET to validate its object version;
  // matching versions reuse the already-parsed metadata, with no HEAD.
  CHECK(warm_gets == 1);
  require_same_bind_result(cold, warm);
  CHECK(cold.file_metadata == warm.file_metadata);
}

TEST_CASE("describe_parquet isolates a MinIO same-key overwrite in the next bind generation",
          "[s3][integration][describe_parquet][version]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket = require_env("SIRIUS_TEST_S3_BUCKET");
  auto const key    = std::string{"parquet/e1-same-key-overwrite.parquet"};
  auto const uri    = "s3://" + bucket + "/" + key;
  auto const root   = fs::temp_directory_path();
  auto const first_path  = root / "sirius_e1_overwrite_v1.parquet";
  auto const second_path = root / "sirius_e1_overwrite_v2.parquet";
  std::error_code ec;
  fs::remove(first_path, ec);
  fs::remove(second_path, ec);

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  auto write = [&](fs::path const& path, std::string const& query) {
    auto result = con.Query("COPY (" + query + ") TO " + sql_quote(path.string()) + " (FORMAT PARQUET)");
    REQUIRE(result);
    REQUIRE_FALSE(result->HasError());
  };
  write(first_path, "SELECT 1::INTEGER AS generation_marker");
  write(second_path, "SELECT i::INTEGER AS generation_marker FROM range(2) AS t(i)");

  // This helper intentionally refuses external endpoints: an overwrite test
  // must not mutate a user-provided S3 bucket.
  if (!sirius::test::put_s3_container_object(key, read_file_bytes(first_path))) {
    fs::remove(first_path, ec);
    fs::remove(second_path, ec);
    SUCCEED("managed MinIO is required for same-key overwrite coverage");
    return;
  }

  scan_manager_fixture fixture;
  sirius_scan_manager manager{make_minio_rest_config(), *fixture.memory, fixture.topology};
  auto first = manager.describe_parquet(uri);
  REQUIRE(first.file_metadata);
  REQUIRE(first.total_num_rows == 1);
  auto const old_footer = first.file_metadata;

  REQUIRE(sirius::test::put_s3_container_object(key, read_file_bytes(second_path)));
  auto second = manager.describe_parquet(uri);
  REQUIRE(second.file_metadata);
  CHECK(second.total_num_rows == 2);
  CHECK(second.file_metadata != old_footer);
  // The original binding is immutable: retiring its store entry cannot change
  // what an already-bound query holds.
  CHECK(old_footer->num_rows == 1);

  fs::remove(first_path, ec);
  fs::remove(second_path, ec);
}

TEST_CASE("describe_parquet footer fetch stays bounded for small and larger S3 parquet objects",
          "[s3][integration][describe_parquet]")
{
  if (!sirius::test::ensure_s3_container_env()) { return; }

  auto const bucket = require_env("SIRIUS_TEST_S3_BUCKET");

  auto require_bounded_footer_fetch = [&](std::string const& file_name) {
    scan_manager_fixture fixture;
    sirius_scan_manager manager{
      make_minio_rest_config(/*perf_instrumentation=*/true), *fixture.memory, fixture.topology};
    auto const uri = parquet_uri(bucket, file_name);

    std::uint64_t get_count = 0;
    auto result             = describe_with_counter(manager, uri, get_count);

    INFO(file_name << " footer GET count: " << get_count
                   << ", object_size: " << result.object_size);
    CHECK(get_count >= 1);
    CHECK(get_count <= 4);
    CHECK(result.object_size > 0);
    CHECK(result.total_num_rows > 0);
  };

  require_bounded_footer_fetch("nation.parquet");
  require_bounded_footer_fetch("lineitem.parquet");
}
