/*
 * Copyright 2026, Sirius Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */
// Hidden host-only workload. This measures manager overhead, not Parquet parsing
// or TPC-H/TPC-DS performance. Run '[metadata_cache_bench]' explicitly.
#include "io/cache/metadata_cache.hpp"

#include <catch2/catch_test_macros.hpp>

#include <barrier>
#include <cstdio>
#include <thread>
#include <vector>

namespace {
struct benchmark_metadata : sirius::io::io_object_metadata {
  std::vector<char> bytes = std::vector<char>(4096);
  size_t retained_bytes() const noexcept override { return sizeof(*this) + bytes.capacity(); }
};
}  // namespace
TEST_CASE("Metadata cache synthetic 10000-file workload", "[.][metadata_cache_bench]")
{
  using namespace sirius::io;
  using cache::metadata_cache;
  metadata_cache::options policy;
  policy.background = false;
  metadata_cache cache(policy);
  object_identity identity{identity_kind::object_tag, 1024 * 1024, "benchmark-version"};
  std::vector<std::string> paths;
  for (int i = 0; i < 10000; ++i)
    paths.push_back("s3://benchmark-bucket/dataset/part-" + std::to_string(i) + ".parquet");
  auto start = metadata_cache::clock::now();
  for (auto const& path : paths)
    cache.put(1, path, "parquet-v1", identity, 1, std::make_shared<benchmark_metadata>());
  auto elapsed = [&] {
    return std::chrono::duration<double, std::milli>(metadata_cache::clock::now() - start).count();
  };
  std::printf(
    "cold_insert files=10000 ms=%.3f charge=%zu\n", elapsed(), cache.statistics().retained_bytes);
  REQUIRE(cache.statistics().entries == paths.size());
  for (int workers : {1, 4, 8}) {
    for (int sample = 0; sample < 3; ++sample) {
      constexpr int operations = 100000;
      std::barrier gate(workers + 1);
      std::atomic<int> hits{0};
      std::vector<std::thread> threads;
      for (int worker = 0; worker < workers; ++worker)
        threads.emplace_back([&, worker] {
          gate.arrive_and_wait();
          int local_hits = 0;
          for (int i = worker; i < operations; i += workers) {
            auto metadata = cache.get(1, paths[i % paths.size()], "parquet-v1", identity);
            local_hits += static_cast<bool>(metadata);
          }
          hits.fetch_add(local_hits, std::memory_order_relaxed);
        });
      start = metadata_cache::clock::now();
      gate.arrive_and_wait();
      for (auto& thread : threads)
        thread.join();
      std::printf(
        "warm_hit workers=%d sample=%d ops=%d ms=%.3f\n", workers, sample, operations, elapsed());
      CHECK(hits == operations);
    }
  }
  cache.erase_scope(1);
  CHECK(cache.statistics().retained_bytes == 0);
}
