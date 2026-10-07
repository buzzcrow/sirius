/*
 * Copyright 2026, Sirius Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "io/cache/metadata_cache.hpp"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <thread>
#include <vector>

using namespace sirius::io;
using cache_type = cache::metadata_cache;
namespace {
struct value : io_object_metadata {
  explicit value(size_t bytes) : bytes(bytes) {}
  size_t retained_bytes() const noexcept override { return bytes; }
  size_t bytes;
};
object_identity tag(std::string version = "v1")
{
  return {identity_kind::object_tag, 32, std::move(version)};
}
cache_type::options policy()
{
  cache_type::options p;
  p.background = false;
  return p;
}
}  // namespace
TEST_CASE("Metadata cache isolates identity profile and scope", "[metadata_cache]")
{
  cache_type c(policy());
  auto v = std::make_shared<value>(100);
  c.put(1, "file", "parser", tag(), 1, v);
  CHECK(c.get(1, "file", "parser", tag()) == v);
  CHECK_FALSE(c.get(2, "file", "parser", tag()));
  CHECK_FALSE(c.get(1, "file", "other", tag()));
  CHECK_FALSE(c.get(1, "file", "parser", tag("v2")));
  auto next = std::make_shared<value>(200);
  c.put(1, "file", "parser", tag("v2"), 2, next);
  CHECK(c.statistics().entries == 1);
  c.put(1, "file", "parser", tag(), 1, v);
  CHECK(c.get(1, "file", "parser", tag("v2")) == next);
  CHECK(c.statistics().bypasses == 1);
  c.put(1, "file", "parser", tag("v2"), 4, next);
  c.put(1, "file", "parser", tag("late"), 3, v);
  CHECK(c.get(1, "file", "parser", tag("v2")) == next);
  CHECK(c.statistics().entries == 1);
  CHECK(c.statistics().bypasses == 2);
  c.erase_scope(1);
  CHECK(c.statistics().retained_bytes == 0);
  CHECK(v->bytes == 100);
}
TEST_CASE("Metadata cache capacity is shared across namespaces with LRU", "[metadata_cache]")
{
  auto p     = policy();
  p.capacity = 3000;
  cache_type c(p);
  auto v = std::make_shared<value>(800);
  c.put(1, "a", "p", tag(), 1, v);
  c.put(2, "b", "p", tag(), 2, std::make_shared<value>(800));
  REQUIRE(c.statistics().entries == 2);
  REQUIRE(c.get(1, "a", "p", tag()));
  c.put(3, "c", "p", tag(), 3, std::make_shared<value>(800));
  CHECK(c.statistics().retained_bytes <= p.capacity);
  CHECK_FALSE(c.get(2, "b", "p", tag()));
  CHECK(c.get(1, "a", "p", tag()));
  CHECK(c.get(3, "c", "p", tag()));
  CHECK(c.statistics().capacity_evictions == 1);
  c.put(4, "huge", "p", tag(), 4, std::make_shared<value>(p.capacity));
  CHECK(c.statistics().entries == 2);
  CHECK(c.statistics().bypasses == 1);
}
TEST_CASE("Metadata idle expiry is not extended by failed lookup or candidate", "[metadata_cache]")
{
  auto now = cache_type::clock::time_point{};
  auto p   = policy();
  p.idle   = std::chrono::seconds(10);
  p.now    = [&] { return now; };
  cache_type c(p);
  auto v                    = std::make_shared<value>(100);
  std::weak_ptr<value> weak = v;
  c.put(1, "a", "p", tag(), 1, v);
  now += std::chrono::seconds(8);
  CHECK(c.contains(1, "a"));
  CHECK_FALSE(c.get(1, "a", "p", tag("other")));
  now += std::chrono::seconds(2);
  CHECK(c.sweep() == 1);
  CHECK(c.statistics().retained_bytes == 0);
  CHECK_FALSE(weak.expired());
  v.reset();
  CHECK(weak.expired());
}
TEST_CASE("Metadata maintenance reclaims without get or put", "[metadata_cache]")
{
  std::atomic<int> elapsed{0};
  auto p       = policy();
  p.background = true;
  p.idle       = std::chrono::seconds(10);
  p.sweep      = std::chrono::milliseconds(2);
  p.now = [&] { return cache_type::clock::time_point{} + std::chrono::seconds(elapsed.load()); };
  cache_type c(p);
  c.put(1, "a", "p", tag(), 1, std::make_shared<value>(100));
  elapsed       = 11;
  auto deadline = cache_type::clock::now() + std::chrono::seconds(2);
  while (c.statistics().entries != 0 && cache_type::clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  CHECK(c.statistics().entries == 0);
}
TEST_CASE("Metadata shared global manager and concurrent bounded publication", "[metadata_cache]")
{
  auto a = cache_type::global();
  auto b = cache_type::global();
  CHECK(a == b);
  auto p     = policy();
  p.capacity = 8192;
  cache_type c(p);
  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i)
    threads.emplace_back([&, i] {
      for (int j = 0; j < 100; ++j) {
        c.put(i, std::to_string(j), "p", tag(), j, std::make_shared<value>(512));
        auto held = c.get(i, std::to_string(j), "p", tag());
      }
      c.erase_scope(i);
    });
  for (auto& thread : threads)
    thread.join();
  CHECK(c.statistics().retained_bytes == 0);
  CHECK(c.statistics().entries == 0);
}

TEST_CASE("Metadata live charge survives eviction until the last reader releases it",
          "[metadata_cache]")
{
  auto baseline = io_object_metadata::live_retained_bytes();
  cache_type c(policy());
  auto v = std::make_shared<value>(1234);
  c.put(1, "held", "p", tag(), 1, v);
  auto reader = c.get(1, "held", "p", tag());
  CHECK(io_object_metadata::live_retained_bytes() == baseline + 1234);
  c.erase_scope(1);
  v.reset();
  CHECK(c.statistics().retained_bytes == 0);
  CHECK(io_object_metadata::live_retained_bytes() == baseline + 1234);
  reader.reset();
  CHECK(io_object_metadata::live_retained_bytes() == baseline);
}

TEST_CASE("Metadata shared allocations are charged once across aliases", "[metadata_cache]")
{
  auto p     = policy();
  p.capacity = 5000;
  cache_type c(p);
  auto v = std::make_shared<value>(3000);
  c.put(1, "a", "p", tag(), 1, v);
  auto single = c.statistics().retained_bytes;
  c.put(2, "b", "p", tag(), 1, v);
  CHECK(c.statistics().entries == 2);
  CHECK(c.statistics().retained_bytes == 2 * single - 3000);
  c.erase_scope(1);
  CHECK(c.statistics().retained_bytes == single);
  CHECK(c.get(2, "b", "p", tag()) == v);
  c.erase_scope(2);
  CHECK(c.statistics().retained_bytes == 0);
}

TEST_CASE("Metadata replacement stress and bounded sweep", "[metadata_cache]")
{
  auto now   = cache_type::clock::time_point{};
  auto p     = policy();
  p.capacity = 1 << 20;
  p.batch    = 32;
  p.now      = [&] { return now; };
  cache_type c(p);
  for (int i = 0; i < 10000; ++i) {
    auto path = std::to_string(i % 1000);
    c.put(1, path, "p", tag(std::to_string(i)), i, std::make_shared<value>(2048));
    REQUIRE(c.statistics().retained_bytes <= p.capacity);
  }
  CHECK(c.statistics().capacity_evictions > 9000);
  now += p.idle;
  CHECK(c.sweep() == p.batch);
  while (c.sweep() != 0) {}
  CHECK(c.statistics().entries == 0);
  CHECK(c.statistics().retained_bytes == 0);
}

TEST_CASE("Open-local entries never suppress the next open footer probe", "[metadata_cache]")
{
  cache_type c(policy());
  object_identity identity{identity_kind::opened_handle, 42, "1"};
  auto v = std::make_shared<value>(256);
  c.put(1, "file", "p", identity, 1, v);
  CHECK_FALSE(c.contains(1, "file"));
  CHECK(c.get(1, "file", "p", identity) == v);
  identity.version = "2";
  CHECK_FALSE(c.get(1, "file", "p", identity));
  c.erase_scope(1);
  CHECK(c.statistics().retained_bytes == 0);
}

TEST_CASE("Metadata eviction releases payloads outside the manager lock", "[metadata_cache]")
{
  cache_type c(policy());
  bool released = false;
  struct reentrant_value : value {
    std::function<void()> on_release;
    explicit reentrant_value(std::function<void()> callback)
      : value(128), on_release(std::move(callback))
    {
    }
    ~reentrant_value() override { on_release(); }
  };
  c.put(1, "file", "p", tag(), 1, std::make_shared<reentrant_value>([&] {
          released = c.statistics().entries == 0;
        }));
  c.erase_scope(1);
  CHECK(released);
}
