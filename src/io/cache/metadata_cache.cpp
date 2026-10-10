/*
 * Copyright 2026, Sirius Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "io/cache/metadata_cache.hpp"

#include <limits>
#include <stdexcept>

namespace sirius::io::cache {
metadata_cache::metadata_cache(options config) : config_(std::move(config))
{
  if (!config_.now || config_.idle <= clock::duration::zero() ||
      config_.sweep <= clock::duration::zero() || config_.batch == 0)
    throw std::invalid_argument("invalid metadata cache policy");
  if (config_.background) {
    worker_ = std::thread([this] {
      std::unique_lock lock(mutex_);
      while (!wake_.wait_for(lock, config_.sweep, [this] { return stopping_; })) {
        lock.unlock();
        while (sweep() == config_.batch) {
          std::this_thread::yield();
          std::lock_guard guard(mutex_);
          if (stopping_) break;
        }
        lock.lock();
      }
    });
  }
}
metadata_cache::~metadata_cache()
{
  {
    std::lock_guard lock(mutex_);
    stopping_ = true;
  }
  wake_.notify_all();
  if (worker_.joinable()) worker_.join();
}
std::shared_ptr<metadata_cache> metadata_cache::global()
{
  static std::mutex mutex;
  static std::weak_ptr<metadata_cache> instance;
  std::lock_guard lock(mutex);
  auto result = instance.lock();
  if (!result) {
    result   = std::make_shared<metadata_cache>(options{});
    instance = result;
  }
  return result;
}
std::string metadata_cache::key(uint64_t scope, std::string const& path, std::string const& profile)
{
  return std::to_string(scope) + ":" + std::to_string(path.size()) + ":" + path + profile;
}
bool metadata_cache::expired(entry const& value, clock::time_point now) const
{
  return now - value.accessed >= config_.idle;
}
void metadata_cache::retire(iterator it, entries& retired)
{
  stats_.retained_bytes -= it->charge;
  auto charged = charges_.find(it->value.get());
  if (--charged->second.references == 0) {
    stats_.retained_bytes -= charged->second.bytes;
    charges_.erase(charged);
  }
  --stats_.entries;
  index_.erase(it->key);
  auto candidate = candidates_.find(it->candidate_key);
  if (candidate != candidates_.end() && --candidate->second == 0) candidates_.erase(candidate);
  retired.splice(retired.end(), lru_, it);
}
std::shared_ptr<io_object_metadata> metadata_cache::get(uint64_t scope,
                                                        std::string const& path,
                                                        std::string const& profile,
                                                        object_identity const& identity)
{
  auto lookup = key(scope, path, profile);
  entries retired;  // destroyed after unlocking, including arbitrary value destructors
  std::lock_guard lock(mutex_);
  auto found = index_.find(lookup);
  if (found != index_.end()) {
    auto it  = found->second;
    auto now = config_.now();
    if (expired(*it, now)) {
      ++stats_.idle_evictions;
      retire(it, retired);
    } else if (it->identity == identity) {
      it->accessed = now;
      lru_.splice(lru_.begin(), lru_, it);
      ++stats_.hits;
      return it->value;
    }
  }
  ++stats_.misses;
  return {};
}
bool metadata_cache::contains(uint64_t scope, std::string const& path)
{
  std::lock_guard lock(mutex_);
  return candidates_.contains(key(scope, path, ""));
}
void metadata_cache::put(uint64_t scope,
                         std::string const& path,
                         std::string const& profile,
                         object_identity identity,
                         uint64_t generation,
                         std::shared_ptr<io_object_metadata> value)
{
  if (!value) return;
  auto bytes  = value->account_retention();
  auto lookup = key(scope, path, profile);
  // Open-local evidence cannot help the next open; keep its footer-probe fast path.
  auto candidate_key =
    identity.kind == identity_kind::opened_handle ? std::string{} : key(scope, path, "");
  // Conservative charge includes duplicate index key, list/map nodes and buckets.
  auto overhead =
    sizeof(entry) + 256 + 4 * lookup.capacity() + path.capacity() + identity.version.capacity();
  entries retired;
  std::lock_guard lock(mutex_);
  auto found = index_.find(lookup);
  if (found != index_.end()) {
    auto it = found->second;
    if (generation < it->generation) {
      ++stats_.bypasses;
      return;
    }
    if (it->identity == identity) {
      // A duplicate publication still advances the local ordering fence.
      // Otherwise an older, different-version fill could replace this record.
      it->generation = generation;
      return;
    }
    ++stats_.replacements;
    retire(it, retired);
  }
  if (bytes == 0 || overhead > config_.capacity || bytes > config_.capacity - overhead) {
    ++stats_.bypasses;
    return;
  }
  // A shared record can be referenced by multiple compatible cache entries.
  // Recompute after each eviction: retiring its last alias changes admission.
  auto charge = [&] { return overhead + (charges_.contains(value.get()) ? 0 : bytes); };
  auto now    = config_.now();
  size_t work = 0;
  while (!lru_.empty() && work < config_.batch &&
         (expired(lru_.back(), now) || stats_.retained_bytes > config_.capacity - charge())) {
    if (expired(lru_.back(), now))
      ++stats_.idle_evictions;
    else
      ++stats_.capacity_evictions;
    retire(std::prev(lru_.end()), retired);
    ++work;
  }
  if (stats_.retained_bytes > config_.capacity - charge()) {
    ++stats_.bypasses;
    return;
  }
  lru_.push_front({std::move(lookup),
                   std::move(candidate_key),
                   scope,
                   generation,
                   std::move(identity),
                   std::move(value),
                   overhead,
                   now});
  try {
    charges_.try_emplace(lru_.front().value.get(), value_charge{0, bytes});
    if (!lru_.front().candidate_key.empty()) ++candidates_[lru_.front().candidate_key];
    try {
      index_.emplace(lru_.front().key, lru_.begin());
    } catch (...) {
      auto candidate = candidates_.find(lru_.front().candidate_key);
      if (candidate != candidates_.end() && --candidate->second == 0) candidates_.erase(candidate);
      throw;
    }
  } catch (...) {
    auto charged = charges_.find(lru_.front().value.get());
    if (charged != charges_.end() && charged->second.references == 0) charges_.erase(charged);
    retired.splice(retired.end(), lru_, lru_.begin());
    throw;
  }
  auto& charged = charges_.at(lru_.front().value.get());
  stats_.retained_bytes += overhead + (charged.references++ == 0 ? charged.bytes : 0);
  ++stats_.entries;
}
void metadata_cache::erase_scope(uint64_t scope)
{
  entries retired;
  std::lock_guard lock(mutex_);
  for (auto it = lru_.begin(); it != lru_.end();) {
    auto current = it++;
    if (current->scope == scope) retire(current, retired);
  }
}
size_t metadata_cache::sweep()
{
  entries retired;
  std::lock_guard lock(mutex_);
  auto now    = config_.now();
  size_t work = 0;
  while (!lru_.empty() && work < config_.batch && expired(lru_.back(), now)) {
    retire(std::prev(lru_.end()), retired);
    ++stats_.idle_evictions;
    ++work;
  }
  return work;
}
metadata_cache::counters metadata_cache::statistics() const
{
  std::lock_guard lock(mutex_);
  return stats_;
}
}  // namespace sirius::io::cache
