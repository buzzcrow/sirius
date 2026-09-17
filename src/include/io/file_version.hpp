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

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include <sys/stat.h>

namespace sirius {
namespace io {

/// Version evidence captured for a local file. It deliberately contains no
/// descriptor: a bind must not keep a file open for its entire query lifetime.
struct local_file_version {
  local_file_version() = default;
  constexpr local_file_version(bool available, std::size_t size, std::int64_t mtime_ns)
    : available(available), size(size), mtime_ns(mtime_ns)
  {
  }

  bool available{false};
  std::size_t size{0};
  std::int64_t mtime_ns{0};

  bool operator==(local_file_version const& other) const noexcept
  {
    return available == other.available && size == other.size && mtime_ns == other.mtime_ns;
  }
  bool operator!=(local_file_version const& other) const noexcept { return !(*this == other); }
};

/// Extract the size and nanosecond mtime from an already-open local file.
/// The descriptor anchors the evidence to the exact inode that will be read.
inline local_file_version local_file_version_from_fd(int fd) noexcept
{
  struct stat st {};
  if (::fstat(fd, &st) != 0 || st.st_size < 0) { return {}; }
  return {true,
          static_cast<std::size_t>(st.st_size),
          static_cast<std::int64_t>(st.st_mtim.tv_sec) * 1000000000LL +
            static_cast<std::int64_t>(st.st_mtim.tv_nsec)};
}

/// Cache identity for a local file. Versioned files can share an entry; an
/// unavailable version is deliberately isolated to one open, never reused by
/// a later query that happens to use the same path.
inline std::string local_file_cache_id(std::string const& path, local_file_version version)
{
  if (version.available) {
    return path + "\x1flocal:" + std::to_string(version.size) + ":" +
           std::to_string(version.mtime_ns);
  }
  static std::atomic<std::uint64_t> next_id{0};
  return path + "\x1funversioned:" + std::to_string(next_id.fetch_add(1, std::memory_order_relaxed));
}

/// Cache identity for an object-store response. ETags are opaque equality
/// tokens; without one, isolate the entry to this open rather than sharing a
/// path-only entry with a later generation.
inline std::string etag_file_cache_id(std::string const& path, std::string const& etag)
{
  if (!etag.empty()) { return path + "\x1f" "etag:" + etag; }
  static std::atomic<std::uint64_t> next_id{0};
  return path + "\x1funversioned:" + std::to_string(next_id.fetch_add(1, std::memory_order_relaxed));
}

}  // namespace io
}  // namespace sirius
