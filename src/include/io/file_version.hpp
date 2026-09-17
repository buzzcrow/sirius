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

#include <cstddef>
#include <cstdint>

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

}  // namespace io
}  // namespace sirius
