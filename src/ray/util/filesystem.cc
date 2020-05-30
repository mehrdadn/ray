#include "ray/util/filesystem.h"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#else
#include <unistd.h>
#endif

#include <stdlib.h>

#include "ray/util/logging.h"

#ifdef _WIN32
#include <Windows.h>
#endif

namespace ray {

std::string GetExeSuffix() {
  std::string result;
#ifdef _WIN32
  result = ".exe";
#endif
  return result;
}

std::string GetFileName(const std::string &path) {
  size_t i = GetRootPathLength(path), j = path.size();
  while (j > i && !IsDirSep(path[j - 1])) {
    --j;
  }
  return path.substr(j);
}

std::string GetParentPath(const std::string &path) {
  size_t i = GetRootPathLength(path), j = path.size();
  while (j > i && !IsDirSep(path[j - 1])) {
    --j;
  }
  while (j > i && IsDirSep(path[j - 1])) {
    --j;
  }
  return path.substr(0, j);
}

std::string GetRootPath(const std::string &path) {
  return path.substr(0, GetRootPathLength(path));
}

size_t GetRootPathLength(const std::string &path) {
  size_t i = 0;
#ifdef _WIN32
  if (i + 2 < path.size() && IsDirSep(path[i]) && IsDirSep(path[i + 1]) &&
      !IsDirSep(path[i + 2])) {
    // UNC paths begin with two separators (but not 1 or 3)
    i += 2;
    for (int k = 0; k < 2; ++k) {
      while (i < path.size() && !IsDirSep(path[i])) {
        ++i;
      }
      while (i < path.size() && IsDirSep(path[i])) {
        ++i;
      }
    }
  } else if (i + 1 < path.size() && path[i + 1] == ':') {
    i += 2;
  }
#endif
  while (i < path.size() && IsDirSep(path[i])) {
    ++i;
  }
  return i;
}

std::string GetRayTempDir() { return JoinPaths(GetUserTempDir(), "ray"); }

std::string GetUserTempDir() {
  std::string result;
#if defined(__APPLE__) || defined(__linux__)
  // Prefer the hard-coded path for now, for compatibility.
  result = "/tmp";
#elif defined(_WIN32)
  result.resize(1 << 8);
  DWORD n = GetTempPath(static_cast<DWORD>(result.size()), &*result.begin());
  if (n > result.size()) {
    result.resize(n);
    n = GetTempPath(static_cast<DWORD>(result.size()), &*result.begin());
  }
  result.resize(0 < n && n <= result.size() ? static_cast<size_t>(n) : 0);
#else  // not Linux, Darwin, or Windows
  const char *candidates[] = {"TMPDIR", "TMP", "TEMP", "TEMPDIR"};
  const char *found = NULL;
  for (char const *candidate : candidates) {
    found = getenv(candidate);
    if (found) {
      break;
    }
  }
  result = found ? found : "/tmp";
#endif
  // Strip trailing separators
  while (!result.empty() && IsDirSep(result.back())) {
    result.pop_back();
  }
  RAY_CHECK(!result.empty());
  return result;
}

std::string ReadAllFile(const char *path, std::error_code *ec) {
  std::string data;
  bool success = false;
  int fd, options = O_RDONLY;
#ifdef _WIN32
  fd = open(path, options | O_NOINHERIT | O_SEQUENTIAL | O_BINARY, S_IREAD | S_IWRITE);
#else
  fd = open(path, options | O_CLOEXEC | O_NOCTTY, S_IRWXU | S_IRWXG | S_IRWXO);
#endif
  if (fd != -1) {
    size_t buffer_size = 1 << 12;
    if (char *buf = static_cast<char *>(malloc(buffer_size))) {
      size_t i = 0;
      for (;;) {
        if (i >= buffer_size) {
          buffer_size = i + buffer_size / 2;
          char *buf2 = static_cast<char *>(realloc(buf, buffer_size));
          if (!buf2) {
            errno = ENOMEM;
            break;
          }
          buf = buf2;
        }
        ptrdiff_t n = read(fd, &buf[i], buffer_size - i);
        if (n == 0) {
          try {
            data.assign(buf, i);
            success = true;
          } catch (std::bad_alloc &) {
            errno = ENOMEM;
          }
        }
        if (n <= 0) {
          break;
        }
        i += static_cast<size_t>(n);
      }
      free(buf);
    } else {
      errno = ENOMEM;
    }
  }
  if (fd != -1) {
    close(fd);
  }
  if (!ec) {
    RAY_CHECK(success) << "error " << errno << " reading file: " << path;
  } else {
    *ec = std::error_code(success ? 0 : errno, std::system_category());
  }
  return data;
}

std::string TrimDirSep(const std::string &path) {
  size_t i = GetRootPathLength(path), j = path.size();
  while (j > i && IsDirSep(path[j - 1])) {
    --j;
  }
  return path.substr(0, j);
}

size_t WriteAllFile(const char *path, const std::string &data, std::error_code *ec) {
  bool success = false;
  int fd, options = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
  fd = open(path, options | O_NOINHERIT | O_SEQUENTIAL | O_BINARY, S_IREAD | S_IWRITE);
#else
  fd = open(path, options | O_CLOEXEC | O_NOCTTY, S_IRWXU | S_IRWXG | S_IRWXO);
#endif
  size_t i = 0;
  if (fd != -1) {
    size_t block_size = 1 << 18;
    for (;;) {
      size_t left = data.size() - i;
      ptrdiff_t n = left ? write(fd, &data[i], left < block_size ? left : block_size) : 0;
      if (n == 0) {
        if (i < data.size()) {
          errno = ENOSPC;
        } else {
          success = true;
        }
      }
      if (n <= 0) {
        break;
      }
      i += static_cast<size_t>(n);
    }
  }
  if (fd != -1) {
    close(fd);
  }
  if (!ec) {
    RAY_CHECK(success) << "error " << errno << " reading file: " << path;
  } else {
    *ec = std::error_code(success ? 0 : errno, std::system_category());
  }
  return i;
}

}  // namespace ray
