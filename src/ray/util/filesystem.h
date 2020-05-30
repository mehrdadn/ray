#pragma once

#include <string>
#include <utility>

// Filesystem and path manipulation APIs.
// (NTFS stream & attribute paths are not supported.)

namespace ray {

/// \return The portable directory separator (slash on all OSes).
static inline char GetAltDirSep() { return '/'; }

/// \return The platform directory separator (backslash on Windows, slash on other OSes).
static inline char GetDirSep() {
  char result;
#ifdef _WIN32
  result = '\\';
#else
  result = '/';
#endif
  return result;
}

/// \return The platform PATH separator (semicolon on Windows, colon on other OSes).
static inline char GetPathSep() {
  char result;
#ifdef _WIN32
  result = ';';
#else
  result = ':';
#endif
  return result;
}

/// Returns the executable binary suffix for the platform, if any.
std::string GetExeSuffix();

/// Equivalent to Python's os.path.basename() for file system paths.
std::string GetFileName(const std::string &path);

/// Equivalent to Python's os.path.dirname() for file system paths.
std::string GetParentPath(const std::string &path);

/// \return The prefix of this path that constitutes the root of the path.
///         On Windows, this can be multiple characters (e.g. C:\ for C:\Windows).
std::string GetRootPath(const std::string &path);

size_t GetRootPathLength(const std::string &path);

/// \return A non-volatile temporary directory in which Ray can stores its files.
std::string GetRayTempDir();

/// \return The non-volatile temporary directory for the current user (often /tmp).
std::string GetUserTempDir();

/// \return Whether or not the given character is a directory separator on this platform.
static inline bool IsDirSep(char ch) {
  bool result = ch == GetDirSep();
#ifdef _WIN32
  result |= ch == GetAltDirSep();
#endif
  return result;
}

/// \return Whether or not the given character is a PATH separator on this platform.
static inline bool IsPathSep(char ch) { return ch == GetPathSep(); }

/// \return The result of joining multiple path components.
template <class... Paths>
std::string JoinPaths(std::string base, Paths... components) {
  std::string to_append[] = {components...};
  for (size_t i = 0; i < sizeof(to_append) / sizeof(*to_append); ++i) {
    const std::string &s = to_append[i];
    if (!base.empty() && !IsDirSep(base.back()) && !s.empty() && !IsDirSep(s[0])) {
      base += GetDirSep();
    }
    base += s;
  }
  return base;
}

/// Reads binary contents from a file. (There is no text translation.)
/// \param ec Optional variable to receive the error code, or NULL to abort on error.
std::string ReadAllFile(const char *path, std::error_code *ec = NULL);

/// \return The equivalent path with all directory separators removed from the end.
std::string TrimDirSep(const std::string &path);

/// Writes binary contents to a file. (There is no text translation.)
/// \param ec Optional variable to receive the error code, or NULL to abort on error.
/// \return The actual number of bytes written. Matches the data size, unless there is an
/// error.
size_t WriteAllFile(const char *path, const std::string &data,
                    std::error_code *ec = NULL);

}  // namespace ray
