#include "ray/util/filesystem.h"

#include "gtest/gtest.h"

namespace ray {

TEST(FileSystemTest, PathParseTest) {
#ifdef _WIN32
  ASSERT_EQ(GetRootPath("C:"), "C:");
  ASSERT_EQ(GetRootPath("C:\\"), "C:\\");
  ASSERT_EQ(GetRootPath("C:\\foo"), "C:\\");
  ASSERT_EQ(GetRootPath("./"), "");
  ASSERT_EQ(GetRootPath("foo"), "");
  ASSERT_EQ(GetRootPath("\\"), "\\");
  ASSERT_EQ(GetRootPath("\\/"), "\\/");
  ASSERT_EQ(GetRootPath("//"), "//");
  ASSERT_EQ(GetRootPath("\\\\a\\b\\c"), "\\\\a\\b\\");
  ASSERT_EQ(GetRootPath("\\\\a\\b\\"), "\\\\a\\b\\");
  ASSERT_EQ(GetRootPath("\\\\a\\b"), "\\\\a\\b");
  ASSERT_EQ(GetRootPath("\\\\a\\"), "\\\\a\\");
#endif

  ASSERT_EQ(TrimDirSep("/"), "/");
  ASSERT_EQ(TrimDirSep("///"), "///");
  ASSERT_EQ(TrimDirSep("/foo/"), "/foo");
  ASSERT_EQ(TrimDirSep("///foo/"), "///foo");
  ASSERT_EQ(TrimDirSep("foo/"), "foo");
#ifdef _WIN32
  ASSERT_EQ(TrimDirSep("C:\\/"), "C:\\/");
  ASSERT_EQ(TrimDirSep("C:\\foo/"), "C:\\foo");
#endif

  ASSERT_EQ(GetParentPath("foo/bar"), "foo");
  ASSERT_EQ(GetParentPath("foo/"), "foo");
  ASSERT_EQ(GetParentPath("foo"), "");
  ASSERT_EQ(GetParentPath("./"), ".");
  ASSERT_EQ(GetParentPath("."), "");
  ASSERT_EQ(GetParentPath("/"), "/");
  ASSERT_EQ(GetParentPath("///"), "///");
  ASSERT_EQ(GetParentPath("///foo"), "///");
  ASSERT_EQ(GetParentPath("///foo/"), "///foo");
#ifdef _WIN32
  ASSERT_EQ(GetParentPath("C:"), "C:");
  ASSERT_EQ(GetParentPath("C::"), "C:");  // just to match Python behavior
  ASSERT_EQ(GetParentPath("C:\\/"), "C:\\/");
  ASSERT_EQ(GetParentPath("C:\\foo/bar"), "C:\\foo");
#endif

  ASSERT_EQ(GetFileName("."), ".");
  ASSERT_EQ(GetFileName(".."), "..");
  ASSERT_EQ(GetFileName("foo/bar"), "bar");
  ASSERT_EQ(GetFileName("///bar"), "bar");
  ASSERT_EQ(GetFileName("///bar/"), "");
#ifdef _WIN32
  ASSERT_EQ(GetFileName("C:"), "");
  ASSERT_EQ(GetFileName("C::"), ":");  // just to match Python behavior
  ASSERT_EQ(GetFileName("CC::"), "CC::");
  ASSERT_EQ(GetFileName("C:\\"), "");
#endif
}

TEST(FileSystemTest, FileIOTest) {
  const std::string f = JoinPaths(GetUserTempDir(), "filesystem_test.tmp");
  unlink(f.c_str());
  for (int n = 0; n < 2; ++n) {
    // Do this more than once to ensure idempotency
    std::error_code ec;
    ASSERT_EQ(ReadAllFile(f.c_str(), &ec), "");
    ASSERT_NE(ec, std::error_code());
  }
  std::string data = "abc";
  for (int n = 0; n < 2; ++n) {
    // Do this more than once to ensure idempotency
    std::error_code ec;
    ASSERT_EQ(WriteAllFile(f.c_str(), data, &ec), data.size());
    ASSERT_EQ(ec, std::error_code());
  }
  {
    std::error_code ec;
    ASSERT_EQ(ReadAllFile(f.c_str(), &ec), data);
  }
  unlink(f.c_str());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
