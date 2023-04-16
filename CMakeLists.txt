cmake_minimum_required(VERSION 3.4)

set(ZENFS_STANDALONE true)

if(ZENFS_STANDALONE)
  project(zenfs)

  file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fs/version.h "#pragma once\n#define ZENFS_VERSION \"v2.1.0-57-gb04ca0c\"\n")

  set(zenfs_SOURCES "fs/fs_zenfs.cc" "fs/zbd_zenfs.cc" "fs/io_zenfs.cc" "fs/zonefs_zenfs.cc"
    "fs/zbdlib_zenfs.cc")
  set(zenfs_HEADERS "fs/fs_zenfs.h" "fs/zbd_zenfs.h" "fs/io_zenfs.h" "fs/version.h" "fs/metrics.h"
    "fs/snapshot.h" "fs/filesystem_utility.h" "fs/zonefs_zenfs.h" "fs/zbdlib_zenfs.h")
  set(zenfs_LIBS "zbd" "gflags" "rocksdb")

#  file(GLOB_RECURSE port_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/port/*.cc)
#  file(GLOB_RECURSE util_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/util/util/*.cc)
#  file(GLOB_RECURSE test_util_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_util/*.cc)

  if(NOT ZENFS_SKIP_LIBZBD_CHECK)
    find_package(PkgConfig REQUIRED)
    pkg_search_module(ZBD REQUIRED libzbd>=1.5.0)
    pkg_search_module(rocksdb REQUIRED)
    find_package(gflags REQUIRED)
  endif()

#  add_library(port STATIC ${port_SOURCES})
#  add_library(util STATIC ${util_SOURCES})

  include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/util)
  add_library(azenfs STATIC ${zenfs_SOURCES} ${zenfs_HEADERS})
  target_link_libraries(azenfs ${zenfs_LIBS})

  add_definitions(-DWITH_TERARKDB=1 -DOS_LINUX=1 -DROCKSDB_PLATFORM_POSIX=1)
  add_executable(zenfs ${CMAKE_CURRENT_SOURCE_DIR}/util/zenfs.cc)
  target_link_libraries(zenfs azenfs)
else()
  set(zenfs_SOURCES "fs/fs_zenfs.cc" "fs/zbd_zenfs.cc" "fs/io_zenfs.cc" "fs/zonefs_zenfs.cc"
    "fs/zbdlib_zenfs.cc" PARENT_SCOPE)
  set(zenfs_HEADERS "fs/fs_zenfs.h" "fs/zbd_zenfs.h" "fs/io_zenfs.h" "fs/version.h" "fs/metrics.h"
    "fs/snapshot.h" "fs/filesystem_utility.h" "fs/zonefs_zenfs.h" "fs/zbdlib_zenfs.h" PARENT_SCOPE)
  set(zenfs_LIBS "zbd" PARENT_SCOPE)
  set(zenfs_CMAKE_EXE_LINKER_FLAGS "-u zenfs_filesystems_reg" PARENT_SCOPE)

  if(NOT ZENFS_SKIP_LIBZBD_CHECK)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(ZBD REQUIRED libzbd>=1.5.0)
  endif()

  execute_process(WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE GENVER_RESULT
    COMMAND ./generate-version.sh)

  if(GENVER_RESULT AND NOT GENVER_RESULT EQUAL 0)
    message(FATAL_ERROR "Generating ZenFS version failed")
  endif()
endif()