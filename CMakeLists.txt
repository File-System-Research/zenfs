cmake_minimum_required(VERSION 3.4)

if (NOT DEFINED AQUAFS_STANDALONE)
    set(AQUAFS_STANDALONE 1)
endif ()

set(AQUAFS_VERSION v0.0.1-alpha)

if (AQUAFS_STANDALONE)
    project(aquafs)

    file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fs/version.h "#pragma once\n#define AQUAFS_VERSION \"${AQUAFS_VERSION}\"\n")

    set(aquafs_SOURCES "fs/fs_aquafs.cc" "fs/zbd_aquafs.cc" "fs/io_aquafs.cc" "fs/zonefs_aquafs.cc"
            "fs/zbdlib_aquafs.cc" "fs/zone_raid.cc")
    set(aquafs_HEADERS "fs/fs_aquafs.h" "fs/zbd_aquafs.h" "fs/io_aquafs.h" "fs/version.h" "fs/metrics.h"
            "fs/snapshot.h" "fs/filesystem_utility.h" "fs/zonefs_aquafs.h" "fs/zbdlib_aquafs.h" "fs/zone_raid.h")
    set(aquafs_LIBS "zbd" "gflags" "rocksdb" "uring" "zstd" "lz4" "snappy" "bz2" "z" "numa")

    #  file(GLOB_RECURSE port_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/port/*.cc)
    #  file(GLOB_RECURSE util_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/util/util/*.cc)
    #  file(GLOB_RECURSE test_util_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_util/*.cc)

    if (NOT AQUAFS_SKIP_LIBZBD_CHECK)
        find_package(PkgConfig REQUIRED)
        pkg_search_module(ZBD REQUIRED libzbd>=1.5.0)
        # pkg_search_module(rocksdb REQUIRED)
        find_package(gflags REQUIRED)
        add_definitions(-DGFLAGS=1)
        include_directories(${GFLAGS_INCLUDE_DIR})
        # find_package(NUMA REQUIRED)
        add_definitions(-DNUMA)
        # include_directories(${NUMA_INCLUDE_DIR})
    endif ()

    #  add_library(port STATIC ${port_SOURCES})
    #  add_library(util STATIC ${util_SOURCES})

    include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/util)
    add_library(aaquafs STATIC ${aquafs_SOURCES} ${aquafs_HEADERS})
    target_link_libraries(aaquafs ${aquafs_LIBS})

    add_definitions(-DWITH_TERARKDB=1 -DOS_LINUX=1 -DROCKSDB_PLATFORM_POSIX=1)
    add_executable(aquafs ${CMAKE_CURRENT_SOURCE_DIR}/util/aquafs.cc)
    target_link_libraries(aquafs aaquafs)

    add_executable(db_bench ${CMAKE_CURRENT_SOURCE_DIR}/util/db_bench.cc)
    target_link_libraries(db_bench aaquafs)

    # basic test
    enable_testing()
    add_test(NAME aquafs-mkfs COMMAND sudo $<TARGET_FILE:aquafs> mkfs --zbd=nullb0 --aux_path=/tmp/aux_path --force)
    add_test(NAME aquafs-list COMMAND sudo $<TARGET_FILE:aquafs> list --zbd=nullb0)

    add_test(NAME aquafs-mkfs-raid0 COMMAND sudo $<TARGET_FILE:aquafs> mkfs --raids=raid0:dev:nullb0,dev:nullb1 --aux_path=/tmp/aux_path --force)
    add_test(NAME aquafs-mkfs-raid1 COMMAND sudo $<TARGET_FILE:aquafs> mkfs --raids=raid1:dev:nullb0,dev:nullb1 --aux_path=/tmp/aux_path --force)

    add_test(NAME aquafs_db_bench COMMAND sudo $<TARGET_FILE:db_bench> --fs_uri=aquafs://dev:nullb0 --benchmarks=fillrandom --use_direct_io_for_flush_and_compaction)
else ()
    set(aquafs_SOURCES "fs/fs_aquafs.cc" "fs/zbd_aquafs.cc" "fs/io_aquafs.cc" "fs/zonefs_aquafs.cc"
            "fs/zbdlib_aquafs.cc" "fs/zone_raid.cc" PARENT_SCOPE)
    set(aquafs_HEADERS "fs/fs_aquafs.h" "fs/zbd_aquafs.h" "fs/io_aquafs.h" "fs/version.h" "fs/metrics.h"
            "fs/snapshot.h" "fs/filesystem_utility.h" "fs/zonefs_aquafs.h" "fs/zbdlib_aquafs.h" "fs/zone_raid.h" PARENT_SCOPE)
    set(aquafs_LIBS "zbd" PARENT_SCOPE)
    set(aquafs_CMAKE_EXE_LINKER_FLAGS "-u aquafs_filesystems_reg" PARENT_SCOPE)

    if (NOT AQUAFS_SKIP_LIBZBD_CHECK)
        find_package(PkgConfig REQUIRED)
        pkg_check_modules(ZBD REQUIRED libzbd>=1.5.0)
    endif ()

    #  execute_process(WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    #    RESULT_VARIABLE GENVER_RESULT
    #    COMMAND ./generate-version.sh)
    #
    #  if(GENVER_RESULT AND NOT GENVER_RESULT EQUAL 0)
    #    message(FATAL_ERROR "Generating AquaFS version failed")
    #  endif()

    file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fs/version.h "#pragma once\n#define AQUAFS_VERSION \"${AQUAFS_VERSION}\"\n")
endif ()