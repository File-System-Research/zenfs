// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <gflags/gflags.h>

#include "fs/version.h"
#include "tools/tools.h"

int main(int argc, char **argv) {
  gflags::SetUsageMessage(
      std::string("\nUSAGE:\n") + argv[0] +
      +" <command> [OPTIONS]...\nCommands: mkfs, list, ls-uuid, " +
      +"df, backup, restore, dump, fs-info, link, delete, rename, rmdir");
  if (argc < 2) {
    fprintf(stderr, "You need to specify a command:\n");
    fprintf(stderr,
            "\t./aquafs [list | ls-uuid | df | backup | restore | dump | "
            "fs-info | link | delete | rename | rmdir]\n");
    return 1;
  }

  gflags::SetVersionString(AQUAFS_VERSION);
  std::string subcmd(argv[1]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_zonefs.empty() && FLAGS_zbd.empty() && FLAGS_raids.empty() &&
      subcmd != "ls-uuid") {
    fprintf(stderr,
            "You need to specify a zoned block device using --zbd or --zonefs "
            "or --raids\n");
    return 1;
  }
  if (!FLAGS_zonefs.empty() && !FLAGS_zbd.empty()) {
    fprintf(stderr,
            "You need to specify a zoned block device using either "
            "--zbd or --zonefs - not both\n");
    return 1;
  }
  if (subcmd == "mkfs") {
    return aquafs::aquafs_tool_mkfs();
  } else if (subcmd == "list") {
    return aquafs::aquafs_tool_list();
  } else if (subcmd == "ls-uuid") {
    return aquafs::aquafs_tool_lsuuid();
  } else if (subcmd == "df") {
    return aquafs::aquafs_tool_df();
  } else if (subcmd == "backup") {
    return aquafs::aquafs_tool_backup();
  } else if (subcmd == "restore") {
    return aquafs::aquafs_tool_restore();
  } else if (subcmd == "dump") {
    return aquafs::aquafs_tool_dump();
  } else if (subcmd == "fs-info") {
    return aquafs::aquafs_tool_fsinfo();
  } else if (subcmd == "link") {
    return aquafs::aquafs_tool_link();
  } else if (subcmd == "delete") {
    return aquafs::aquafs_tool_delete_file();
  } else if (subcmd == "rename") {
    return aquafs::aquafs_tool_rename_file();
  } else if (subcmd == "rmdir") {
    return aquafs::aquafs_tool_remove_directory();
  } else {
    fprintf(stderr, "Subcommand not recognized: %s\n", subcmd.c_str());
    return 1;
  }

  return 0;
}
