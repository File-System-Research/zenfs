//
// Created by chiro on 23-6-3.
//

#ifndef ROCKSDB_TOOLS_H
#define ROCKSDB_TOOLS_H

#include <gflags/gflags.h>

#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

DECLARE_string(zbd);
DECLARE_string(zonefs);
DECLARE_string(raids);
DECLARE_string(aux_path);
DECLARE_bool(force);
DECLARE_string(path);
DECLARE_int32(finish_threshold);
DECLARE_string(restore_path);
DECLARE_string(backup_path);
DECLARE_string(src_file);
DECLARE_string(dest_file);
DECLARE_bool(enable_gc);

namespace aquafs {

int aquafs_tool_mkfs();
int aquafs_tool_list();
int aquafs_tool_df();
int aquafs_tool_lsuuid();

rocksdb::IOStatus aquafs_tool_copy_file(rocksdb::FileSystem *f_fs,
                                        const std::string &f,
                                        rocksdb::FileSystem *t_fs,
                                        const std::string &t);
rocksdb::IOStatus aquafs_tool_copy_dir(rocksdb::FileSystem *f_fs,
                                       const std::string &f_dir,
                                       rocksdb::FileSystem *t_fs,
                                       const std::string &t_dir);

int aquafs_tool_backup();
int aquafs_tool_link();
int aquafs_tool_delete_file();
int aquafs_tool_rename_file();
int aquafs_tool_remove_directory();
int aquafs_tool_restore();
int aquafs_tool_dump();
int aquafs_tool_fsinfo();

}  // namespace aquafs

#endif  // ROCKSDB_TOOLS_H
