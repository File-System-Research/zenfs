//
// Created by chiro on 23-5-1.
//

#ifndef ROCKSDB_AQUAFS_NAMESPACE_H
#define ROCKSDB_AQUAFS_NAMESPACE_H

#ifndef AQUAFS_NAMESPACE
#define AQUAFS_NAMESPACE aquafs
#endif

#ifdef AQUAFS_STANDALONE
#include "standalone/include/standalone.h"
#else
#include <rocksdb/rocksdb_namespace.h>
#include <rocksdb/file_system.h>
namespace aquafs {
using namespace ROCKSDB_NAMESPACE;
};
#endif

#endif  // ROCKSDB_AQUAFS_NAMESPACE_H
