//
// Created by chiro on 23-5-20.
//

#include "include/file_system.h"

namespace aquafs {

class PosixFileSystem : public FileSystem {};

std::shared_ptr<FileSystem> FileSystem::Default() { return nullptr; }

};  // namespace aquafs