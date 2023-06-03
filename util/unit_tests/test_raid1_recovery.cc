//
// Created by chiro on 23-6-3.
//

#include <filesystem>
#include <fstream>
#include <string>

#include "../tools/tools.h"
#include "fs/fs_aquafs.h"

using namespace aquafs;

size_t get_file_hash(std::filesystem::path file) {
  std::ifstream infile(file, std::ios::binary);
  std::hash<std::string> hash_fn;
  constexpr size_t block_size = 1 << 20;  // 1MB
  char buffer[block_size];
  size_t file_hash = 0;
  while (infile.read(buffer, block_size)) {
    file_hash ^= hash_fn(std::string(buffer, buffer + infile.gcount()));
  }
  if (infile.gcount() > 0) {
    file_hash ^= hash_fn(std::string(buffer, buffer + infile.gcount()));
  }
  return file_hash;
}

void emit_device_zone_offline(const std::string& devID) {
  // mount
  std::unique_ptr<AquaFS> aquaFS;
  FileSystem* fs = nullptr;
  auto status = NewAquaFS(&fs, ZbdBackendType::kRaid, devID);
  assert(status.ok());
  aquaFS.reset(dynamic_cast<AquaFS*>(fs));
  aquaFS->selectZoneToOffline();
}

int main() {
  prepare_test_env();
  const char* fs_uri =
      "--raids=raida:dev:nullb0,dev:nullb1,dev:nullb2,dev:nullb3";
  aquafs_tools_call({"mkfs", fs_uri, "--aux_path=/tmp/aux_path", "--force"});
  auto data_source_dir = std::filesystem::temp_directory_path() / "aquafs_test";
  std::filesystem::create_directories(data_source_dir);
  auto file = data_source_dir / "test_file";
  // std::ofstream ofs(file);
  // ofs << "test content\n";
  // ofs.close();
  auto mib = 2l;
  // std::filesystem::resize_file(file, mib * 1024 * 1024);
  system((std::string("dd if=/dev/random of=") + file.string() +
          " bs=1M count=" + std::to_string(mib))
             .c_str());
  // calculate checksum
  size_t file_hash = get_file_hash(file);
  printf("file hash: %zx\n", file_hash);
  // call restore
  aquafs_tools_call({"restore", fs_uri, "--path=" + data_source_dir.string()});

  // emit zone offline
  std::string dev = std::string(fs_uri + strlen("--raids"));
  emit_device_zone_offline(dev);

  auto dump_dir = std::filesystem::temp_directory_path() / "aquafs_dump";
  std::filesystem::create_directories(dump_dir);
  aquafs_tools_call({"backup", fs_uri, "--path=" + dump_dir.string()});
  // calculate checksum again
  size_t file_hash2 = get_file_hash(dump_dir / "test_file");
  printf("file hash2: %zx\n", file_hash2);
  assert(file_hash == file_hash2);
  return 0;
}