//
// Created by chiro on 23-6-3.
//

#include <filesystem>
#include <fstream>
#include <string>

#include "../tools/tools.h"

using namespace aquafs;

int main() {
  prepare_test_env();
  auto fs_uri = "--raids=raida:dev:nullb0,dev:nullb1,dev:nullb2,dev:nullb3";
  aquafs_tools_call({"mkfs", fs_uri, "--aux_path=/tmp/aux_path", "--force"});
  auto data_source_dir = std::filesystem::temp_directory_path() / "aquafs_test";
  std::filesystem::create_directories(data_source_dir);
  auto file = data_source_dir / "test_file";
  std::ofstream ofs(file);
  ofs << "\n";
  ofs.close();
  // 128 MiB
  std::filesystem::resize_file(file, 128 * 1024 * 1024);
  // call restore
  aquafs_tools_call({"restore", fs_uri, "--path=" + data_source_dir.string()});
  return 0;
}