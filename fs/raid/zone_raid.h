#include <map>
#include <memory>
#include <numeric>
#include <unordered_map>

#include "../zbd_aquafs.h"
#include "rocksdb/io_status.h"
#include "rocksdb/rocksdb_namespace.h"

namespace AQUAFS_NAMESPACE {
using namespace ROCKSDB_NAMESPACE;

class RaidConsoleLogger;

enum class RaidMode : uint32_t {
  // AquaFS: No RAID, just use the first backend device
  RAID_NONE = 0,
  RAID0,
  RAID1,
  RAID5,
  RAID6,
  RAID10,
  // AquaFS: Concat-RAID
  RAID_C,
  // AquaFS: Auto-RAID
  RAID_A
};

__attribute__((__unused__)) static const char *raid_mode_str(RaidMode mode) {
  switch (mode) {
    case RaidMode::RAID0:
      return "0";
    case RaidMode::RAID1:
      return "1";
    case RaidMode::RAID5:
      return "5";
    case RaidMode::RAID6:
      return "6";
    case RaidMode::RAID10:
      return "10";
    case RaidMode::RAID_A:
      return "a";
    case RaidMode::RAID_C:
      return "c";
    case RaidMode::RAID_NONE:
      return "n";
    default:
      return "UNKNOWN";
  }
}
__attribute__((__unused__)) static RaidMode raid_mode_from_str(
    const std::string &str) {
  if (str == "0") {
    return RaidMode::RAID0;
  } else if (str == "1") {
    return RaidMode::RAID1;
  } else if (str == "5") {
    return RaidMode::RAID5;
  } else if (str == "6") {
    return RaidMode::RAID6;
  } else if (str == "10") {
    return RaidMode::RAID10;
  } else if (str == "A" || str == "a" || str == "-a" || str == "-A") {
    return RaidMode::RAID_A;
  } else if (str == "C" || str == "c" || str == "-c" || str == "-C") {
    return RaidMode::RAID_C;
  }
  return RaidMode::RAID_A;
}

using idx_t = unsigned int;

class RaidMapItem {
 public:
  // device index
  idx_t device_idx{};
  // zone index on this device
  idx_t zone_idx{};
  // when invalid, ignore this <device_idx, zone_idx> in the early record
  uint16_t invalid{};

  Status DecodeFrom(Slice *input);
};

class RaidModeItem {
 public:
  RaidMode mode = RaidMode::RAID_NONE;
  // extra option for raid mode, for example: n extra zones for raid5
  uint32_t option{};

  Status DecodeFrom(Slice *input);
};

class AbstractRaidZonedBlockDevice : public ZonedBlockDeviceBackend {
 public:
  explicit AbstractRaidZonedBlockDevice(
      const std::shared_ptr<Logger> &logger, RaidMode main_mode,
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &devices);
  IOStatus Open(bool readonly, bool exclusive, unsigned int *max_active_zones,
                unsigned int *max_open_zones) override;

  using raid_zone_t = struct zbd_zone;

  [[nodiscard]] ZonedBlockDeviceBackend *def_dev() const {
    return devices_.begin()->get();
  }

 protected:
  std::shared_ptr<Logger> logger_{};
  RaidMode main_mode_{};
  std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices_{};
  // how many zones in total of all devices
  uint32_t total_nr_devices_zones_{};
  const IOStatus unsupported = IOStatus::NotSupported("Raid unsupported");

  virtual void syncBackendInfo() = 0;
};
};  // namespace AQUAFS_NAMESPACE