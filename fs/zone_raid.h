#include <map>
#include <memory>
#include <numeric>
#include <unordered_map>

#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {
enum class RaidMode {
  RAID0,
  RAID1,
  RAID5,
  RAID6,
  RAID10,
  // AquaFS Auto-RAID
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
  }
  return RaidMode::RAID_A;
}

using idx_t = unsigned int;

class RaidMapItem {
 public:
  // device index
  idx_t device_idx;
  // zone index on this device
  idx_t zone_idx;
  // when invalid, ignore this <device_idx, zone_idx> in the early record
  uint8_t invalid;
};

class RaidZonedBlockDevice : public ZonedBlockDeviceBackend {
 private:
  std::shared_ptr<Logger> logger_;
  RaidMode main_mode_;
  std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices_;
  // use `map` or `unordered_map` to store raid mappings
  template <typename K, typename V>
  using map_use = std::unordered_map<K, V>;
  // map: raid zone idx -> device idx, device zone idx
  map_use<idx_t, RaidMapItem> raid_map_;

  const IOStatus unsupported = IOStatus::NotSupported("Raid unsupported");

  void syncBackendInfo();

  ZonedBlockDeviceBackend *device_default() { return devices_.begin()->get(); }

 public:
  explicit RaidZonedBlockDevice(
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices,
      RaidMode mode, std::shared_ptr<Logger> logger);
  explicit RaidZonedBlockDevice(
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices)
      : RaidZonedBlockDevice(std::move(devices), RaidMode::RAID_A, nullptr) {}

  // void load_layout();

  IOStatus Open(bool readonly, bool exclusive, unsigned int *max_active_zones,
                unsigned int *max_open_zones) override;
  std::unique_ptr<ZoneList> ListZones() override;
  IOStatus Reset(uint64_t start, bool *offline,
                 uint64_t *max_capacity) override;
  IOStatus Finish(uint64_t start) override;
  IOStatus Close(uint64_t start) override;
  int Read(char *buf, int size, uint64_t pos, bool direct) override;
  int Write(char *data, uint32_t size, uint64_t pos) override;
  int InvalidateCache(uint64_t pos, uint64_t size) override;
  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                           idx_t idx) override;
  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  std::string GetFilename() override;
  ~RaidZonedBlockDevice() override = default;
};
};  // namespace ROCKSDB_NAMESPACE