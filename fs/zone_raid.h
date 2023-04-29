#include <memory>
#include <numeric>

#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {
enum class RaidMode { RAID0, RAID1, RAID5, RAID6, RAID10 };

__attribute__((__unused__)) static const char *raid_mode_str(RaidMode mode) {
  switch (mode) {
    case RaidMode::RAID0:
      return "RAID0";
    case RaidMode::RAID1:
      return "RAID1";
    case RaidMode::RAID5:
      return "RAID5";
    case RaidMode::RAID6:
      return "RAID6";
    case RaidMode::RAID10:
      return "RAID10";
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
  }
  return RaidMode::RAID0;
}

class RaidZonedBlockDevice : public ZonedBlockDeviceBackend {
 private:
  RaidMode mode_;
  // TODO: multi-devices RAID
  std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices_;

  unsigned int max_active_zones_{};
  unsigned int max_open_zones_{};

  const IOStatus unsupported = IOStatus::NotSupported("Raid unsupported");

  void syncMetaData() {
    auto total_nr_zones = std::accumulate(
        devices_.begin(), devices_.end(), 0,
        [](int sum, const std::unique_ptr<ZonedBlockDeviceBackend> &dev) {
          return sum + dev->nr_zones_;
        });
    block_sz_ = devices_.begin()->get()->block_sz_;
    zone_sz_ = devices_.begin()->get()->zone_sz_;
    if (mode_ == RaidMode::RAID0) {
      nr_zones_ = total_nr_zones;
    } else if (mode_ == RaidMode::RAID1) {
      nr_zones_ = total_nr_zones >> 1;
    } else {
      nr_zones_ = 0;
    }
  }

 public:
  explicit RaidZonedBlockDevice(
      RaidMode mode,
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices)
      : mode_(mode), devices_(std::move(devices)) {
    assert(!devices_.empty());
    assert(mode_ == RaidMode::RAID1);
    syncMetaData();
  }
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
  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;
  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                     unsigned int idx) override;
  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                      unsigned int idx) override;
  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                    unsigned int idx) override;
  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;
  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                     unsigned int idx) override;
  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                           unsigned int idx) override;
  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;
  std::string GetFilename() override;
  ~RaidZonedBlockDevice() override = default;
};
};  // namespace ROCKSDB_NAMESPACE