#include <memory>

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
  if (str == "RAID0") {
    return RaidMode::RAID0;
  } else if (str == "RAID1") {
    return RaidMode::RAID1;
  } else if (str == "RAID5") {
    return RaidMode::RAID5;
  } else if (str == "RAID6") {
    return RaidMode::RAID6;
  } else if (str == "RAID10") {
    return RaidMode::RAID10;
  }
  return RaidMode::RAID0;
}

class RaidZonedBlockDevice : public ZonedBlockDeviceBackend {
 private:
  RaidMode mode_;
  // TODO: multi-devices RAID
  std::unique_ptr<ZonedBlockDeviceBackend> device_;

  unsigned int max_active_zones_{};
  unsigned int max_open_zones_{};

  const IOStatus unsupported = IOStatus::NotSupported("Raid unsupported");

  void syncMetaData() {
    block_sz_ = device_->block_sz_;
    zone_sz_ = device_->zone_sz_;
    nr_zones_ = device_->nr_zones_;
  }

 public:
  explicit RaidZonedBlockDevice(RaidMode mode,
                                std::unique_ptr<ZonedBlockDeviceBackend> device)
      : mode_(mode), device_(std::move(device)) {
    assert(device);
    assert(mode == RaidMode::RAID1);
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