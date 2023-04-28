#include <memory>

#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {
enum class RaidMode { RAID0, RAID1, RAID5, RAID6, RAID10 };

class RaidZonedBlockDevice : ZonedBlockDeviceBackend {
 private:
  RaidMode mode;
  // TODO: multi-devices RAID
  std::shared_ptr<ZonedBlockDeviceBackend> device;

  unsigned int max_active_zones_{};
  unsigned int max_open_zones_{};

  const IOStatus unsupported = IOStatus::NotSupported("Raid unsupported");

  void syncMetaData() {
    block_sz_ = device->block_sz_;
    zone_sz_ = device->zone_sz_;
    nr_zones_ = device->nr_zones_;
  }

 public:
  RaidZonedBlockDevice(RaidMode mode,
                       const std::shared_ptr<ZonedBlockDeviceBackend> &device)
      : mode(mode), device(device) {
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