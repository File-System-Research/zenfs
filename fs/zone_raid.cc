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
    assert(mode == RaidMode::RAID0);
  }

 private:
  IOStatus Open(bool readonly, bool exclusive, unsigned int *max_active_zones,
                unsigned int *max_open_zones) override {
    auto r =
        device->Open(readonly, exclusive, max_active_zones, max_open_zones);
    max_active_zones_ = *max_active_zones;
    max_open_zones_ = *max_open_zones;
    return r;
  }
  std::unique_ptr<ZoneList> ListZones() override {
    if (mode == RaidMode::RAID0) {
      // only half zones available
      auto zones = device->ListZones();
      if (zones && zones->ZoneCount() > 0) {
        // clone one
        auto nr_zones = zones->ZoneCount() / 2;
        auto original_data = (struct zbd_zone *)zones->GetData();
        auto data = new struct zbd_zone[nr_zones];
        memcpy(data, original_data, sizeof(struct zbd_zone) * nr_zones);
        return std::make_unique<ZoneList>(data, nr_zones);
      }
    }
    return {};
  }
  IOStatus Reset(uint64_t start, bool *offline,
                 uint64_t *max_capacity) override {
    if (mode == RaidMode::RAID0) {
      bool offline_a, offline_b;
      uint64_t max_capacity_a, max_capacity_b;
      auto a = device->Reset(start, &offline_a, &max_capacity_a);
      auto b = device->Reset(start << 1, &offline_b, &max_capacity_b);
      if (!a.ok()) return a;
      if (!b.ok()) return b;
      *offline = offline_a || offline_b;
      assert(max_capacity_a == max_capacity_b);
      *max_capacity = max_capacity_a;
      return IOStatus::OK();
    }
    return unsupported;
  }
  IOStatus Finish(uint64_t start) override {
    if (mode == RaidMode::RAID0) {
      auto a = device->Finish(start);
      auto b = device->Finish(start << 1);
      if (!a.ok()) return a;
      if (!b.ok()) return b;
      return IOStatus::OK();
    }
    return unsupported;
  }
  IOStatus Close(uint64_t start) override {
    if (mode == RaidMode::RAID0) {
      auto a = device->Close(start);
      auto b = device->Close(start << 1);
      if (!a.ok()) return a;
      if (!b.ok()) return b;
      return IOStatus::OK();
    }
    return unsupported;
  }
  int Read(char *buf, int size, uint64_t pos, bool direct) override {
    if (mode == RaidMode::RAID0) {
      // TODO: data check
      return device->Read(buf, size, pos, direct);
    }
    return 0;
  }
  int Write(char *data, uint32_t size, uint64_t pos) override {
    if (mode == RaidMode::RAID0) {
      auto a = device->Write(data, size, pos);
      auto b = device->Write(data, size, pos << 1);
      return a;
    }
    return 0;
  }
  int InvalidateCache(uint64_t pos, uint64_t size) override {
    if (mode == RaidMode::RAID0) {
      auto a = device->InvalidateCache(pos, size);
      auto b = device->InvalidateCache(pos << 1, size);
      return a;
    }
    return 0;
  }
  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, unsigned int idx) override {
    if (mode == RaidMode::RAID0) {
      return device->ZoneIsSwr(zones, idx);
    }
    return false;
  }
  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                     unsigned int idx) override {
    if (mode == RaidMode::RAID0) {
      return device->ZoneIsOffline(zones, idx);
    }
    return false;
  }
  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                      unsigned int idx) override {
    if (mode == RaidMode::RAID0) {
      return device->ZoneIsWritable(zones, idx);
    }
    return false;
  }
  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                    unsigned int idx) override {
    if (mode == RaidMode::RAID0) {
      return device->ZoneIsActive(zones, idx);
    }
    return false;
  }
  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones, unsigned int idx) override {
    if (mode == RaidMode::RAID0) {
      return device->ZoneIsOpen(zones, idx);
    }
    return false;
  }
  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                     unsigned int idx) override {
    if (mode == RaidMode::RAID0) {
      return device->ZoneStart(zones, idx);
    }
    return 0;
  }
  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                           unsigned int idx) override {
    if (mode == RaidMode::RAID0) {
      return device->ZoneMaxCapacity(zones, idx);
    }
    return 0;
  }
  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, unsigned int idx) override {
    if (mode == RaidMode::RAID0) {
      return device->ZoneWp(zones, idx);
    }
    return 0;
  }
  std::string GetFilename() override {
    if (mode == RaidMode::RAID0) {
      return device->GetFilename();
    }
    return {};
  }
  ~RaidZonedBlockDevice() override {}
};
};  // namespace ROCKSDB_NAMESPACE