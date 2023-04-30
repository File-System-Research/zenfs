//
// Created by chiro on 23-4-28.
//

#include "zone_raid.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief Construct a new Raid Zoned Block Device object
 * @param mode main mode. RAID_A for auto-raid
 * @param devices
 */
RaidZonedBlockDevice::RaidZonedBlockDevice(
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices,
    RaidMode mode)
    : main_mode_(mode), devices_(std::move(devices)) {
  assert(!devices_.empty());
  syncMetaData();
}

IOStatus RaidZonedBlockDevice::Open(bool readonly, bool exclusive,
                                    unsigned int *max_active_zones,
                                    unsigned int *max_open_zones) {
  auto r = device_default()->Open(readonly, exclusive, max_active_zones,
                                  max_open_zones);
  syncMetaData();
  return r;
}

void RaidZonedBlockDevice::syncMetaData() {
  auto total_nr_zones = std::accumulate(
      devices_.begin(), devices_.end(), 0,
      [](int sum, const std::unique_ptr<ZonedBlockDeviceBackend> &dev) {
        return sum + dev->nr_zones_;
      });
  block_sz_ = device_default()->block_sz_;
  zone_sz_ = device_default()->zone_sz_;
  if (main_mode_ == RaidMode::RAID0) {
    nr_zones_ = total_nr_zones;
  } else if (main_mode_ == RaidMode::RAID1) {
    nr_zones_ = device_default()->nr_zones_;
  } else {
    nr_zones_ = 0;
  }
}

std::unique_ptr<ZoneList> RaidZonedBlockDevice::ListZones() {
  if (main_mode_ == RaidMode::RAID0) {
    return device_default()->ListZones();
  } else if (main_mode_ == RaidMode::RAID1) {
    // only half zones available
    auto zones = device_default()->ListZones();
    if (zones && zones->ZoneCount() > 0) {
      // clone one
      auto nr_zones = zones->ZoneCount() >> 1;
      auto original_data = (struct zbd_zone *)zones->GetData();
      auto data = new struct zbd_zone[nr_zones];
      memcpy(data, original_data, sizeof(struct zbd_zone) * nr_zones);
      return std::make_unique<ZoneList>(data, nr_zones);
    }
  }
  return {};
}

IOStatus RaidZonedBlockDevice::Reset(uint64_t start, bool *offline,
                                     uint64_t *max_capacity) {
  if (main_mode_ == RaidMode::RAID0) {
    return device_default()->Reset(start, offline, max_capacity);
  } else if (main_mode_ == RaidMode::RAID1) {
    bool offline_a, offline_b;
    uint64_t max_capacity_a, max_capacity_b;
    auto a = device_default()->Reset(start, &offline_a, &max_capacity_a);
    auto b = device_default()->Reset(start << 1, &offline_b, &max_capacity_b);
    if (!a.ok()) return a;
    if (!b.ok()) return b;
    *offline = offline_a || offline_b;
    assert(max_capacity_a == max_capacity_b);
    *max_capacity = max_capacity_a;
    return IOStatus::OK();
  }
  return unsupported;
}

IOStatus RaidZonedBlockDevice::Finish(uint64_t start) {
  if (main_mode_ == RaidMode::RAID0) {
    return device_default()->Finish(start);
  } else if (main_mode_ == RaidMode::RAID1) {
    auto a = device_default()->Finish(start);
    auto b = device_default()->Finish(start << 1);
    if (!a.ok()) return a;
    if (!b.ok()) return b;
    return IOStatus::OK();
  }
  return unsupported;
}

IOStatus RaidZonedBlockDevice::Close(uint64_t start) {
  if (main_mode_ == RaidMode::RAID0) {
    return device_default()->Close(start);
  } else if (main_mode_ == RaidMode::RAID1) {
    auto a = device_default()->Close(start);
    auto b = device_default()->Close(start << 1);
    if (!a.ok()) return a;
    if (!b.ok()) return b;
    return IOStatus::OK();
  }
  return unsupported;
}

int RaidZonedBlockDevice::Read(char *buf, int size, uint64_t pos, bool direct) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    // TODO: data read and check
    return device_default()->Read(buf, size, pos, direct);
  }
  return 0;
}

int RaidZonedBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
  if (main_mode_ == RaidMode::RAID0) {
    return device_default()->Write(data, size, pos);
  } else if (main_mode_ == RaidMode::RAID1) {
    auto a = device_default()->Write(data, size, pos);
    auto b = device_default()->Write(data, size, pos << 1);
    return a;
  }
  return 0;
}

int RaidZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  if (main_mode_ == RaidMode::RAID0) {
    return device_default()->InvalidateCache(pos, size);
  } else if (main_mode_ == RaidMode::RAID1) {
    auto a = device_default()->InvalidateCache(pos, size);
    auto b = device_default()->InvalidateCache(pos << 1, size);
    return a;
  }
  return 0;
}

bool RaidZonedBlockDevice::ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                                     idx_t idx) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->ZoneIsSwr(zones, idx);
  }
  return false;
}

bool RaidZonedBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                         idx_t idx) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->ZoneIsOffline(zones, idx);
  }
  return false;
}

bool RaidZonedBlockDevice::ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                                          idx_t idx) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->ZoneIsWritable(zones, idx);
  }
  return false;
}

bool RaidZonedBlockDevice::ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                                        idx_t idx) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->ZoneIsActive(zones, idx);
  }
  return false;
}

bool RaidZonedBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                      idx_t idx) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->ZoneIsOpen(zones, idx);
  }
  return false;
}

uint64_t RaidZonedBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                         idx_t idx) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->ZoneStart(zones, idx);
  }
  return 0;
}

uint64_t RaidZonedBlockDevice::ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                               idx_t idx) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->ZoneMaxCapacity(zones, idx);
  }
  return 0;
}

uint64_t RaidZonedBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                      idx_t idx) {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->ZoneWp(zones, idx);
  }
  return 0;
}

std::string RaidZonedBlockDevice::GetFilename() {
  if (main_mode_ == RaidMode::RAID0 || main_mode_ == RaidMode::RAID1) {
    return device_default()->GetFilename();
  }
  return {};
}
}  // namespace ROCKSDB_NAMESPACE