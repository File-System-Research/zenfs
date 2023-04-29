//
// Created by chiro on 23-4-28.
//

#include "zone_raid.h"

namespace ROCKSDB_NAMESPACE {

IOStatus RaidZonedBlockDevice::Open(bool readonly, bool exclusive,
                                    unsigned int *max_active_zones,
                                    unsigned int *max_open_zones) {
  auto r = devices_.begin()->get()->Open(readonly, exclusive, max_active_zones,
                                         max_open_zones);
  max_active_zones_ = *max_active_zones;
  max_open_zones_ = *max_open_zones;
  syncMetaData();
  return r;
}
std::unique_ptr<ZoneList> RaidZonedBlockDevice::ListZones() {
  if (mode_ == RaidMode::RAID0) {
    return devices_.begin()->get()->ListZones();
  } else if (mode_ == RaidMode::RAID1) {
    // only half zones available
    auto zones = devices_.begin()->get()->ListZones();
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
  if (mode_ == RaidMode::RAID0) {
    return devices_.begin()->get()->Reset(start, offline, max_capacity);
  } else if (mode_ == RaidMode::RAID1) {
    bool offline_a, offline_b;
    uint64_t max_capacity_a, max_capacity_b;
    auto a = devices_.begin()->get()->Reset(start, &offline_a, &max_capacity_a);
    auto b =
        devices_.begin()->get()->Reset(start << 1, &offline_b, &max_capacity_b);
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
  if (mode_ == RaidMode::RAID0) {
    return devices_.begin()->get()->Finish(start);
  } else if (mode_ == RaidMode::RAID1) {
    auto a = devices_.begin()->get()->Finish(start);
    auto b = devices_.begin()->get()->Finish(start << 1);
    if (!a.ok()) return a;
    if (!b.ok()) return b;
    return IOStatus::OK();
  }
  return unsupported;
}
IOStatus RaidZonedBlockDevice::Close(uint64_t start) {
  if (mode_ == RaidMode::RAID0) {
    return devices_.begin()->get()->Close(start);
  } else if (mode_ == RaidMode::RAID1) {
    auto a = devices_.begin()->get()->Close(start);
    auto b = devices_.begin()->get()->Close(start << 1);
    if (!a.ok()) return a;
    if (!b.ok()) return b;
    return IOStatus::OK();
  }
  return unsupported;
}
int RaidZonedBlockDevice::Read(char *buf, int size, uint64_t pos, bool direct) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    // TODO: data read and check
    return devices_.begin()->get()->Read(buf, size, pos, direct);
  }
  return 0;
}
int RaidZonedBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
  if (mode_ == RaidMode::RAID0) {
    return devices_.begin()->get()->Write(data, size, pos);
  } else if (mode_ == RaidMode::RAID1) {
    auto a = devices_.begin()->get()->Write(data, size, pos);
    auto b = devices_.begin()->get()->Write(data, size, pos << 1);
    return a;
  }
  return 0;
}
int RaidZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  if (mode_ == RaidMode::RAID0) {
    return devices_.begin()->get()->InvalidateCache(pos, size);
  } else if (mode_ == RaidMode::RAID1) {
    auto a = devices_.begin()->get()->InvalidateCache(pos, size);
    auto b = devices_.begin()->get()->InvalidateCache(pos << 1, size);
    return a;
  }
  return 0;
}
bool RaidZonedBlockDevice::ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                                     unsigned int idx) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->ZoneIsSwr(zones, idx);
  }
  return false;
}
bool RaidZonedBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                         unsigned int idx) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->ZoneIsOffline(zones, idx);
  }
  return false;
}
bool RaidZonedBlockDevice::ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                                          unsigned int idx) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->ZoneIsWritable(zones, idx);
  }
  return false;
}
bool RaidZonedBlockDevice::ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                                        unsigned int idx) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->ZoneIsActive(zones, idx);
  }
  return false;
}
bool RaidZonedBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                      unsigned int idx) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->ZoneIsOpen(zones, idx);
  }
  return false;
}
uint64_t RaidZonedBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                         unsigned int idx) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->ZoneStart(zones, idx);
  }
  return 0;
}
uint64_t RaidZonedBlockDevice::ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                               unsigned int idx) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->ZoneMaxCapacity(zones, idx);
  }
  return 0;
}
uint64_t RaidZonedBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                      unsigned int idx) {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->ZoneWp(zones, idx);
  }
  return 0;
}
std::string RaidZonedBlockDevice::GetFilename() {
  if (mode_ == RaidMode::RAID0 || mode_ == RaidMode::RAID1) {
    return devices_.begin()->get()->GetFilename();
  }
  return {};
}
}  // namespace ROCKSDB_NAMESPACE