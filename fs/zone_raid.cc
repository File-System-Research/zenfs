//
// Created by chiro on 23-4-28.
//

#include "zone_raid.h"

#include <memory>
#include <utility>

#include "rocksdb/io_status.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/mutexlock.h"

namespace AQUAFS_NAMESPACE {
using namespace ROCKSDB_NAMESPACE;

class RaidConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  RaidConsoleLogger() : Logger(InfoLogLevel::INFO_LEVEL) {}

  void Logv(const char *format, va_list ap) override {
    MutexLock _(&lock_);
    printf("[RAID] ");
    vprintf(format, ap);
    printf("\n");
    fflush(stdout);
  }

  port::Mutex lock_;
};

/**
 * @brief Construct a new Raid Zoned Block Device object
 * @param mode main mode. RAID_A for auto-raid
 * @param devices all devices under management
 */
RaidZonedBlockDevice::RaidZonedBlockDevice(
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices,
    RaidMode mode, std::shared_ptr<Logger> logger)
    : logger_(std::move(logger)),
      main_mode_(mode),
      devices_(std::move(devices)) {
  if (!logger_) logger_.reset(new RaidConsoleLogger());
  assert(!devices_.empty());
  // Debug(logger_, "RAID Devices: ");
  for (auto &&d : devices_) {
    // Debug(logger_, "  %s", d->GetFilename().c_str());
  }
  // create temporal device map: AQUAFS_META_ZONES in the first device is used
  // as meta zones, and marked as RAID_NONE; others are marked as RAID0
  idx_t idx;
  for (idx = 0; idx < AQUAFS_META_ZONES; idx++) {
    for (size_t i = 0; i < nr_dev(); i++)
      device_zone_map_[idx + i] = {0, idx, 0};
    mode_map_[idx] = {RaidMode::RAID_NONE, 0};
  }
  syncBackendInfo();
}

IOStatus RaidZonedBlockDevice::Open(bool readonly, bool exclusive,
                                    unsigned int *max_active_zones,
                                    unsigned int *max_open_zones) {
  // Debug(logger_, "Open(readonly=%s, exclusive=%s)",
  //       std::to_string(readonly).c_str(), std::to_string(exclusive).c_str());
  IOStatus s;
  for (auto &&d : devices_) {
    s = d->Open(readonly, exclusive, max_active_zones, max_open_zones);
    if (!s.ok()) return s;
    // Debug(logger_,
    //       "%s opened, sz=%lx, nr_zones=%x, zone_sz=%lx blk_sz=%x "
    //       "max_active_zones=%x, max_open_zones=%x",
    //       d->GetFilename().c_str(), d->GetNrZones() * d->GetZoneSize(),
    //       d->GetNrZones(), d->GetZoneSize(), d->GetBlockSize(),
    //       *max_active_zones, *max_open_zones);
    assert(d->GetNrZones() == def_dev()->GetNrZones());
    assert(d->GetZoneSize() == def_dev()->GetZoneSize());
    assert(d->GetBlockSize() == def_dev()->GetBlockSize());
  }
  syncBackendInfo();
  a_zones_.reset(new raid_zone_t[nr_zones_]);
  memset(a_zones_.get(), 0, sizeof(raid_zone_t) * nr_zones_);
  return s;
}

void RaidZonedBlockDevice::syncBackendInfo() {
  total_nr_devices_zones_ = std::accumulate(
      devices_.begin(), devices_.end(), 0,
      [](int sum, const std::unique_ptr<ZonedBlockDeviceBackend> &dev) {
        return sum + dev->GetNrZones();
      });
  block_sz_ = def_dev()->GetBlockSize();
  zone_sz_ = def_dev()->GetZoneSize();
  nr_zones_ = def_dev()->GetNrZones();
  switch (main_mode_) {
    case RaidMode::RAID_C:
      nr_zones_ = total_nr_devices_zones_;
      break;
    case RaidMode::RAID_A:
    case RaidMode::RAID0:
      zone_sz_ *= nr_dev();
      break;
    case RaidMode::RAID1:
      break;
    default:
      nr_zones_ = 0;
      break;
  }
  // Debug(logger_, "syncBackendInfo(): blksz=%x, zone_sz=%lx, nr_zones=%x",
  //       block_sz_, zone_sz_, nr_zones_);
}

std::unique_ptr<ZoneList> RaidZonedBlockDevice::ListZones() {
  // Debug(logger_, "ListZones()");
  if (main_mode_ == RaidMode::RAID_C) {
    std::vector<std::unique_ptr<ZoneList>> list;
    for (auto &&dev : devices_) {
      auto zones = dev->ListZones();
      if (zones) {
        list.emplace_back(std::move(zones));
      }
    }
    // merge zones
    auto nr_zones = std::accumulate(
        list.begin(), list.end(), 0,
        [](int sum, auto &zones) { return sum + zones->ZoneCount(); });
    auto data = new struct zbd_zone[nr_zones];
    auto ptr = data;
    for (auto &&zones : list) {
      auto nr = zones->ZoneCount();
      memcpy(ptr, zones->GetData(), sizeof(struct zbd_zone) * nr);
      ptr += nr;
    }
    return std::make_unique<ZoneList>(data, nr_zones);
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ListZones();
  } else if (main_mode_ == RaidMode::RAID0) {
    auto zones = def_dev()->ListZones();
    if (zones) {
      auto nr_zones = zones->ZoneCount();
      // TODO: mix use of ZoneFS and libzbd
      auto data = new struct zbd_zone[nr_zones];
      auto ptr = data;
      memcpy(data, zones->GetData(), sizeof(struct zbd_zone) * nr_zones);
      for (decltype(nr_zones) i = 0; i < nr_zones; i++) {
        ptr->start *= nr_dev();
        ptr->capacity *= nr_dev();
        // what's this? len == capacity?
        ptr->len *= nr_dev();
        ptr++;
      }
      return std::make_unique<ZoneList>(data, nr_zones);
    }
  } else if (main_mode_ == RaidMode::RAID_A) {
    auto data = new raid_zone_t[nr_zones_];
    memcpy(data, a_zones_.get(), sizeof(raid_zone_t) * nr_zones_);
    return std::make_unique<ZoneList>(data, nr_zones_);
  }
  return {};
}

IOStatus RaidZonedBlockDevice::Reset(uint64_t start, bool *offline,
                                     uint64_t *max_capacity) {
  // Debug(logger_, "Reset(start=%lx)", start);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      auto sz = d->GetNrZones() * d->GetZoneSize();
      if (sz > start) {
        return d->Reset(start, offline, max_capacity);
      } else {
        start -= sz;
      }
    }
    return IOStatus::IOError();
  } else if (main_mode_ == RaidMode::RAID1) {
    IOStatus s;
    for (auto &&d : devices_) {
      s = d->Reset(start, offline, max_capacity);
      if (!s.ok()) return s;
    }
    return s;
  } else if (main_mode_ == RaidMode::RAID0) {
    assert(start % GetBlockSize() == 0);
    assert(start % GetZoneSize() == 0);
    // auto idx_dev = get_idx_dev(start);
    auto s = start / nr_dev();
    // auto r = devices_[idx_dev]->Reset(s, offline, max_capacity);
    IOStatus r{};
    for (auto &&d : devices_) {
      r = d->Reset(s, offline, max_capacity);
      if (r.ok()) {
        *max_capacity *= nr_dev();
      }
    }
    return r;
  }
  return unsupported;
}

IOStatus RaidZonedBlockDevice::Finish(uint64_t start) {
  // Debug(logger_, "Finish(%lx)", start);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      auto sz = d->GetNrZones() * d->GetZoneSize();
      if (sz > start) {
        return d->Finish(start);
      } else {
        start -= sz;
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    IOStatus s;
    for (auto &&d : devices_) {
      s = d->Finish(start);
      if (!s.ok()) return s;
    }
    return s;
  }
  return unsupported;
}

IOStatus RaidZonedBlockDevice::Close(uint64_t start) {
  // Debug(logger_, "Close(start=%lx)", start);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      auto sz = d->GetNrZones() * d->GetZoneSize();
      if (sz > start) {
        return d->Close(start);
      } else {
        start -= sz;
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    IOStatus s;
    for (auto &&d : devices_) {
      s = d->Close(start);
      if (!s.ok()) return s;
    }
    return s;
  } else if (main_mode_ == RaidMode::RAID0) {
    assert(start % GetBlockSize() == 0);
    assert(start % GetZoneSize() == 0);
    // auto idx_dev = get_idx_dev(start);
    auto s = start / nr_dev();
    // auto r = devices_[idx_dev]->Close(s);
    IOStatus r{};
    for (auto &&d : devices_) {
      r = d->Close(s);
      if (!r.ok()) {
        return r;
      }
    }
    return r;
  }
  return unsupported;
}

int RaidZonedBlockDevice::Read(char *buf, int size, uint64_t pos, bool direct) {
  // Debug(logger_, "Read(sz=%x, pos=%lx, direct=%s)", size, pos,
  // std::to_string(direct).c_str());
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      auto sz = d->GetNrZones() * d->GetZoneSize();
      if (sz > pos) {
        return d->Read(buf, size, pos, direct);
      } else {
        pos -= sz;
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    int r = 0;
    for (auto &&d : devices_) {
      if ((r = d->Read(buf, size, pos, direct))) {
        return r;
      }
    }
    return r;
  } else if (main_mode_ == RaidMode::RAID0) {
    // split read range as blocks
    int sz_read = 0;
    // TODO: write blocks in multi-threads
    int r;
    while (size > 0) {
      auto req_size = std::min(
          size, static_cast<int>(GetBlockSize() - pos % GetBlockSize()));
      r = devices_[get_idx_dev(pos)]->Read(buf, req_size, req_pos(pos), direct);
      if (r > 0) {
        size -= r;
        sz_read += r;
        buf += r;
        pos += r;
      } else {
        return r;
      }
    }
    return sz_read;
  }
  return 0;
}

int RaidZonedBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
  // Debug(logger_, "Write(size=%x, pos=%lx)", size, pos);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      auto sz = d->GetNrZones() * d->GetZoneSize();
      if (sz > pos) {
        return d->Write(data, size, pos);
      } else {
        pos -= sz;
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    int r = 0;
    for (auto &&d : devices_) {
      if ((r = d->Write(data, size, pos))) {
        return r;
      }
    }
    return r;
  } else if (main_mode_ == RaidMode::RAID0) {
    // split read range as blocks
    int sz_written = 0;
    // TODO: write blocks in multi-threads
    int r;
    while (size > 0) {
      auto req_size = std::min(
          size, GetBlockSize() - (static_cast<uint32_t>(pos)) % GetBlockSize());
      auto p = req_pos(pos);
      auto idx_dev = get_idx_dev(pos);
      r = devices_[idx_dev]->Write(data, req_size, p);
      // Debug(logger_, "WRITE: pos=%lx, dev=%lu, req_sz=%x, req_pos=%lx,
      // ret=%d",
      //       pos, idx_dev, req_size, p, r);
      if (r > 0) {
        size -= r;
        sz_written += r;
        data += r;
        pos += r;
      } else {
        return r;
      }
    }
    return sz_written;
  }
  return 0;
}

int RaidZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  // Debug(logger_, "InvalidateCache(pos=%lx, sz=%lx)", pos, size);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      auto sz = d->GetNrZones() * d->GetZoneSize();
      if (sz > pos) {
        return d->InvalidateCache(pos, size);
      } else {
        pos -= sz;
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    int r = 0;
    for (auto &&d : devices_) r = d->InvalidateCache(pos, size);
    return r;
  } else if (main_mode_ == RaidMode::RAID0) {
    assert(size % GetBlockSize() == 0);
    for (size_t i = 0; i < nr_dev(); i++) {
      devices_[i]->InvalidateCache(req_pos(pos), size / nr_dev());
    }
  }
  // default OK
  return 0;
}

bool RaidZonedBlockDevice::ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                                     idx_t idx) {
  // Debug(logger_, "ZoneIsSwr(idx=%x)", idx);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      if (d->GetNrZones() > idx) {
        auto z = d->ListZones();
        return d->ZoneIsSwr(z, idx);
      } else {
        idx -= d->GetNrZones();
      }
    }
    return false;
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ZoneIsSwr(zones, idx);
  } else if (main_mode_ == RaidMode::RAID0) {
    // asserts that all devices have the same zone layout
    auto z = def_dev()->ListZones();
    return def_dev()->ZoneIsSwr(z, idx);
  }
  return false;
}

bool RaidZonedBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                         idx_t idx) {
  // Debug(logger_, "ZoneIsOffline(idx=%x)", idx);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      if (d->GetNrZones() > idx) {
        // FIXME: optimize list-zones
        auto z = d->ListZones();
        return d->ZoneIsOffline(z, idx);
      } else {
        idx -= d->GetNrZones();
      }
    }
    return false;
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ZoneIsOffline(zones, idx);
  } else if (main_mode_ == RaidMode::RAID0) {
    // asserts that all devices have the same zone layout
    auto z = def_dev()->ListZones();
    return def_dev()->ZoneIsOffline(z, idx);
  }
  return false;
}

bool RaidZonedBlockDevice::ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                                          idx_t idx) {
  // Debug(logger_, "ZoneIsWriteable(idx=%x)", idx);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      if (d->GetNrZones() > idx) {
        auto z = d->ListZones();
        return d->ZoneIsWritable(z, idx);
      } else {
        idx -= d->GetNrZones();
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ZoneIsWritable(zones, idx);
  } else if (main_mode_ == RaidMode::RAID0) {
    // asserts that all devices have the same zone layout
    auto z = def_dev()->ListZones();
    return def_dev()->ZoneIsWritable(z, idx);
  }
  return false;
}

bool RaidZonedBlockDevice::ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                                        idx_t idx) {
  // Debug(logger_, "ZoneIsActive(idx=%x)", idx);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      if (d->GetNrZones() > idx) {
        auto z = d->ListZones();
        return d->ZoneIsActive(z, idx);
      } else {
        idx -= d->GetNrZones();
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ZoneIsActive(zones, idx);
  } else if (main_mode_ == RaidMode::RAID0) {
    // asserts that all devices have the same zone layout
    auto z = def_dev()->ListZones();
    return def_dev()->ZoneIsActive(z, idx);
  }
  return false;
}

bool RaidZonedBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                      idx_t idx) {
  // Debug(logger_, "ZoneIsOpen(idx=%x)", idx);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      if (d->GetNrZones() > idx) {
        auto z = d->ListZones();
        return d->ZoneIsOpen(z, idx);
      } else {
        idx -= d->GetNrZones();
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ZoneIsOpen(zones, idx);
  } else if (main_mode_ == RaidMode::RAID0) {
    // asserts that all devices have the same zone layout
    auto z = def_dev()->ListZones();
    return def_dev()->ZoneIsOpen(z, idx);
  }
  return false;
}

uint64_t RaidZonedBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                         idx_t idx) {
  // Debug(logger_, "ZoneStart(idx=%x)", idx);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      if (d->GetNrZones() > idx) {
        auto z = d->ListZones();
        return d->ZoneStart(z, idx);
      } else {
        idx -= d->GetNrZones();
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ZoneStart(zones, idx);
  } else if (main_mode_ == RaidMode::RAID0) {
    auto r =
        std::accumulate(devices_.begin(), devices_.end(),
                        static_cast<uint64_t>(0), [&](uint64_t sum, auto &d) {
                          auto z = d->ListZones();
                          return sum + d->ZoneStart(z, idx);
                        });
    return r;
  }
  return 0;
}

uint64_t RaidZonedBlockDevice::ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                               idx_t idx) {
  // Debug(logger_, "ZoneMaxCapacity(idx=%x)", idx);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      if (d->GetNrZones() > idx) {
        auto z = d->ListZones();
        return d->ZoneMaxCapacity(z, idx);
      } else {
        idx -= d->GetNrZones();
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ZoneMaxCapacity(zones, idx);
  } else if (main_mode_ == RaidMode::RAID0) {
    // asserts that all devices have the same zone layout
    auto z = def_dev()->ListZones();
    return def_dev()->ZoneMaxCapacity(z, idx) * nr_dev();
  }
  return 0;
}

uint64_t RaidZonedBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                      idx_t idx) {
  // Debug(logger_, "ZoneWp(idx=%x)", idx);
  if (main_mode_ == RaidMode::RAID_C) {
    for (auto &&d : devices_) {
      if (d->GetNrZones() > idx) {
        auto z = d->ListZones();
        return d->ZoneWp(z, idx);
      } else {
        idx -= d->GetNrZones();
      }
    }
  } else if (main_mode_ == RaidMode::RAID1) {
    return def_dev()->ZoneWp(zones, idx);
  } else if (main_mode_ == RaidMode::RAID0) {
    return std::accumulate(devices_.begin(), devices_.end(),
                           static_cast<uint64_t>(0),
                           [&](uint64_t sum, auto &d) {
                             auto z = d->ListZones();
                             return sum + d->ZoneWp(z, idx);
                           });
  }
  return 0;
}

std::string RaidZonedBlockDevice::GetFilename() {
  std::string name = std::string("raid") + raid_mode_str(main_mode_) + ":";
  for (auto p = devices_.begin(); p != devices_.end(); p++) {
    name += (*p)->GetFilename();
    if (p + 1 != devices_.end()) name += ",";
  }
  return name;
}
bool RaidZonedBlockDevice::IsRAIDEnabled() const { return true; }
RaidMode RaidZonedBlockDevice::getMainMode() const { return main_mode_; }
}  // namespace AQUAFS_NAMESPACE