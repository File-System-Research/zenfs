//
// Created by chiro on 23-4-28.
//

#include "zone_raid.h"

#include <memory>
#include <queue>
#include <utility>

#include "rocksdb/io_status.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace AQUAFS_NAMESPACE {
using namespace ROCKSDB_NAMESPACE;

class RaidConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  RaidConsoleLogger() : Logger(InfoLogLevel::DEBUG_LEVEL) {}

  void Logv(const char *format, va_list ap) override {
    MutexLock _(&lock_);
    printf("[RAID] ");
    vprintf(format, ap);
    printf("\n");
    fflush(stdout);
  }

  port::Mutex lock_;
};

AbstractRaidZonedBlockDevice::AbstractRaidZonedBlockDevice(
    const std::shared_ptr<Logger> &logger, RaidMode main_mode,
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &devices)
    : logger_(logger), main_mode_(main_mode), devices_(std::move(devices)) {
  if (!logger_) logger_.reset(new RaidConsoleLogger());
  assert(!devices_.empty());
  Info(logger_, "RAID Devices: ");
  for (auto &&d : devices_) Info(logger_, "  %s", d->GetFilename().c_str());
}

IOStatus AbstractRaidZonedBlockDevice::Open(bool readonly, bool exclusive,
                                            unsigned int *max_active_zones,
                                            unsigned int *max_open_zones) {
  Info(logger_, "Open(readonly=%s, exclusive=%s)",
       std::to_string(readonly).c_str(), std::to_string(exclusive).c_str());
  IOStatus s;
  for (auto &&d : devices_) {
    s = d->Open(readonly, exclusive, max_active_zones, max_open_zones);
    if (!s.ok()) return s;
    Info(logger_,
         "%s opened, sz=%lx, nr_zones=%x, zone_sz=%lx blk_sz=%x "
         "max_active_zones=%x, max_open_zones=%x",
         d->GetFilename().c_str(), d->GetNrZones() * d->GetZoneSize(),
         d->GetNrZones(), d->GetZoneSize(), d->GetBlockSize(),
         *max_active_zones, *max_open_zones);
    assert(d->GetNrZones() == def_dev()->GetNrZones());
    assert(d->GetZoneSize() == def_dev()->GetZoneSize());
    assert(d->GetBlockSize() == def_dev()->GetBlockSize());
  }
  syncBackendInfo();
  Info(logger_, "after Open(): nr_zones=%x, zone_sz=%lx blk_sz=%x", nr_zones_,
       zone_sz_, block_sz_);
  return s;
}
}  // namespace AQUAFS_NAMESPACE