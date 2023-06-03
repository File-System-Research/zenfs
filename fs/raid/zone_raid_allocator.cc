//
// Created by chiro on 23-6-3.
//

#include "zone_raid_allocator.h"
namespace aquafs {

Status ZoneRaidAllocator::setMapping(idx_t logical_raid_zone_sub_idx,
                                     idx_t physical_device_idx,
                                     idx_t physical_zone_idx) {
  // printf("setMapping(zone_sub=%x, device=%x, zone=%x)\n",
  //        logical_raid_zone_sub_idx, physical_device_idx, physical_zone_idx);
  // TODO: check allocation
  device_zone_map_[logical_raid_zone_sub_idx] = {
      physical_device_idx, static_cast<idx_t>(physical_zone_idx), 0};
  device_zone_inv_map_[std::make_pair(physical_device_idx, physical_zone_idx)] =
      logical_raid_zone_sub_idx;
  return Status::OK();
}
void ZoneRaidAllocator::setMappingMode(idx_t logical_raid_zone_idx,
                                       RaidModeItem mode) {
  mode_map_[logical_raid_zone_idx] = mode;
}
void ZoneRaidAllocator::setMappingMode(idx_t logical_raid_zone_idx,
                                       RaidMode mode) {
  setMappingMode(logical_raid_zone_idx, {mode, 0});
}
int ZoneRaidAllocator::getFreeDeviceZone(idx_t device) {
  for (idx_t j = 0; j < zone_nr_; j++) {
    auto key = std::make_pair(device, j);
    auto f = device_zone_inv_map_.find(key);
    if (f == device_zone_inv_map_.end()) return static_cast<int>(j);
  }
  return -1;
}
int ZoneRaidAllocator::getFreeZoneDevice(idx_t device_zone) {
  for (idx_t i = 0; i < device_nr_; i++) {
    auto key = std::make_pair(i, device_zone);
    auto f = device_zone_inv_map_.find(key);
    if (f == device_zone_inv_map_.end()) return static_cast<int>(i);
  }
  return -1;
}
Status ZoneRaidAllocator::createMapping(idx_t logical_raid_zone_idx) {
  size_t allocated = 0;
  while (allocated < device_nr_) {
    bool has_allocated = false;
    for (idx_t zone = 0; zone < zone_nr_ && allocated < device_nr_;) {
      auto d = getFreeZoneDevice(zone);
      if (d >= 0) {
        setMapping(logical_raid_zone_idx * device_nr_ + allocated,
                   static_cast<idx_t>(d), zone);
        allocated++;
        has_allocated = true;
      } else {
        zone++;
      }
    }
    if (!has_allocated) break;
  }
  if (allocated != device_nr_)
    return Status::NoSpace();
  else
    return Status::OK();
}

}  // namespace aquafs