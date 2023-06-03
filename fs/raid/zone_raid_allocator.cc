//
// Created by chiro on 23-6-3.
//

#include "zone_raid_allocator.h"
namespace aquafs {

Status ZoneRaidAllocator::createMapping(idx_t logical_raid_zone_sub_idx,
                                        idx_t physical_device_idx,
                                        idx_t physical_zone_sub_idx) {
  // TODO: check allocation
  device_zone_map_[logical_raid_zone_sub_idx] = {
      physical_device_idx, static_cast<idx_t>(physical_zone_sub_idx), 0};
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

}  // namespace aquafs