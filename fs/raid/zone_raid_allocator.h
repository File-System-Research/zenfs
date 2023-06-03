//
// Created by chiro on 23-6-3.
//

#ifndef ROCKSDB_ZONE_RAID_ALLOCATOR_H
#define ROCKSDB_ZONE_RAID_ALLOCATOR_H

#include <map>

#include "zone_raid.h"

namespace aquafs {

class ZoneRaidAllocator {
 public:
  // use `map` or `unordered_map` to store raid mappings
  template <typename K, typename V>
  using map_use = std::unordered_map<K, V>;
  using device_zone_map_t = map_use<idx_t, RaidMapItem>;
  using mode_map_t = map_use<idx_t, RaidModeItem>;

  // map: raid zone idx (* sz) -> device idx, device zone idx
  device_zone_map_t device_zone_map_{};
  // map: raid zone idx -> raid mode, option
  mode_map_t mode_map_{};

  const device_zone_map_t &getDeviceZoneMap() const { return device_zone_map_; }
  const mode_map_t &getModeMap() const { return mode_map_; }

  Status createMapping(idx_t logical_raid_zone_sub_idx,
                       idx_t physical_device_idx, idx_t physical_zone_sub_idx);
  void setMappingMode(idx_t logical_raid_zone_idx, RaidModeItem mode);
  void setMappingMode(idx_t logical_raid_zone_idx, RaidMode mode);
};

}  // namespace aquafs

#endif  // ROCKSDB_ZONE_RAID_ALLOCATOR_H
