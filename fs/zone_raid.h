#include <map>
#include <memory>
#include <numeric>
#include <unordered_map>

#include "rocksdb/io_status.h"
#include "rocksdb/rocksdb_namespace.h"
#include "zbd_aquafs.h"

namespace AQUAFS_NAMESPACE {
using namespace ROCKSDB_NAMESPACE;

class RaidConsoleLogger;

enum class RaidMode : uint32_t {
  // AquaFS: No RAID, just use the first backend device
  RAID_NONE = 0,
  RAID0,
  RAID1,
  RAID5,
  RAID6,
  RAID10,
  // AquaFS: Concat-RAID
  RAID_C,
  // AquaFS: Auto-RAID
  RAID_A
};

__attribute__((__unused__)) static const char *raid_mode_str(RaidMode mode) {
  switch (mode) {
    case RaidMode::RAID0:
      return "0";
    case RaidMode::RAID1:
      return "1";
    case RaidMode::RAID5:
      return "5";
    case RaidMode::RAID6:
      return "6";
    case RaidMode::RAID10:
      return "10";
    case RaidMode::RAID_A:
      return "a";
    case RaidMode::RAID_C:
      return "c";
    case RaidMode::RAID_NONE:
      return "n";
    default:
      return "UNKNOWN";
  }
}
__attribute__((__unused__)) static RaidMode raid_mode_from_str(
    const std::string &str) {
  if (str == "0") {
    return RaidMode::RAID0;
  } else if (str == "1") {
    return RaidMode::RAID1;
  } else if (str == "5") {
    return RaidMode::RAID5;
  } else if (str == "6") {
    return RaidMode::RAID6;
  } else if (str == "10") {
    return RaidMode::RAID10;
  } else if (str == "A" || str == "a" || str == "-a" || str == "-A") {
    return RaidMode::RAID_A;
  } else if (str == "C" || str == "c" || str == "-c" || str == "-C") {
    return RaidMode::RAID_C;
  }
  return RaidMode::RAID_A;
}

using idx_t = unsigned int;

class RaidMapItem {
 public:
  // device index
  idx_t device_idx{};
  // zone index on this device
  idx_t zone_idx{};
  // when invalid, ignore this <device_idx, zone_idx> in the early record
  uint16_t invalid{};

  Status DecodeFrom(Slice *input);
};

class RaidModeItem {
 public:
  RaidMode mode = RaidMode::RAID_NONE;
  // extra option for raid mode, for example: n extra zones for raid5
  uint32_t option{};

  Status DecodeFrom(Slice *input);
};

class RaidZonedBlockDevice : public ZonedBlockDeviceBackend {
 public:
  // use `map` or `unordered_map` to store raid mappings
  template <typename K, typename V>
  using map_use = std::unordered_map<K, V>;
  using device_zone_map_t = map_use<idx_t, RaidMapItem>;
  using mode_map_t = map_use<idx_t, RaidModeItem>;
  using raid_zone_t = struct zbd_zone;

  ZonedBlockDeviceBackend *def_dev() const { return devices_.begin()->get(); }

 private:
  std::shared_ptr<Logger> logger_;
  RaidMode main_mode_;
  std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices_;
  // map: raid zone idx (* sz) -> device idx, device zone idx
  device_zone_map_t device_zone_map_;
  // map: raid zone idx -> raid mode, option
  mode_map_t mode_map_;
  uint32_t total_nr_devices_zones_;
  // auto-raid: manually managed zone info
  std::unique_ptr<raid_zone_t> a_zones_{};

  const IOStatus unsupported = IOStatus::NotSupported("Raid unsupported");

  void syncBackendInfo();

  void flush_zone_info();

 public:
  explicit RaidZonedBlockDevice(
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices,
      RaidMode mode, std::shared_ptr<Logger> logger);
  explicit RaidZonedBlockDevice(
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices)
      : RaidZonedBlockDevice(std::move(devices), RaidMode::RAID_A, nullptr) {}

  void layout_update(device_zone_map_t &&device_zone, mode_map_t &&mode_map) {
    for (auto &&p : device_zone) device_zone_map_.insert(p);
    for (auto &&p : mode_map) mode_map_.insert(p);
    flush_zone_info();
  }
  void layout_setup(device_zone_map_t &&device_zone, mode_map_t &&mode_map) {
    device_zone_map_ = std::move(device_zone);
    mode_map_ = std::move(mode_map);
    flush_zone_info();
  }
  const device_zone_map_t &getDeviceZoneMap() const { return device_zone_map_; }
  const mode_map_t &getModeMap() const { return mode_map_; }

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
  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                           idx_t idx) override;
  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  std::string GetFilename() override;

  template <class T>
  T nr_dev_t() const {
    return static_cast<T>(devices_.size());
  }
  auto nr_dev() const { return devices_.size(); }
  template <typename T>
  auto get_idx_block(T pos) const {
    return pos / static_cast<T>(GetBlockSize());
  }
  template <typename T>
  auto get_idx_dev(T pos) const {
    return get_idx_dev(pos, nr_dev_t<T>());
  }
  template <typename T>
  auto get_idx_dev(T pos, T m) const {
    return get_idx_block(pos) % m;
  }
  template <typename T>
  auto req_pos(T pos) const {
    auto blk_offset = pos % static_cast<T>(GetBlockSize());
    return blk_offset +
           ((pos - blk_offset) / GetBlockSize()) / nr_dev() * GetBlockSize();
  }
  bool IsRAIDEnabled() const override;
  RaidMode getMainMode() const;
  template <class T>
  RaidMapItem getAutoDeviceZone(T pos);
  template <class T>
  T getAutoMappedDevicePos(T pos);

  ~RaidZonedBlockDevice() override = default;
};

class RaidInfoBasic {
 public:
  RaidMode main_mode = RaidMode::RAID_NONE;
  uint32_t nr_devices = 0;
  // assert all devices are same in these fields
  uint32_t dev_block_size = 0; /* in bytes */
  uint32_t dev_zone_size = 0;  /* in blocks */
  uint32_t dev_nr_zones = 0;   /* in one device */

  void load(ZonedBlockDevice *zbd) {
    assert(sizeof(RaidInfoBasic) == sizeof(uint32_t) * 5);
    if (zbd->IsRAIDEnabled()) {
      auto be = dynamic_cast<RaidZonedBlockDevice *>(zbd->getBackend().get());
      if (!be) return;
      main_mode = be->getMainMode();
      nr_devices = be->nr_dev();
      dev_block_size = be->def_dev()->GetBlockSize();
      dev_zone_size = be->def_dev()->GetZoneSize();
      dev_nr_zones = be->def_dev()->GetNrZones();
    }
  }

  Status compatible(ZonedBlockDevice *zbd) const {
    if (!zbd->IsRAIDEnabled()) return Status::OK();
    auto be = dynamic_cast<RaidZonedBlockDevice *>(zbd->getBackend().get());
    if (!be) return Status::NotSupported("RAID Error", "cannot cast pointer");
    if (main_mode != be->getMainMode())
      return Status::Corruption(
          "RAID Error", "main_mode mismatch: superblock-raid" +
                            std::string(raid_mode_str(main_mode)) +
                            " != disk-raid" + raid_mode_str(be->getMainMode()));
    if (nr_devices != be->nr_dev())
      return Status::Corruption("RAID Error", "nr_devices mismatch");
    if (dev_block_size != be->def_dev()->GetBlockSize())
      return Status::Corruption("RAID Error", "dev_block_size mismatch");
    if (dev_zone_size != be->def_dev()->GetZoneSize())
      return Status::Corruption("RAID Error", "dev_zone_size mismatch");
    if (dev_nr_zones != be->def_dev()->GetNrZones())
      return Status::Corruption("RAID Error", "dev_nr_zones mismatch");
    return Status::OK();
  }
};

class RaidInfoAppend {
 public:
  RaidZonedBlockDevice::device_zone_map_t device_zone_map;
  RaidZonedBlockDevice::mode_map_t mode_map;
};
};  // namespace AQUAFS_NAMESPACE