//
// Created by chiro on 23-5-6.
//

#include "zone_raid0.h"

#ifdef AQUAFS_RAID_URING
#include <algorithm>
#include <ranges>
#include <tuple>

#include "../../liburing4cpp/include/liburing/io_service.hpp"
#include "fs/zbdlib_aquafs.h"
#endif

namespace AQUAFS_NAMESPACE {
void Raid0ZonedBlockDevice::syncBackendInfo() {
  AbstractRaidZonedBlockDevice::syncBackendInfo();
  zone_sz_ *= nr_dev();
}
Raid0ZonedBlockDevice::Raid0ZonedBlockDevice(
    const std::shared_ptr<Logger> &logger,
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices)
    : AbstractRaidZonedBlockDevice(logger, RaidMode::RAID0,
                                   std::move(devices)) {
  syncBackendInfo();
}
std::unique_ptr<ZoneList> Raid0ZonedBlockDevice::ListZones() {
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
  } else {
    return nullptr;
  }
}
IOStatus Raid0ZonedBlockDevice::Reset(uint64_t start, bool *offline,
                                      uint64_t *max_capacity) {
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
IOStatus Raid0ZonedBlockDevice::Finish(uint64_t start) {
  assert(start % GetBlockSize() == 0);
  assert(start % GetZoneSize() == 0);
  // auto idx_dev = get_idx_dev(start);
  auto s = start / nr_dev();
  // auto r = devices_[idx_dev]->Finish(s);
  IOStatus r{};
  for (auto &&d : devices_) {
    r = d->Finish(s);
    if (!r.ok()) return r;
  }
  return r;
}
IOStatus Raid0ZonedBlockDevice::Close(uint64_t start) {
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

template <typename T, T...>
struct integer_sequence {};

template <std::size_t N, std::size_t... I>
struct gen_indices : gen_indices<(N - 1), (N - 1), I...> {};
template <std::size_t... I>
struct gen_indices<0, I...> : integer_sequence<std::size_t, I...> {};

template <typename H>
std::string &to_string_impl(std::string &s, H &&h) {
  using std::to_string;
  s += to_string(std::forward<H>(h));
  return s;
}

template <typename H, typename... T>
std::string &to_string_impl(std::string &s, H &&h, T &&...t);

template <typename... T>
std::string &to_string_impl(std::string &s, char *const &h, T &&...t) {
  // s += h;
  s += std::to_string((uint64_t)((void *)h)) + ",";
  return to_string_impl(s, std::forward<T>(t)...);
}

template <typename H, typename... T>
std::string &to_string_impl(std::string &s, H &&h, T &&...t) {
  s += std::to_string(std::forward<H>(h)) + ",";
  return to_string_impl(s, std::forward<T>(t)...);
}

template <typename... T, std::size_t... I>
std::string to_string(const std::tuple<T...> &tup,
                      integer_sequence<std::size_t, I...>) {
  std::string result;
  int ctx[] = {(to_string_impl(result, std::get<I>(tup)...), 0), 0};
  (void)ctx;
  return result;
}

template <typename... T>
std::string to_string(const std::tuple<T...> &tup) {
  return to_string(tup, gen_indices<sizeof...(T)>{});
}

int Raid0ZonedBlockDevice::Read(char *buf, int size, uint64_t pos,
                                bool direct) {
#ifndef AQUAFS_RAID_URING
  // split read range as blocks
  int sz_read = 0;
  int r;
  while (size > 0) {
    auto req_size =
        std::min(size, static_cast<int>(GetBlockSize() - pos % GetBlockSize()));
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
#else

#if 1
  char *const buf_raw = buf;
  memset(buf, 0, size);
  uio::io_service service;
  // split read range as blocks
  int sz_read = 0;
  using req_item_t = std::tuple<int, char *, uint64_t, off_t>;
  std::vector<req_item_t> requests;
  std::vector<ZbdlibBackend *> bes(nr_dev());
  for (decltype(nr_dev()) i = 0; i < nr_dev(); i++) {
    // bes[i] = dynamic_cast<ZbdlibBackend *>(devices_[i].get());
    // assert(bes[i] != nullptr);
    bes[i] = (ZbdlibBackend *)(devices_[i].get());
  }
  while (size > 0) {
    auto req_size =
        std::min(size, static_cast<int>(GetBlockSize() - pos % GetBlockSize()));
    if (req_size == 0) break;
    // auto be = dynamic_cast<ZbdlibBackend
    // *>(devices_[get_idx_dev(pos)].get());
    auto be = bes[get_idx_dev(pos)];
    assert(be != nullptr);
    int fd = direct ? be->read_direct_f_ : be->read_f_;
    // int dev_idx = get_idx_dev(pos);
    // requests.emplace_back(fd, buf, req_pos(pos), req_size);
    requests.emplace_back(fd, buf, req_size, req_pos(pos));
    size -= req_size;
    sz_read += req_size;
    buf += req_size;
    pos += req_size;
  }
  std::vector<int> results;
  service.run([&]() -> uio::task<> {
#if 0
    // it stucks...

    // std::ostringstream requests_stream;
    // std::transform(requests.begin(), requests.end(),
    //                std::ostream_iterator<std::string>(requests_stream,","),
    //                [](const auto &req) { return "<" + to_string(req) + ">";
    //                });
    // Warn(logger_, "got %zu requests: %s", requests.size(),
    //      requests_stream.str().c_str());
    std::vector<uio::sqe_awaitable> futures;
    std::transform(requests.begin(), requests.end(),
                   std::back_inserter(futures), [&](const auto &req) {
                     return service.read(std::get<0>(req), std::get<1>(req),
                                         std::get<2>(req), std::get<3>(req), 0);
                   });
    // Warn(logger_, "got %zu futures", futures.size());
    for (auto &&res : futures) {
      auto r = co_await res | uio::panic_on_err("failed to read!", true);
      Warn(logger_, "got result %d", r);
      if (std::accumulate(buf_raw, buf_raw + size, 0) == 0)
        Warn(logger_, "got all zero data!");
      results.push_back(r);
    }
#else
    std::map<int, std::vector<iovec>> req_vec;
    for (auto &req : requests) {
      results.emplace_back(
          co_await service.read(std::get<0>(req), std::get<1>(req),
                                std::get<2>(req), std::get<3>(req), 0) |
          uio::panic_on_err("failed to read!", true));
      // results.emplace_back(std::get<2>(req));
      // req_vec[std::get<0>(req)].emplace_back(std::get<1>(req),
      // std::get<2>(req));
    }
#endif
  }());
  auto neg_p =
      std::find_if(results.begin(), results.end(), [](int r) { return r < 0; });
  if (neg_p != results.end()) {
    Error(logger_, "got failed result %d", *neg_p);
    return *neg_p;
  }
  sz_read = std::accumulate(results.begin(), results.end(), 0);
  return sz_read;

#else
  int sz_read = size;
  uio::io_service service;
  using req_item_t = std::tuple<int, char *, uint64_t, off_t>;
  std::vector<req_item_t> requests;
  service.run([&]() -> uio::task<> {
    while (size > 0) {
      auto req_size = std::min(
          size, static_cast<int>(GetBlockSize() - pos % GetBlockSize()));
      auto be = dynamic_cast<ZbdlibBackend *>(devices_[get_idx_dev(pos)].get());
      assert(be != nullptr);
      int fd = direct ? be->read_direct_f_ : be->read_f_;
      // int r = co_await
      // service.read(fd, buf, req_size, req_pos(pos), IOSQE_IO_DRAIN) |
      //     uio::panic_on_err("failed to read!", true);
      // Warn(logger_, "r = %d, size=%d", r, size);
      requests.emplace_back(fd, buf, req_size, req_pos(pos));
      size -= req_size;
      buf += req_size;
      pos += req_size;
    }
    // merge requests
    std::map<int, std::vector<req_item_t>> req_map;
    for (auto &req : requests) {
      req_map[std::get<0>(req)].emplace_back(req);
    }
    for (auto &req : req_map) {
      auto &reqs = req.second;
      std::vector<iovec> req_vec;
      for (auto &r : reqs) {
        req_vec.emplace_back(std::get<1>(r), std::get<2>(r));
      }
      auto r = co_await service.readv(req.first, req_vec, 0) |
               uio::panic_on_err("failed to read!", true);
      if (r < 0) {
        Error(logger_, "got failed result %d", r);
        co_return;
      }
    }
    co_return;
  }());
  return sz_read;
#endif
#endif
}
int Raid0ZonedBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
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
int Raid0ZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  assert(size % GetBlockSize() == 0);
  for (size_t i = 0; i < nr_dev(); i++) {
    devices_[i]->InvalidateCache(req_pos(pos), size / nr_dev());
  }
  return 0;
}
bool Raid0ZonedBlockDevice::ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                                      unsigned int idx) {
  // asserts that all devices have the same zone layout
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsSwr(z, idx);
}
bool Raid0ZonedBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                          unsigned int idx) {
  // asserts that all devices have the same zone layout
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsOffline(z, idx);
}
bool Raid0ZonedBlockDevice::ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                                           unsigned int idx) {
  // asserts that all devices have the same zone layout
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsWritable(z, idx);
}
bool Raid0ZonedBlockDevice::ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                                         unsigned int idx) {
  // asserts that all devices have the same zone layout
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsActive(z, idx);
}
bool Raid0ZonedBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                       unsigned int idx) {
  // asserts that all devices have the same zone layout
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsOpen(z, idx);
}
uint64_t Raid0ZonedBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                          unsigned int idx) {
  auto r =
      std::accumulate(devices_.begin(), devices_.end(),
                      static_cast<uint64_t>(0), [&](uint64_t sum, auto &d) {
                        auto z = d->ListZones();
                        return sum + d->ZoneStart(z, idx);
                      });
  return r;
}
uint64_t Raid0ZonedBlockDevice::ZoneMaxCapacity(
    std::unique_ptr<ZoneList> &zones, unsigned int idx) {
  // asserts that all devices have the same zone layout
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneMaxCapacity(z, idx) * nr_dev();
}
uint64_t Raid0ZonedBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                       unsigned int idx) {
  return std::accumulate(devices_.begin(), devices_.end(),
                         static_cast<uint64_t>(0), [&](uint64_t sum, auto &d) {
                           auto z = d->ListZones();
                           return sum + d->ZoneWp(z, idx);
                         });
}
}  // namespace AQUAFS_NAMESPACE
