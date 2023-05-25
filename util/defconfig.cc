//
// Created by chiro on 23-5-23.
//

#include <iostream>
#include <sstream>
#include "fs/configuration.h"

// required:
// GC_START_LEVEL,GC_SLOPE,
// finish_threshold_, -> when mkfs
// GC_AWAKE, -> sleep time?
// ZBD_ABSTRACT_TYPE, -> zbd / zonefs
// RAID_LEVEL,
// TARGET -> result through

int main(int argc, char **argv) {
  std::ostringstream stream;
  stream << "{\n";

  stream << "\"gc_start_level\": " << FLAGS_gc_start_level << ",\n";
  stream << "\"gc_slope\": " << FLAGS_gc_slope << ",\n";
  stream << "\"gc_sleep_time\": " << FLAGS_gc_sleep_time << "\n";

  stream << "}";
  std::cout << stream.str();
  return 0;
}
