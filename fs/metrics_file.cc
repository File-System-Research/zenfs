//
// Created by chiro on 23-6-4.
//

#include "metrics_file.h"

#include <fstream>

namespace aquafs {

AquaFSFileMetrics::AquaFSFileMetrics(const std::filesystem::path& distFile)
    : dist_file(distFile) {}
AquaFSFileMetrics::~AquaFSFileMetrics() {
  // save metrics data to dist file
  std::ofstream f(dist_file);
  f << "{\n";
  for (const auto& info : info_map_) {
    if (std::find_if(
            data.begin(), data.end(),
            [&](const std::pair<AquaFSMetricsHistograms,
                                std::vector<std::pair<float, uint64_t>>>&
                    item) { return item.first == info.first; }) == data.end())
      continue;
    f << "\"" << info.second.first << "\": {\n\t\"data\":\n\t\t\"result: [\n";
    f << "{ \"metric\": { \"type\": \"count\"}, \"values\": [\n";
    for (const auto& metric :
         data[static_cast<AquaFSMetricsHistograms>(info.first)]) {
      if (metric.first == info.first) {
        // f << "[" << std::to_string(metric.second->count.load()) << ", "
        //   << std::to_string(metric.second->count.load()) << "]\n";
      }
      // f << metric.second->ToString();
    }
  }
  f.close();
}

}  // namespace aquafs
