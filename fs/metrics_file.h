//
// Created by chiro on 23-6-4.
//

#ifndef ROCKSDB_METRICS_FILE_H
#define ROCKSDB_METRICS_FILE_H

#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "aquafs_namespace.h"
#include "metrics.h"
#include "metrics_prometheus.h"

namespace AQUAFS_NAMESPACE {

class AquaFSFileMetrics : public AquaFSPrometheusMetrics {
 public:
  std::filesystem::path dist_file;
  std::map<AquaFSMetricsHistograms, std::vector<std::pair<float, std::string>>>
      data;
  explicit AquaFSFileMetrics(const std::filesystem::path& distFile);
  ~AquaFSFileMetrics() override;
}

}  // namespace AQUAFS_NAMESPACE

#endif  // ROCKSDB_METRICS_FILE_H
