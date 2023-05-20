//
// Created by chiro on 23-5-20.
//

#include "include/convenience.h"

#include "include/object_registry.h"

namespace aquafs {

ConfigOptions::ConfigOptions()
    : registry(ObjectRegistry::NewInstance()), env(Env::Default()) {}
ConfigOptions::ConfigOptions(const DBOptions& db_opts)
    : env(db_opts.env), registry(ObjectRegistry::NewInstance()) {}

class MyEnv : public Env {

};

Env* Env::Default() {
  static MyEnv* env_singleton = nullptr;
  if (!env_singleton) {
    env_singleton = new MyEnv();
  }
  return env_singleton;
}

};  // namespace aquafs