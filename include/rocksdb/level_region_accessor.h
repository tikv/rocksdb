//
// Created by CheneyDing on 2021/3/5.
//

#ifndef ROCKSDB_LEVEL_REGION_ACCESSOR_H
#define ROCKSDB_LEVEL_REGION_ACCESSOR_H

#pragma once

#include <memory>
#include <string>

#include "rocksdb/slice.h"

namespace rocksdb {

class Slice;

struct RegionBoundaries {
  const Slice* smallest_user_key;
  const Slice* largest_user_key;
};

struct AccessorResult {
  const RegionBoundaries* regions;
  int region_count;
};

struct AccessorRequest {
  AccessorRequest(const Slice& smallest_user_key_,
                  const Slice& largest_user_key_,
                  int level_)
      : smallest_user_key(&smallest_user_key_),
        largest_user_key(&largest_user_key_) {}
  const Slice* smallest_user_key;
  const Slice* largest_user_key;
};

class LevelRegionAccessor {
 public:
  virtual ~LevelRegionAccessor() {}

  // Return the name of this accessor.
  virtual const char* Name() const = 0;

  // Return the next region information in this level.
  virtual AccessorResult* LevelRegions(const AccessorRequest& request) const = 0;
};

}

#endif  // ROCKSDB_LEVEL_REGION_ACCESSOR_H
