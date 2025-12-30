#pragma once

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

int RedisModule_HopperStats(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);

#ifdef __cplusplus
}
#endif

namespace hopper::stats {

struct MemStats {
  long long total_allocated;
  long long keys_count;
  long long startup_allocated;
  long long clients_normal;
  long long functions_caches;
  double avg_kv_size;
};

void record_get_done(size_t key_size, size_t val_size, bool is_miss);
void record_set_done(size_t key_size, size_t val_size);

} // namespace hopper::stats
