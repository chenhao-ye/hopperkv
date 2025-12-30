// gcache connector
#pragma once

#include <cstdint>
#include <string_view>

#include "redismodule.h"
#include "stats.h"

namespace hopper::ghost {

void init();
void destroy();

void access_key(const std::string_view key, uint32_t val_size,
                bool update_miss_ratio);

void update_kv_size(const std::string_view key, uint32_t val_size);

// this function reply three stats: ghost.{ticks, miss_cnt, hit_cnt}
void reply_ghost_stats(RedisModuleCtx *ctx, stats::MemStats &ms);

// helper to round tick (to be compatible with ghost cache sampling rate)
uint32_t round_tick(uint32_t tick);

} // namespace hopper::ghost

#ifdef __cplusplus
extern "C" {
#endif

int RedisModule_HopperGhostSave(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
int RedisModule_HopperGhostLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
#ifdef __cplusplus
}
#endif
