#pragma once

#include <string>

#include "redismodule.h"

namespace hopper::config {

namespace policy {
// whether allocate total network bandwidth (Redis-client + Redis-DynamoDB)
// if false, only Redis-client network bandwidth
// Note Redis only report Redis-client network bandwidth, so it's purely up to
// the allocator on what to allocate and throttle
static constexpr bool alloc_total_net_bw = true;
} // namespace policy

namespace dynamo {
// config name: `dynamo.table`
// the name of the DynamoDB table to read/write data
extern std::string table;

// config name: `dynamo.mock`
// whether actually get/set data from DynamoDB; if false, will return fake data
extern bool mock;

// not configurable for now
static constexpr double mock_dynamo_latency_sec = 0.005; // 5 ms

static constexpr double storage_thread_poll_freq_sec = 0.001; // 1 ms

} // namespace dynamo

namespace cache {
// config name: `cache.admit_write`
// whether admit `SET` data into the cache if the key is not presented in the
// cache (after writing to DynamoDB); setting flag as true means we believe a
// freshly written value is likely to be read soon
extern bool admit_write;

// whether enable inflight deduplication
// if true, multiple requests for the same key will be deduplicated to a single
// request to DynamoDB, and all clients will be unblocked when the DynamoDB
// request completes
constexpr static bool enable_inflight_dedup = true;
} // namespace cache

namespace ghost {
// config name: `ghost.range`, followed by `<tick> <min_tick> <max_tick>`
extern uint32_t tick;
extern uint32_t min_tick;
extern uint32_t max_tick;

namespace heuristic {
// profiling-based calibration parameters for memory estimation heuristic

static constexpr bool calib_fixed = true;
static constexpr bool calib_abnormal = true;
static constexpr bool calib_small_cache = false;

// threshold to use avg_kv_size instead of memory stats for ghost ticks
static constexpr uint32_t min_total_allocated = 20 * 1024 * 1024;
static constexpr uint32_t max_per_key_overhead = 300;

/**
 * We model the relation of total memory and key count as the heuristic below:
 *     total_memory = base_overhead + bytes_per_key * keys_count
 * where base_overhead = startup.allocated + clients.normal + functions.caches
 *                       + mem_fixed_overhead
 * This model works in general, except for very small cache, where there is
 * additional overhead. In those cases, additional calibration is needed.
 */
static constexpr uint32_t mem_fixed_overhead = 1 * 1024 * 1024; // 1 MB

// if a cache is very small, typically there is additional overhead that cannot
// be amortized to each key
static constexpr uint32_t small_cache_threshold = 4 * 1024 * 1024; // 4 MB

static constexpr uint32_t small_cache_overhead = 2 * 1024 * 1024; // 2 MB

} // namespace heuristic

} // namespace ghost

namespace stats {
// decay rate when computiong run-average of key-value size
constexpr double kv_size_decay_rate = 0.99;
} // namespace stats

} // namespace hopper::config

#ifdef __cplusplus
extern "C" {
#endif

int RedisModule_HopperConfigGet(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
int RedisModule_HopperConfigSet(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);

#ifdef __cplusplus
}
#endif
