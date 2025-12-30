#include "ghost.h"

#include <cmath>
#include <cstdint>
#include <fstream>

#include "config.h"
#include "gcache/ghost_kv_cache.h"
#include "stats.h"

using namespace hopper;
namespace heuristic = config::ghost::heuristic;

namespace hopper::ghost {

// Forward declaration
namespace mem_estimate {
uint32_t estimate(uint32_t key_size, uint32_t val_size);
}

constexpr static uint32_t ghost_sample_shift = 5;
// block_id used in ghost cache is already a hash, so use the identical mapping
// as the underlying cache
static gcache::SampledGhostKvCache<ghost_sample_shift> *ghost_cache = nullptr;

constexpr static const char *ckpt_filename = "dump.ghc";
int save();
int load();

void init() {
  destroy(); // we may re-init after `HOPPER.CONFIG.SET ghost.range`
  ghost_cache = new gcache::SampledGhostKvCache<ghost_sample_shift>(
      config::ghost::tick, config::ghost::min_tick, config::ghost::max_tick);
  int rc = load(); // best-effort; may fail if no ckpt file exist
  if (rc == -2) {  // -2 means a ckpt file is detected but incompatible
    RedisModule_Log(
        nullptr, "warning",
        "Detect incompatible dump.ghc; likely the checkpoint was produced by "
        "another platform with different gcache::gshash implementation "
        "OR the file was corrupted; will abort");
    throw std::runtime_error("Incompatible ghost cache checkpoint");
  }
}

void destroy() {
  delete ghost_cache;
  ghost_cache = nullptr;
}

void access_key(const std::string_view key, uint32_t val_size,
                bool update_miss_ratio) {
  ghost_cache->access(key, mem_estimate::estimate(key.size(), val_size),
                      update_miss_ratio ? gcache::AccessMode::DEFAULT
                                        : gcache::AccessMode::NOOP);
}

void update_kv_size(const std::string_view key, uint32_t val_size) {
  ghost_cache->update_size(key, mem_estimate::estimate(key.size(), val_size));
}

void reply_ghost_stats(RedisModuleCtx *ctx, stats::MemStats &ms) {
  if (ms.keys_count == 0) {
    RedisModule_ReplyWithSimpleString(ctx, "ghost.ticks");
    RedisModule_ReplyWithNull(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "ghost.miss_cnt");
    RedisModule_ReplyWithNull(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "ghost.hit_cnt");
    RedisModule_ReplyWithNull(ctx);
    return;
  }

  auto curve = ghost_cache->get_cache_stat_curve();
  if (curve.empty()) {
    RedisModule_ReplyWithSimpleString(ctx, "ghost.ticks");
    RedisModule_ReplyWithNull(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "ghost.miss_cnt");
    RedisModule_ReplyWithNull(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "ghost.hit_cnt");
    RedisModule_ReplyWithNull(ctx);
    return;
  }
  uint32_t len = curve.size();

  uint64_t bytes_startup =
      ms.startup_allocated + ms.clients_normal + ms.functions_caches;
  if (heuristic::calib_small_cache && // check whether is small cache
      ms.total_allocated - bytes_startup < heuristic::small_cache_threshold) {
    // avoid underflow
    if (bytes_startup + heuristic::small_cache_overhead < ms.total_allocated)
      bytes_startup += heuristic::small_cache_overhead;
  } else if (heuristic::calib_fixed) {
    // avoid underflow
    if (bytes_startup + heuristic::mem_fixed_overhead < ms.total_allocated) {
      bytes_startup += heuristic::mem_fixed_overhead;
    }
  }

  double bytes_per_key =
      double(ms.total_allocated - bytes_startup) / ms.keys_count;

  if constexpr (heuristic::calib_abnormal) {
    // detect abnormal bytes_per_key overhead
    if (ms.total_allocated < heuristic::min_total_allocated &&
        bytes_per_key > ms.avg_kv_size + heuristic::max_per_key_overhead) {
      // fall back to avg_kv_size for calibration
      bytes_per_key = ms.avg_kv_size + heuristic::max_per_key_overhead;
      bytes_startup = ms.total_allocated - bytes_per_key * ms.keys_count;
    }
  }

  uint64_t acc_cnt;   // total access counter
  double mem_amplify; // memory amplify factor (=total_mem_cost / total_kv_cost)
  {
    auto [first_count, first_size, first_cache_stat] = curve[0];
    acc_cnt = first_cache_stat.hit_cnt + first_cache_stat.miss_cnt;
    mem_amplify = bytes_per_key / (double(first_size) / first_count);
  }

  for (auto [c_count, c_size, _] : curve) {
    if (ms.keys_count > c_count) break;
    mem_amplify = bytes_per_key / (double(c_size) / c_count);
  }

  std::vector<uint64_t> hit_cnt_list, miss_cnt_list;
  hit_cnt_list.reserve(len);
  miss_cnt_list.reserve(len);

  // report ticks
  RedisModule_ReplyWithSimpleString(ctx, "ghost.ticks");
  RedisModule_ReplyWithArray(ctx, len + 1);
  RedisModule_ReplyWithLongLong(ctx, bytes_startup);
  for (auto [mrc_count, mrc_size, cache_stat] : curve) {
    auto data_mem = mrc_size * mem_amplify;
    auto mem = data_mem + bytes_startup;
    if constexpr (heuristic::calib_small_cache) {
      if (data_mem < heuristic::small_cache_threshold) {
        // replace fixed overhead (if any) with small cache overhead
        mem += heuristic::small_cache_overhead;
        if (heuristic::calib_fixed) mem -= heuristic::mem_fixed_overhead;
      }
    }
    RedisModule_ReplyWithLongLong(ctx, mem);
    hit_cnt_list.emplace_back(cache_stat.hit_cnt);
    miss_cnt_list.emplace_back(cache_stat.miss_cnt);
  }

  RedisModule_ReplyWithSimpleString(ctx, "ghost.hit_cnt");
  RedisModule_ReplyWithArray(ctx, len + 1);
  RedisModule_ReplyWithLongLong(ctx, 0);
  for (auto hit_cnt : hit_cnt_list) RedisModule_ReplyWithLongLong(ctx, hit_cnt);

  RedisModule_ReplyWithSimpleString(ctx, "ghost.miss_cnt");
  RedisModule_ReplyWithArray(ctx, len + 1);
  RedisModule_ReplyWithLongLong(ctx, acc_cnt);
  for (uint64_t miss_cnt : miss_cnt_list)
    RedisModule_ReplyWithLongLong(ctx, miss_cnt);
}

uint32_t round_tick(uint32_t tick) {
  return (tick >> ghost_sample_shift) << ghost_sample_shift;
}

int save() { // -1 for file open failure; 0 for success
  assert(ghost::ghost_cache);
  // ideally, we should create a temp file and rename it to avoid corrupting the
  // dump file upon failure, but we actually don't care much about integrity of
  // the ghost cache (it's cache afterall)
  std::ofstream f(ckpt_filename, std::ios::out | std::ios::binary);
  if (!f) return -1;

  // first write a 8-byte header
  // checkpoint only works if using the same hash function; when loading a
  // checkpoint (maybe produced by another machine), validating header_hash
  char header_str[5] = "hare";
  uint32_t header_hash = gcache::gshash{}("hare"); // can be platform-dependent
  f.write(header_str, 4);
  f.write(reinterpret_cast<char *>(&header_hash), sizeof(header_hash));

  ghost::ghost_cache->for_each_lru(
      [&f](const gcache::SampledGhostKvCache<ghost_sample_shift>::Handle_t h) {
        struct {
          uint32_t key_hash;
          uint32_t kv_size;
        } buf;
        buf.key_hash = h.get_key();
        buf.kv_size = h->kv_size;
        f.write(reinterpret_cast<char *>(&buf), sizeof(buf));
      });
  f.flush();
  // for similar reasons, we should call fsync, but fine if not because we can
  // tolerate the corruption
  return 0;
}

int load() { // -1 for file open failure; 0 for success
  assert(ghost::ghost_cache);
  std::ifstream f(ckpt_filename, std::ios::in | std::ios::binary);
  if (!f) return -1;

  // validate the header
  char header_str[5] = {};
  f.read(header_str, 4);
  if (strcmp(header_str, "hare") != 0) return -2;

  uint32_t header_hash = 0;
  f.read(reinterpret_cast<char *>(&header_hash), sizeof(header_hash));
  if (gcache::gshash{}("hare") != header_hash) return -2;

  struct {
    uint32_t key_hash;
    uint32_t kv_size;
  } buf;

  while (f.read(reinterpret_cast<char *>(&buf), sizeof(buf)))
    ghost::ghost_cache->access(buf.key_hash, buf.kv_size,
                               gcache::AccessMode::NOOP);
  return 0;
}

// convert key_size and val_size to Redis memory cost
// take jemalloc class allocation into consideration (based on profiling)
namespace mem_estimate {

// round size based on jemalloc slab allocator
uint32_t round_size(uint32_t s) {
  // common case: quick lookup
  if (s <= 4) return 4;
  if (s <= 60) return (s + 3) / 8 * 8 + 4;
  if (s <= 124) return (s + 3) / 16 * 16 + 12;
  if (s <= 252) return (s + 3) / 32 * 32 + 28;
  if (s <= 508) return (s + 3) / 64 * 64 + 60;
  if (s <= 1020) return (s + 3) / 128 * 128 + 124;
  if (s <= 2044) return (s + 3) / 256 * 256 + 252;
  if (s <= 4092) return (s + 3) / 512 * 512 + 508;
  // larger range fall back to more general rounding

  // The pattern:
  // for divisor = 2^power, upper_bound = divisor * 8 - 4, offset = divisor - 4
  uint32_t power = 3; // Start with 2^3 = 8
  while (true) {
    uint32_t divisor = 1 << power; // 2^power using bit shift
    uint32_t upper_bound = divisor * 8 - 4;
    if (s <= upper_bound) {
      uint32_t offset = divisor - 4;
      return (s + 3) / divisor * divisor + offset;
    }
    power++;
  }
}

uint32_t estimate(uint32_t key_size, uint32_t val_size) {
  // 55 is based on offline profiling and linear regression
  static constexpr uint32_t fixed_cost = 55;
  return fixed_cost + round_size(key_size) + round_size(val_size);
}

} // namespace mem_estimate

} // namespace hopper::ghost

int RedisModule_HopperGhostSave(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);
  int rc = ghost::save();
  if (rc)
    return RedisModule_ReplyWithError(ctx, "ERR Fail to open dump.ghc file");
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

int RedisModule_HopperGhostLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);
  int rc = ghost::load();
  if (rc == -1)
    return RedisModule_ReplyWithError(ctx, "ERR Fail to open dump.ghc file");
  if (rc == -2)
    return RedisModule_ReplyWithError(ctx, "ERR Detect incompatible dump.ghc");
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}
