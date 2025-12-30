#include "stats.h"

#include <cstdint>

#include "config.h"
#include "ghost.h"
#include "redismodule.h"
#include "utils.h"

using namespace hopper;

namespace hopper::stats {

// closure: only visible within this file and can only be accessed from the
// functions below
static struct {
  uint64_t req_cnt = 0;  // include get and set
  uint64_t hit_cnt = 0;  // only for get
  uint64_t miss_cnt = 0; // only for get

  // for accounting, these fields below should be uint64_t, though we allows
  // allocated_resrc to be double (since they needs to be multiplied time)

  // for demand vector
  uint64_t db_rcu_consump_if_miss = 0; // DynamoDB Read Capacity Unit
  uint64_t net_bw_consump_if_miss = 0; // network bandwidth (unit: bytes)
  uint64_t net_bw_consump_if_hit = 0;  // network bandwidth (unit: bytes)
  // actual consumption
  uint64_t db_rcu_consump = 0; // DynamoDB Read Capacity Unit
  uint64_t db_wcu_consump = 0; // DynaomoDB Write Capacity Unit
  uint64_t net_bw_consump = 0; // network bandwidth (unit: bytes)

  // running average of kv size to detect abnormal bytes_per_key overhead
  double avg_kv_size = 0;
} resrc_stats;

void record_get_done(size_t key_size, size_t val_size, bool is_miss) {
  ++resrc_stats.req_cnt;
  if (is_miss)
    ++resrc_stats.miss_cnt;
  else
    ++resrc_stats.hit_cnt;
  uint64_t db_rcu = utils::resrc::kv_to_rcu(key_size, val_size);
  resrc_stats.db_rcu_consump_if_miss += db_rcu;
  if (is_miss) resrc_stats.db_rcu_consump += db_rcu;

  uint64_t net_bw_client =
      utils::resrc::kv_to_net_get_client(key_size, val_size);
  resrc_stats.net_bw_consump_if_miss += net_bw_client;
  resrc_stats.net_bw_consump_if_hit += net_bw_client;
  resrc_stats.net_bw_consump += net_bw_client;

  if (config::policy::alloc_total_net_bw) {
    uint64_t net_bw_storage =
        utils::resrc::kv_to_net_get_storage(key_size, val_size);
    resrc_stats.net_bw_consump_if_miss += net_bw_storage;
    // net_bw_consump_if_hit += 0
    if (is_miss) resrc_stats.net_bw_consump += net_bw_storage;
  }

  // update running average of kv size
  auto curr_kv_size = key_size + val_size;
  resrc_stats.avg_kv_size =
      resrc_stats.avg_kv_size
          ? resrc_stats.avg_kv_size * config::stats::kv_size_decay_rate +
                curr_kv_size * (1 - config::stats::kv_size_decay_rate)
          : curr_kv_size;
}

void record_set_done(size_t key_size, size_t val_size) {
  ++resrc_stats.req_cnt;
  resrc_stats.db_wcu_consump += utils::resrc::kv_to_wcu(key_size, val_size);

  uint64_t net_bw_client =
      utils::resrc::kv_to_net_set_client(key_size, val_size);
  resrc_stats.net_bw_consump_if_miss += net_bw_client;
  resrc_stats.net_bw_consump_if_hit += net_bw_client;
  resrc_stats.net_bw_consump += net_bw_client;

  if (config::policy::alloc_total_net_bw) {
    uint64_t net_bw_storage =
        utils::resrc::kv_to_net_set_storage(key_size, val_size);
    resrc_stats.net_bw_consump_if_miss += net_bw_storage;
    resrc_stats.net_bw_consump_if_hit += net_bw_storage;
    resrc_stats.net_bw_consump += net_bw_storage;
  }

  if (config::cache::admit_write) {
    // update running average of kv size
    auto curr_kv_size = key_size + val_size;
    resrc_stats.avg_kv_size =
        resrc_stats.avg_kv_size
            ? resrc_stats.avg_kv_size * config::stats::kv_size_decay_rate +
                  curr_kv_size * (1 - config::stats::kv_size_decay_rate)
            : curr_kv_size;
  }
}

int collect_mem_stats(RedisModuleCtx *ctx, MemStats &ms) {
  // retrieve statics
  ms.total_allocated = -1;
  ms.keys_count = -1;
  ms.startup_allocated = -1;
  ms.clients_normal = -1;
  ms.functions_caches = -1;
  ms.avg_kv_size = resrc_stats.avg_kv_size;
  RedisModuleCallReply *r = RedisModule_Call(ctx, "MEMORY", "c", "STATS");
  if (RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ARRAY)
    return RedisModule_ReplyWithError(ctx, "ERR Fail to call <MEMORY STATS>");
  size_t r_len = RedisModule_CallReplyLength(r);
  for (size_t i = 0; i < r_len; ++i) {
    RedisModuleCallReply *sub_r = RedisModule_CallReplyArrayElement(r, i);
    if (RedisModule_CallReplyType(sub_r) != REDISMODULE_REPLY_STRING) continue;

    if (utils::rstr::strcmp(sub_r, "total.allocated") == 0) {
      ++i; // forward to the next element
      assert(i < r_len);
      sub_r = RedisModule_CallReplyArrayElement(r, i);
      assert(RedisModule_CallReplyType(sub_r) == REDISMODULE_REPLY_INTEGER);
      ms.total_allocated = RedisModule_CallReplyInteger(sub_r);
    } else if (utils::rstr::strcmp(sub_r, "keys.count") == 0) {
      ++i; // forward to the next element
      assert(i < r_len);
      sub_r = RedisModule_CallReplyArrayElement(r, i);
      assert(RedisModule_CallReplyType(sub_r) == REDISMODULE_REPLY_INTEGER);
      ms.keys_count = RedisModule_CallReplyInteger(sub_r);
    } else if (utils::rstr::strcmp(sub_r, "startup.allocated") == 0) {
      ++i; // forward to the next element
      assert(i < r_len);
      sub_r = RedisModule_CallReplyArrayElement(r, i);
      assert(RedisModule_CallReplyType(sub_r) == REDISMODULE_REPLY_INTEGER);
      ms.startup_allocated = RedisModule_CallReplyInteger(sub_r);
    } else if (utils::rstr::strcmp(sub_r, "clients.normal") == 0) {
      ++i; // forward to the next element
      assert(i < r_len);
      sub_r = RedisModule_CallReplyArrayElement(r, i);
      assert(RedisModule_CallReplyType(sub_r) == REDISMODULE_REPLY_INTEGER);
      ms.clients_normal = RedisModule_CallReplyInteger(sub_r);
    } else if (utils::rstr::strcmp(sub_r, "functions.caches") == 0) {
      ++i; // forward to the next element
      assert(i < r_len);
      sub_r = RedisModule_CallReplyArrayElement(r, i);
      assert(RedisModule_CallReplyType(sub_r) == REDISMODULE_REPLY_INTEGER);
      ms.functions_caches = RedisModule_CallReplyInteger(sub_r);
    }
  }
  // these fields should be set, but only bytes_overhead must be nonzero
  if (ms.total_allocated < 0)
    return RedisModule_ReplyWithError(ctx,
                                      "ERR Fail to fetch <total.allocated>");
  if (ms.keys_count < 0)
    return RedisModule_ReplyWithError(ctx, "ERR Fail to fetch <keys.count>");
  if (ms.startup_allocated < 0)
    return RedisModule_ReplyWithError(ctx,
                                      "ERR Fail to fetch <startup.allocated>");
  if (ms.clients_normal < 0)
    return RedisModule_ReplyWithError(ctx,
                                      "ERR Fail to fetch <clients.normal>");
  if (ms.functions_caches < 0)
    return RedisModule_ReplyWithError(ctx,
                                      "ERR Fail to fetch <functions.caches>");
  return REDISMODULE_OK;
}

} // namespace hopper::stats

int RedisModule_HopperStats(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);

  stats::MemStats ms{};
  int ret = stats::collect_mem_stats(ctx, ms);
  if (ret != REDISMODULE_OK) return ret;

  RedisModule_ReplyWithArray(ctx, 24);
  // this function reports three stats -> 6 elements in array
  ghost::reply_ghost_stats(ctx, ms);

  RedisModule_ReplyWithSimpleString(ctx, "req_cnt");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.req_cnt);
  RedisModule_ReplyWithSimpleString(ctx, "hit_cnt");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.hit_cnt);
  RedisModule_ReplyWithSimpleString(ctx, "miss_cnt");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.miss_cnt);

  RedisModule_ReplyWithSimpleString(ctx, "db_rcu_consump_if_miss");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.db_rcu_consump_if_miss);
  RedisModule_ReplyWithSimpleString(ctx, "net_bw_consump_if_miss");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.net_bw_consump_if_miss);
  RedisModule_ReplyWithSimpleString(ctx, "net_bw_consump_if_hit");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.net_bw_consump_if_hit);

  RedisModule_ReplyWithSimpleString(ctx, "db_rcu_consump");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.db_rcu_consump);
  RedisModule_ReplyWithSimpleString(ctx, "db_wcu_consump");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.db_wcu_consump);
  RedisModule_ReplyWithSimpleString(ctx, "net_bw_consump");
  RedisModule_ReplyWithLongLong(ctx, stats::resrc_stats.net_bw_consump);

  return REDISMODULE_OK;
}
