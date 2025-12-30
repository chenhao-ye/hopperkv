#include "resrc.h"

#include <cassert>

#include "network.h"
#include "redismodule.h"
#include "storage.h"

using namespace hopper;

namespace hopper::resrc {

static struct {
  uint32_t cache_size = 0;
  double db_rcu = 0;
  double db_wcu = 0;
  double net_bw = 0;
} allocated_resrc;

} // namespace hopper::resrc

int RedisModule_HopperResrcGet(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);

  RedisModule_ReplyWithArray(ctx, 4);
  RedisModule_ReplyWithLongLong(ctx, resrc::allocated_resrc.cache_size);
  RedisModule_ReplyWithLongDouble(ctx, resrc::allocated_resrc.db_rcu);
  RedisModule_ReplyWithLongDouble(ctx, resrc::allocated_resrc.db_wcu);
  RedisModule_ReplyWithLongDouble(ctx, resrc::allocated_resrc.net_bw);

  return REDISMODULE_OK;
}

int RedisModule_HopperResrcSet(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc) {
  if (argc != 5) return RedisModule_WrongArity(ctx);

  int ret;
  long long new_cache_size = 0;
  double new_db_rcu = 0, new_db_wcu = 0, new_net_bw = 0;

  ret = RedisModule_StringToLongLong(argv[1], &new_cache_size);
  if (ret == REDISMODULE_ERR) {
    return RedisModule_ReplyWithError(ctx, "ERR Fail to parse <cache_size>");
  }
  ret = RedisModule_StringToDouble(argv[2], &new_db_rcu);
  if (ret == REDISMODULE_ERR) {
    return RedisModule_ReplyWithError(ctx, "ERR Fail to parse <db_rcu>");
  }
  ret = RedisModule_StringToDouble(argv[3], &new_db_wcu);
  if (ret == REDISMODULE_ERR) {
    return RedisModule_ReplyWithError(ctx, "ERR Fail to parse <db_wcu>");
  }
  ret = RedisModule_StringToDouble(argv[4], &new_net_bw);
  if (ret == REDISMODULE_ERR) {
    return RedisModule_ReplyWithError(ctx, "ERR Fail to parse <net_bw>");
  }

  if (new_cache_size >= 0) { // negative means skip
    RedisModuleCallReply *r = RedisModule_Call(ctx, "CONFIG", "ccl", "SET",
                                               "MAXMEMORY", new_cache_size);
    // reply should be "OK"
    if (RedisModule_CallReplyType(r) != REDISMODULE_REPLY_STRING)
      return RedisModule_ReplyWithError(ctx, "ERR Fail to set cache size");
    RedisModule_FreeCallReply(r);
    resrc::allocated_resrc.cache_size = static_cast<uint32_t>(new_cache_size);
  }

  if (new_db_rcu >= 0) { // negative means skip
    storage::set_rcu_limit(new_db_rcu);
    resrc::allocated_resrc.db_rcu = new_db_rcu;
  }

  if (new_db_wcu >= 0) { // negative means skip
    storage::set_wcu_limit(new_db_wcu);
    resrc::allocated_resrc.db_wcu = new_db_wcu;
  }

  if (new_net_bw >= 0) { // negative means skip
    network::set_net_limit(new_net_bw);
    resrc::allocated_resrc.net_bw = new_net_bw;
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}
