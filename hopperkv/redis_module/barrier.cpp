#include "barrier.h"

#include <vector>

#include "redismodule.h"

namespace hopper::barrier {

static std::vector<RedisModuleBlockedClient *> waiting_clients;

static int wait_callback(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

} // namespace hopper::barrier

int RedisModule_HopperBarrierWait(RedisModuleCtx *ctx, RedisModuleString **argv,
                                  int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);
  RedisModuleBlockedClient *bc = RedisModule_BlockClient(
      ctx, hopper::barrier::wait_callback, nullptr, nullptr, 0);
  hopper::barrier::waiting_clients.emplace_back(bc);
  return REDISMODULE_OK;
}

int RedisModule_HopperBarrierSignal(RedisModuleCtx *ctx,
                                    RedisModuleString **argv, int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);
  for (auto bc : hopper::barrier::waiting_clients)
    RedisModule_UnblockClient(bc, nullptr);
  hopper::barrier::waiting_clients.clear();
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

int RedisModule_HopperBarrierCount(RedisModuleCtx *ctx,
                                   RedisModuleString **argv, int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);
  RedisModule_ReplyWithLongLong(ctx, hopper::barrier::waiting_clients.size());
  return REDISMODULE_OK;
}
