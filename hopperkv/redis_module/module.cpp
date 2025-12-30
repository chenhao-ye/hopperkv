#include "module.h"

#include <pthread.h>
#include <unistd.h>

#include "barrier.h"
#include "config.h"
#include "get.h"
#include "ghost.h"
#include "redismodule.h"
#include "resrc.h"
#include "set.h"
#include "stats.h"
#include "storage.h"

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);

  if (RedisModule_Init(ctx, "hopper", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.GET", RedisModule_HopperGet,
                                "write", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.SET", RedisModule_HopperSet,
                                "write", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.SETC", RedisModule_HopperSetC,
                                "write", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.LOAD", RedisModule_HopperLoad,
                                "write", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.STATS", RedisModule_HopperStats,
                                "admin", 0, 0, 0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.RESRC.GET",
                                RedisModule_HopperResrcGet, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.RESRC.SET",
                                RedisModule_HopperResrcSet, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.CONFIG.GET",
                                RedisModule_HopperConfigGet, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.CONFIG.SET",
                                RedisModule_HopperConfigSet, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.GHOST.SAVE",
                                RedisModule_HopperGhostSave, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.GHOST.LOAD",
                                RedisModule_HopperGhostLoad, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.BARRIER.WAIT",
                                RedisModule_HopperBarrierWait, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.BARRIER.SIGNAL",
                                RedisModule_HopperBarrierSignal, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "HOPPER.BARRIER.COUNT",
                                RedisModule_HopperBarrierCount, "admin", 0, 0,
                                0) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  hopper::ghost::init();
  hopper::storage::init(); // initialize DynamoDB connector

  return REDISMODULE_OK;
}

void RedisModule_OnUnload(RedisModuleCtx *ctx) {
  REDISMODULE_NOT_USED(ctx);
  hopper::ghost::destroy();
  hopper::storage::destroy();
}
