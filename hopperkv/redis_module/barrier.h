#pragma once

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

// Redis Command entry point
int RedisModule_HopperBarrierWait(RedisModuleCtx *ctx, RedisModuleString **argv,
                                  int argc);
int RedisModule_HopperBarrierSignal(RedisModuleCtx *ctx,
                                    RedisModuleString **argv, int argc);
int RedisModule_HopperBarrierCount(RedisModuleCtx *ctx,
                                   RedisModuleString **argv, int argc);

#ifdef __cplusplus
}
#endif
