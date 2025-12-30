#pragma once

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

// Redis Command entry point
int RedisModule_HopperSet(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc);
int RedisModule_HopperSetC(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
int RedisModule_HopperLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);

#ifdef __cplusplus
}
#endif
