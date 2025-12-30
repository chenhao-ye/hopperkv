#pragma once

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

int RedisModule_HopperResrcGet(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc);
int RedisModule_HopperResrcSet(RedisModuleCtx *ctx, RedisModuleString **argv,
                               int argc);

#ifdef __cplusplus
}
#endif

namespace hopper::resrc {}
