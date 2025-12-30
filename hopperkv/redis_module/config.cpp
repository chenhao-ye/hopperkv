#include "config.h"

#include <cstring>
#include <stdexcept>
#include <string>

#include "ghost.h"
#include "redismodule.h"
#include "storage.h"
#include "utils.h"

using namespace hopper;

namespace hopper::config { // config name must all be lower letters!

namespace dynamo {
// config name: `dynamo.table`
// TODO: consider the thread safety if modified concurrently
// should be fine if submit to DynamoDB is done by the Redis main thread
std::string table = "hare_table";

// config name: `dynamo.mock`
// only safe to set if there is no inflight requests
bool mock = false;

}; // namespace dynamo

namespace cache {
// config name: `cache.admit_write`
bool admit_write = true;
} // namespace cache

namespace ghost {
// config name: `ghost.range`, followed by `<tick> <min_tick> <max_tick>`
uint32_t tick = 1 << 15;     // 32K keys
uint32_t min_tick = 1 << 15; // 32K keys
uint32_t max_tick = 1 << 20; // 1M keys
} // namespace ghost

} // namespace hopper::config

int RedisModule_HopperConfigGet(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);

  RedisModule_ReplyWithArray(ctx, 12);

  RedisModule_ReplyWithSimpleString(ctx, "policy.alloc_total_net_bw");
  RedisModule_ReplyWithBool(ctx, config::policy::alloc_total_net_bw);

  RedisModule_ReplyWithSimpleString(ctx, "dynamo.table");
  RedisModule_ReplyWithStringBuffer(ctx, config::dynamo::table.data(),
                                    config::dynamo::table.size());
  RedisModule_ReplyWithSimpleString(ctx, "dynamo.mock");
  RedisModule_ReplyWithBool(ctx, config::dynamo::mock);
  storage::reply_mock_format(ctx);

  RedisModule_ReplyWithSimpleString(ctx, "cache.admit_write");
  RedisModule_ReplyWithBool(ctx, config::cache::admit_write);

  RedisModule_ReplyWithSimpleString(ctx, "ghost.range");
  RedisModule_ReplyWithArray(ctx, 3);
  RedisModule_ReplyWithLongLong(ctx, config::ghost::tick);
  RedisModule_ReplyWithLongLong(ctx, config::ghost::min_tick);
  RedisModule_ReplyWithLongLong(ctx, config::ghost::max_tick);

  return REDISMODULE_OK;
}

int RedisModule_HopperConfigSet(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);

  if (utils::rstr::strcmp(argv[1], "dynamo.table") == 0) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    config::dynamo::table = utils::rstr::to_cppstr(argv[2]);
  } else if (utils::rstr::strcmp(argv[1], "dynamo.mock") == 0) {
    // support three options:
    //   HOPPER.CONFIG.SET dynamo.mock disable
    //   HOPPER.CONFIG.SET dynamo.mock image [image_filename]
    //   HOPPER.CONFIG.SET dynamo.mock format [key_size] [val_size]
    if (argc < 3) return RedisModule_WrongArity(ctx);
    if (utils::rstr::strcmp(argv[2], "disable") == 0) {
      if (argc != 3) return RedisModule_WrongArity(ctx);
      config::dynamo::mock = false;
    } else if (utils::rstr::strcmp(argv[2], "image") == 0) {
      storage::init_mock_image();
      for (int i = 3; i < argc; ++i) {
        int rc =
            storage::load_mock_image(utils::rstr::to_cppstr(argv[i]).c_str());
        if (rc == -1)
          return RedisModule_ReplyWithError(ctx,
                                            "ERR Failed to open image file");
        if (rc == -2)
          return RedisModule_ReplyWithError(ctx,
                                            "ERR Invalid image file format");
      }
      config::dynamo::mock = true;
    } else if (utils::rstr::strcmp(argv[2], "format") == 0) {
      if (argc != 5) return RedisModule_WrongArity(ctx);
      long long mock_key_size;
      int ret = RedisModule_StringToLongLong(argv[3], &mock_key_size);
      if (ret == REDISMODULE_ERR)
        return RedisModule_ReplyWithError(
            ctx, "ERR Invalid `key_size` for <dynamo.mock>");

      unsigned long long mock_val_size;
      ret = RedisModule_StringToULongLong(argv[4], &mock_val_size);
      if (ret == REDISMODULE_ERR)
        return RedisModule_ReplyWithError(
            ctx, "ERR Invalid `val_size` for <dynamo.mock>");
      try {
        storage::update_mock_format(static_cast<uint32_t>(mock_key_size),
                                    static_cast<uint32_t>(mock_val_size));
        config::dynamo::mock = true;
      } catch (const std::invalid_argument &e) {
        return RedisModule_ReplyWithError(
            ctx, (std::string("ERR Ill-formed key-value format: ") + e.what())
                     .c_str());
      }
    } else {
      return RedisModule_ReplyWithError(ctx,
                                        "ERR Invalid option for <dynamo.mock>");
    }
  } else if (utils::rstr::strcmp(argv[1], "cache.admit_write") == 0) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    if (utils::rstr::strcmp(argv[2], "true") == 0)
      config::cache::admit_write = true;
    else if (utils::rstr::strcmp(argv[2], "false") == 0)
      config::cache::admit_write = false;
    else
      return RedisModule_ReplyWithError(
          ctx, "ERR Invalid value for <cache.admit_write>");
  } else if (utils::rstr::strcmp(argv[1], "ghost.range") == 0) {
    if (argc != 5) return RedisModule_WrongArity(ctx);

    int ret;
    unsigned long long new_tick = 0, new_min_tick = 0, new_max_tick = 0;

    ret = RedisModule_StringToULongLong(argv[2], &new_tick);
    if (ret == REDISMODULE_ERR)
      return RedisModule_ReplyWithError(
          ctx, "ERR Invalid value for <ghost.range:tick>");
    ret = RedisModule_StringToULongLong(argv[3], &new_min_tick);
    if (ret == REDISMODULE_ERR)
      return RedisModule_ReplyWithError(
          ctx, "ERR Invalid value for <ghost.range:min_tick>");
    ret = RedisModule_StringToULongLong(argv[4], &new_max_tick);
    if (ret == REDISMODULE_ERR)
      return RedisModule_ReplyWithError(
          ctx, "ERR Invalid value for <ghost.range:max_tick>");

    new_tick = ghost::round_tick(new_tick);
    new_min_tick = ghost::round_tick(new_min_tick);
    new_max_tick = ghost::round_tick(new_max_tick);
    // one more rounding
    new_max_tick =
        new_min_tick + (new_max_tick - new_min_tick) / new_tick * new_tick;

    config::ghost::tick = static_cast<uint32_t>(new_tick);
    config::ghost::min_tick = static_cast<uint32_t>(new_min_tick);
    config::ghost::max_tick = static_cast<uint32_t>(new_max_tick);

    ghost::init(); // reinit ghost cache
  } else if (utils::rstr::strcmp(argv[1], "policy.alloc_total_net_bw") == 0) {
    return RedisModule_ReplyWithError(
        ctx, "ERR <policy.alloc_total_net_bw> is not configurable");
  } else {
    return RedisModule_ReplyWithError(ctx, "ERR unrecognized config");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}
