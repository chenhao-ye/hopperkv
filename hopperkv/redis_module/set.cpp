#include "set.h"

#include <cassert>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>

#include "config.h"
#include "ghost.h"
#include "inflight.h"
#include "network.h"
#include "redismodule.h"
#include "stats.h"
#include "storage.h"
#include "task.h"
#include "utils.h"

using namespace hopper;

namespace hopper::set {

static int reply_callback(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  assert(argc == 3);
  task::TaskSet *t = static_cast<task::TaskSet *>(
      RedisModule_GetBlockedClientPrivateData(ctx));
  assert(t->type == task::Task::Type::SET);
  assert(t->status != task::Task::Status::NONE);
  if (t->status == task::Task::Status::ERR) {
    // clean up the value. this may have minor inconsistency if another GET sees
    // the value, but it at least guarantees eventual consistency
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    RedisModule_DeleteKey(key);
    RedisModule_CloseKey(key);

    return RedisModule_ReplyWithError(
        ctx, (std::string("ERR Fail to set to DynamoDB: ") + t->value).c_str());
  }
  // acknowledge the write completion
  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

static void free_reply_data(RedisModuleCtx *ctx, void *data) {
  task::TaskSet *t = static_cast<task::TaskSet *>(data);
  assert(t->type == task::Task::Type::SET);
  delete t;
}

} // namespace hopper::set

int RedisModule_HopperSet(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);

  int open_flag =
      REDISMODULE_WRITE |
      (config::cache::admit_write ? 0 : REDISMODULE_OPEN_KEY_NOTOUCH);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], open_flag);

  std::string key_str = utils::rstr::to_cppstr(argv[1]);

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_STRING) {
    // update the value for cache-coherence
    RedisModule_StringSet(key, argv[2]);
    inflight::invalidate_inflight(key_str);
  } else if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    // not exist yet; only admit this write if config::cache::admit_write
    if (config::cache::admit_write) {
      RedisModule_StringSet(key, argv[2]);
      inflight::invalidate_inflight(key_str);
    }
  } else { // type error
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }
  RedisModule_CloseKey(key);

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(
      ctx, set::reply_callback, nullptr, set::free_reply_data, 0);
  auto t = new task::TaskSet(bc, std::move(key_str), argv[2]);

  ghost::access_key(t->key, t->value.size(), /*update_miss_ratio*/ false);

  stats::record_set_done(t->key.size(), t->value.size());

  network::wait_until_can_send();
  auto net_consumption =
      utils::resrc::kv_to_net_set_client(t->key.size(), t->value.size());
  if (config::policy::alloc_total_net_bw)
    net_consumption +=
        utils::resrc::kv_to_net_set_storage(t->key.size(), t->value.size());
  network::consume(net_consumption);

  // write to DynamoDB
  storage::set_async(t);

  return REDISMODULE_OK;
}

// cache-only version of `SET`: only update in-memory cache without writing back
// to DynamoDB (but will still maintain stats as if written to DynamoDB)
int RedisModule_HopperSetC(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);

  if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_STRING ||
      RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_StringSet(key, argv[2]);
  } else { // type error
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }
  RedisModule_CloseKey(key);
  RedisModule_ReplyWithSimpleString(ctx, "OK");

  size_t k_len, v_len;
  const char *k_buf = RedisModule_StringPtrLen(argv[1], &k_len);
  const char *v_buf = RedisModule_StringPtrLen(argv[2], &v_len);

  // only update ghost cache for warmup purpose
  ghost::access_key({k_buf, k_len}, v_len, /*update_miss_ratio*/ false);
  // do not update stats or rate limiter

  return REDISMODULE_OK;
}

// Similar to RedisModule_HopperSetC, but directly load from a csv file
int RedisModule_HopperLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);

  size_t filename_len;
  const char *filename = RedisModule_StringPtrLen(argv[1], &filename_len);

  std::ifstream f(filename);
  if (!f) return RedisModule_ReplyWithError(ctx, "ERR Failed to open file");

  std::string line;
  std::getline(f, line);
  if (line != "key,val_size")
    return RedisModule_ReplyWithError(ctx, "ERR Invalid image file format");

  // process each line
  while (std::getline(f, line)) {
    std::istringstream iss(line);
    std::string key_str, val_size_str;

    // Parse CSV line: key,value
    if (!std::getline(iss, key_str, ','))
      return RedisModule_ReplyWithError(ctx, "ERR Invalid image file format");
    if (!std::getline(iss, val_size_str))
      return RedisModule_ReplyWithError(ctx, "ERR Invalid image file format");
    int val_size = std::stoi(val_size_str);
    std::string val_str(val_size, 'v');

    // create RedisModuleString objects for key and value
    RedisModuleString *key_rstr =
        RedisModule_CreateString(ctx, key_str.c_str(), key_str.size());
    RedisModuleString *val_rstr =
        RedisModule_CreateString(ctx, val_str.c_str(), val_str.size());

    // process like RedisModule_HopperSetC
    RedisModuleKey *key = RedisModule_OpenKey(ctx, key_rstr, REDISMODULE_WRITE);

    bool can_set = RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_STRING ||
                   RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY;
    if (can_set) RedisModule_StringSet(key, val_rstr);

    RedisModule_CloseKey(key);
    RedisModule_FreeString(ctx, key_rstr);
    RedisModule_FreeString(ctx, val_rstr);

    if (!can_set)
      return RedisModule_ReplyWithError(ctx, "ERR Invalid image file format");

    // only update ghost cache for warmup purpose
    ghost::access_key(key_str, val_str.size(), /*update_miss_ratio*/ false);
    // do not update stats nor check rate limiter
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}
