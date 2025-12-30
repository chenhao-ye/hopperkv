#include "get.h"

#include <cassert>
#include <cstddef>

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

namespace hopper::get {

static int storage_callback(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc) {
  assert(argc == 2);
  task::TaskGet *t = static_cast<task::TaskGet *>(
      RedisModule_GetBlockedClientPrivateData(ctx));
  assert(t->type == task::Task::Type::GET);
  assert(t->status != task::Task::Status::NONE);

  bool update_cache = inflight::end_inflight(t->key, t);

  if (t->status == task::Task::Status::ERR) {
    for (auto bc : t->dependents) RedisModule_UnblockClient(bc, nullptr);
    return RedisModule_ReplyWithError(ctx, "ERR Fail to get from DynamoDB");
  }

  if (update_cache) {
    RedisModuleString *s =
        RedisModule_CreateString(ctx, t->value.data(), t->value.size());
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    RedisModule_StringSet(key, s);
    RedisModule_CloseKey(key);

    // According to
    // https://github.com/redis/redis/blob/64546d20093b585143593e3728727164855fd64a/tests/modules/stream.c#L26-L27,
    // it should be safe to free string after adding reply
    RedisModule_FreeString(ctx, s);
    ghost::update_kv_size(t->key, t->value.size());
  }
  // else: a concurrent SET makes this value stale; do not update the cache!

  RedisModule_ReplyWithStringBuffer(ctx, t->value.data(), t->value.size());

  for (auto bc : t->dependents)
    RedisModule_UnblockClient(bc, new std::string(t->value));

  stats::record_get_done(t->key.size(), t->value.size(), true);

  network::wait_until_can_send();
  auto net_consumption =
      utils::resrc::kv_to_net_get_client(t->key.size(), t->value.size());
  if (config::policy::alloc_total_net_bw)
    net_consumption +=
        utils::resrc::kv_to_net_get_storage(t->key.size(), t->value.size());
  network::consume(net_consumption);

  return REDISMODULE_OK;
}

static void free_storage_callback_data(RedisModuleCtx *ctx, void *data) {
  task::TaskGet *t = static_cast<task::TaskGet *>(data);
  assert(t->type == task::Task::Type::GET);
  delete t;
}

static int inflight_callback(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
  assert(argc == 2);
  std::string *s =
      static_cast<std::string *>(RedisModule_GetBlockedClientPrivateData(ctx));
  if (!s)
    return RedisModule_ReplyWithError(ctx, "ERR Fail to get from DynamoDB");

  // will not update cache because it is a dependent of an flight request, whose
  // storage_callback should have already updated the cache.
  RedisModule_ReplyWithStringBuffer(ctx, s->data(), s->size());

  size_t k_len;
  RedisModule_StringPtrLen(argv[1], &k_len);

  stats::record_get_done(k_len, s->size(), /*is_miss*/ false);

  network::wait_until_can_send();
  network::consume(utils::resrc::kv_to_net_get_client(k_len, s->size()));

  return REDISMODULE_OK;
}

static void free_inflight_callback_data(RedisModuleCtx *ctx, void *data) {
  std::string *s = static_cast<std::string *>(data);
  delete s;
}

} // namespace hopper::get

int RedisModule_HopperGet(RedisModuleCtx *ctx, RedisModuleString **argv,
                          int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);

  // maintain ghost cache
  size_t k_len;
  const char *k_buf = RedisModule_StringPtrLen(argv[1], &k_len);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

  if (!key) { // key doesn't exist -> miss
    RedisModule_CloseKey(key);

    std::string key_str(k_buf, k_len);
    // touch the ghost cache LRU (the real kv size will be updated in callback)
    ghost::access_key(key_str, /*val_size*/ 0, /*update_miss_ratio*/ true);

    bool has_inflight = inflight::check_inflight(key_str);
    if (has_inflight) {
      RedisModuleBlockedClient *bc =
          RedisModule_BlockClient(ctx, get::inflight_callback, nullptr,
                                  get::free_inflight_callback_data, 0);
      // find an inflight request on the same key; do not submit to storage
      // instead, wait for that inflight request to complete
      inflight::add_dependent(key_str, bc);
    } else {
      RedisModuleBlockedClient *bc =
          RedisModule_BlockClient(ctx, get::storage_callback, nullptr,
                                  get::free_storage_callback_data, 0);
      auto t = new task::TaskGet(bc, std::move(key_str));
      inflight::begin_inflight(t->key, t);
      storage::get_async(t);
    }
    return REDISMODULE_OK;
  }
  // key exists -> hit

  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING) { // type error
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  // directly read and reply
  size_t v_len;
  char *v_buf = RedisModule_StringDMA(key, &v_len, REDISMODULE_READ);
  assert(v_buf);
  RedisModule_ReplyWithStringBuffer(ctx, v_buf, v_len);
  RedisModule_CloseKey(key);

  ghost::access_key({k_buf, k_len}, v_len, /*update_miss_ratio*/ true);
  stats::record_get_done(k_len, v_len, /*is_miss*/ false);

  network::wait_until_can_send();
  network::consume(utils::resrc::kv_to_net_get_client(k_len, v_len));

  return REDISMODULE_OK;
}
