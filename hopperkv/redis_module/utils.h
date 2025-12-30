#pragma once

#include <cassert>
#include <cstring>
#include <string>

#include "redismodule.h"

namespace hopper::utils {

namespace rstr { // RedisModuleString helper functions

static inline int strcmp(RedisModuleString* rstr, const char* cstr) {
  size_t len;
  const char* buf = RedisModule_StringPtrLen(rstr, &len);
  return std::strncmp(buf, cstr, len);
}

static inline int strcmp(RedisModuleCallReply* reply, const char* cstr) {
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char* buf = RedisModule_CallReplyStringPtr(reply, &len);
  return std::strncmp(buf, cstr, len);
}

static std::string to_cppstr(RedisModuleString* rstr) {
  size_t len;
  const char* buf = RedisModule_StringPtrLen(rstr, &len);
  return std::string(buf, len);
}

static std::string to_cppstr(RedisModuleCallReply* reply) {
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char* buf = RedisModule_CallReplyStringPtr(reply, &len);
  return std::string(buf, len);
}

} // namespace rstr

namespace resrc {

// temporarily use a naive estimator
// RCU/WCU is accounted based on "item size", which may be larger than
// key_size + val_size due to additional overhead
static uint64_t kv_to_rcu(size_t key_size, size_t val_size) {
  return (key_size + val_size) / 4096 + 1;
}
static uint64_t kv_to_wcu(size_t key_size, size_t val_size) {
  return (key_size + val_size) / 1024 + 1;
}

// network bandwidth between Redis and client for a GET request
static uint64_t kv_to_net_get_client(size_t key_size, size_t val_size) {
  return key_size + val_size;
}
// network bandwidth between Redis and client for a SET request
static uint64_t kv_to_net_set_client(size_t key_size, size_t val_size) {
  return key_size + val_size;
}
// network bandwidth between Redis and DynamoDB for a GET request
static uint64_t kv_to_net_get_storage(size_t key_size, size_t val_size) {
  // DynamoDB returns items for GET, which includes both key and value; for
  // simplicity, we assume only value is returned
  // TODO: DynamoDB supports only return a specific set of attributes
  return key_size + val_size;
}
// network bandwidth between Redis and DynamoDB for a SET request
static uint64_t kv_to_net_set_storage(size_t key_size, size_t val_size) {
  return key_size + val_size;
}
} // namespace resrc

} // namespace hopper::utils
