#pragma once

#include <cstdint>
#include <queue>
#include <string>

#include "pthread.h"
#include "redismodule.h"
#include "utils.h"

namespace hopper::task {

struct Task {
  enum class Type : uint8_t { GET, SET };
  enum class Status : uint8_t { NONE, OK, ERR };
  Type type;
  Status status;
  RedisModuleBlockedClient *client;

  Task(Type type, Status status, RedisModuleBlockedClient *client)
      : type(type), status(status), client(client) {}
};

struct TaskGet : Task {
  std::string key;
  std::string value; // result from DynamoDB
  // other GET requests on the same key depends on this task
  std::vector<RedisModuleBlockedClient *> dependents;

  TaskGet(RedisModuleBlockedClient *client, RedisModuleString *key)
      : Task(Type::GET, Status::NONE, client),
        key(utils::rstr::to_cppstr(key)) {}

  TaskGet(RedisModuleBlockedClient *client, std::string &&key)
      : Task(Type::GET, Status::NONE, client), key(std::move(key)) {}

  // should only call upon completion
  uint32_t rcu_cost() {
    return utils::resrc::kv_to_rcu(key.size(), value.size());
  }

  void add_dependent(RedisModuleBlockedClient *bc) {
    dependents.emplace_back(bc);
  }
};

struct TaskSet : Task {
  std::string key;
  std::string value;

  TaskSet(RedisModuleBlockedClient *client, RedisModuleString *key,
          RedisModuleString *value)
      : Task(Type::SET, Status::NONE, client),
        key(utils::rstr::to_cppstr(key)),
        value(utils::rstr::to_cppstr(value)) {}

  TaskSet(RedisModuleBlockedClient *client, std::string &&key,
          RedisModuleString *value)
      : Task(Type::SET, Status::NONE, client),
        key(std::move(key)),
        value(utils::rstr::to_cppstr(value)) {}

  uint32_t wcu_cost() {
    return utils::resrc::kv_to_wcu(key.size(), value.size());
  }
};

template <typename Task_t>
class TaskQueue {
  pthread_spinlock_t queue_lock;
  std::queue<Task_t *> queue;

 public:
  TaskQueue() { pthread_spin_init(&queue_lock, PTHREAD_PROCESS_PRIVATE); }
  ~TaskQueue() { pthread_spin_destroy(&queue_lock); }

  void push(Task_t *t) {
    pthread_spin_lock(&queue_lock);
    queue.push(t);
    pthread_spin_unlock(&queue_lock);
  }

  Task_t *pop() {
    Task_t *t = nullptr;
    pthread_spin_lock(&queue_lock);
    if (!queue.empty()) {
      t = queue.front();
      queue.pop();
    }
    pthread_spin_unlock(&queue_lock);
    return t;
  }
};

} // namespace hopper::task
