#include "storage.h"

#include <pthread.h>

#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "config.h"
#include "dynamo.h"
#include "rate.h"
#include "task.h"

namespace hopper::storage {

// currently, we only use DynamoDB as the backend storage
static DynamoDB *db = nullptr;

static rate::RateLimiter<rate::ConcurrentProgress> rcu_rate_limiter(1'000'000);
static rate::RateLimiter<rate::ConcurrentProgress> wcu_rate_limiter(1'000'000);

task::TaskQueue<task::TaskGet> task_get_queue;
task::TaskQueue<task::TaskSet> task_set_queue;

bool storage_thread_running = true;
pthread_t storage_thread;

std::queue<std::pair<
    std::chrono::time_point<std::chrono::high_resolution_clock>, task::Task *> >
    mock_dynamo_queue;

// if mock_image is not nullptr, read from mock_image
std::unordered_map<std::uint32_t, std::uint32_t> *mock_image = nullptr;

// if mock_image is nullptr, return a mocked synthesized key-value pair
// reimplement kv_format.py: deterministically generate mocking key-value pairs
struct {
  uint32_t key_size;
  uint32_t val_size;
  uint32_t size_len;
  uint32_t offset_len;
  uint32_t k_pad_len;
  uint32_t v_pad_len;
} mock_format;

void *storage_thread_main(void *arg);
std::string make_mock_val(const std::string &key);

void init() {
  // default KV format
  update_mock_format(16, 500);

  assert(!db);
  db = new DynamoDB();

  pthread_create(&storage_thread, nullptr, storage_thread_main, nullptr);
}

void destroy() {
  storage_thread_running = false;
  pthread_join(storage_thread, nullptr);

  delete db;
  db = nullptr;

  delete mock_image;
  mock_image = nullptr;
};

void get_async(task::TaskGet *t) {
  assert(db);
  assert(t);
  task_get_queue.push(t);
}

void set_async(task::TaskSet *t) {
  assert(db);
  assert(t);
  task_set_queue.push(t);
}

void process_task_get(task::TaskGet *t) {
  if (config::dynamo::mock) {
    auto ready_ts = std::chrono::high_resolution_clock::now() +
                    std::chrono::duration_cast<
                        std::chrono::high_resolution_clock::duration>(
                        std::chrono::duration<double>(
                            config::dynamo::mock_dynamo_latency_sec));
    mock_dynamo_queue.emplace(ready_ts, t);
    return;
  }

  db->getAsync(
      config::dynamo::table, t->key,
      [t](std::string val) {
        assert(t->type == task::Task::Type::GET);
        assert(t->status == task::Task::Status::NONE);
        t->status = task::Task::Status::OK;
        t->value = std::move(val);
        rcu_rate_limiter.consume(t->rcu_cost() - 1); // 1 RCU prepaid
        RedisModule_UnblockClient(t->client, t);
      },
      [t](std::string err_msg) {
        assert(t->status == task::Task::Status::NONE);
        t->status = task::Task::Status::ERR;
        t->value = "Fail to read from DynamoDB: " + err_msg;
        // 1 RCU will be charged even upon failure (prepaid already)
        RedisModule_UnblockClient(t->client, t);
      });
}

void process_task_set(task::TaskSet *t) {
  if (config::dynamo::mock) {
    mock_dynamo_queue.emplace(
        std::chrono::high_resolution_clock::now() +
            std::chrono::duration_cast<
                std::chrono::high_resolution_clock::duration>(
                std::chrono::duration<double>(
                    config::dynamo::mock_dynamo_latency_sec)),
        t);
    return;
  }

  db->putAsync(
      config::dynamo::table, t->key, t->value,
      [t]() {
        assert(t->type == task::Task::Type::SET);
        assert(t->status == task::Task::Status::NONE);
        t->status = task::Task::Status::OK;
        // no WCU accounting; already done upon submission
        RedisModule_UnblockClient(t->client, t);
      },
      [t](std::string err_msg) {
        assert(t->status == task::Task::Status::NONE);
        t->status = task::Task::Status::ERR;
        t->value = "Fail to write to DynamoDB: " + err_msg;
        // no WCU accounting; already done upon submission
        RedisModule_UnblockClient(t->client, t);
      });
}

// the implementation of mocked DynamoDB is not well optimized, but should be
// sufficient for testing purposes
bool process_mock_dynamo() {
  if (mock_dynamo_queue.empty()) return false;
  auto [ts, t] = mock_dynamo_queue.front();
  if (std::chrono::high_resolution_clock::now() < ts) return false;
  mock_dynamo_queue.pop();

  assert(t->status == task::Task::Status::NONE);
  task::TaskGet *t_get = nullptr;
  task::TaskSet *t_set = nullptr;

  switch (t->type) {
    case task::Task::Type::GET:
      t_get = static_cast<task::TaskGet *>(t);
      if (mock_image) {
        uint32_t key_hash = std::hash<std::string>{}(t_get->key);
        auto iter = mock_image->find(key_hash);
        if (iter != mock_image->end()) {
          t_get->value = std::string((*iter).second, 'v');
          t_get->status = task::Task::Status::OK;
          rcu_rate_limiter.consume(t_get->rcu_cost() - 1); // 1 RCU prepaid
        } else {
          t_get->status = task::Task::Status::ERR;
          t_get->value = "key not found in image";
        }
      } else { // use kv-format to synthesize one
        try {
          t_get->value = std::move(make_mock_val(t_get->key));
          t_get->status = task::Task::Status::OK;
          rcu_rate_limiter.consume(t_get->rcu_cost() - 1); // 1 RCU prepaid
        } catch (const std::invalid_argument &e) {
          t_get->status = task::Task::Status::ERR;
          t_get->value = e.what();
        }
      }
      RedisModule_UnblockClient(t_get->client, t_get);
      break;
    case task::Task::Type::SET:
      t_set = static_cast<task::TaskSet *>(t);
      if (mock_image) { // update mock image with the new size
        uint32_t key_hash = std::hash<std::string>{}(t_set->key);
        (*mock_image)[key_hash] = t_set->value.size();
      }
      t_set->status = task::Task::Status::OK;
      // no WCU accounting; already done upon submission
      RedisModule_UnblockClient(t_set->client, t_set);
      break;
  }
  return true;
}

void *storage_thread_main(void *arg) {
  while (storage_thread_running) {
    bool work_done = false;

    // process mocked DynamoDB requests first, which should be independent from
    // rate limiters
    if (config::dynamo::mock) work_done = process_mock_dynamo();

    // check if rate limiter permits
    auto rcu_wait_time = rcu_rate_limiter.check_wait_time();
    auto wcu_wait_time = wcu_rate_limiter.check_wait_time();

    if (rcu_wait_time <= 0) {
      task::TaskGet *t = task_get_queue.pop();
      if (t) {
        process_task_get(t);
        // RCU accounting happens upon requests completion (only then we know
        // the exact request size); prepaid 1 RCU to prevent the rate limiter
        // permitting a flood of requests before the next completion
        rcu_rate_limiter.consume(1);
        work_done = true;
      }
    }
    if (wcu_wait_time <= 0) {
      task::TaskSet *t = task_set_queue.pop();
      if (t) {
        process_task_set(t);
        // WCU accounting can happen immediately because we know the size
        wcu_rate_limiter.consume(t->wcu_cost());
        work_done = true;
      }
    }

    if (!work_done) { // sleep if there is nothing to do
      if (rcu_wait_time > 0 && wcu_wait_time > 0) { //
        std::this_thread::sleep_for(std::chrono::duration<double>(
            std::min({rcu_wait_time, wcu_wait_time,
                      config::dynamo::storage_thread_poll_freq_sec})));
      } else {
        std::this_thread::sleep_for(std::chrono::duration<double>(
            config::dynamo::storage_thread_poll_freq_sec));
      }
    }
  }
  return nullptr;
}

void set_rcu_limit(double db_rcu) { rcu_rate_limiter.propose_new_rate(db_rcu); }
void set_wcu_limit(double db_wcu) { wcu_rate_limiter.propose_new_rate(db_wcu); }

void init_mock_image() {
  if (!mock_image) // lazily created
    mock_image = new std::unordered_map<std::uint32_t, std::uint32_t>;
  // once done, will no longer use pre-defined format to generate mock data
}

int load_mock_image(const char *image_filename) {
  std::ifstream f(image_filename);
  if (!f) return -1;
  std::string line;

  // header
  std::getline(f, line);
  if (line != "key,val_size") return -2;

  while (std::getline(f, line)) {
    std::istringstream iss(line);
    std::string key, val_size_str;
    if (!std::getline(iss, key, ',')) return -2;
    if (!std::getline(iss, val_size_str)) return -2;
    uint32_t val_size = static_cast<uint32_t>(std::stoul(val_size_str));
    uint32_t key_hash = std::hash<std::string>{}(key);
    // for space-efficiency, we only store the hash of the key
    (*mock_image)[key_hash] = val_size;
  }
  return 0;
}

void update_mock_format(uint32_t key_size, uint32_t val_size) {
  uint32_t size_len = std::max(std::to_string(key_size).length(),
                               std::to_string(val_size).length());
  int32_t least_len_left = std::min(key_size, val_size) - 3 - size_len;
  if (least_len_left <= 0)
    throw std::invalid_argument("Incorrect KV format: least_len_left <= 0");
  uint32_t offset_len = std::min(least_len_left, 10);
  int32_t k_pad_len = key_size - 3 - size_len - offset_len;
  int32_t v_pad_len = val_size - 3 - size_len - offset_len;
  if (k_pad_len < 0)
    throw std::invalid_argument("Incorrect KV format: k_pad_len < 0");
  if (v_pad_len < 0)
    throw std::invalid_argument("Incorrect KV format: v_pad_len < 0");

  // now accept all changes
  mock_format.key_size = key_size;
  mock_format.val_size = val_size;
  mock_format.size_len = size_len;
  mock_format.offset_len = offset_len;
  mock_format.k_pad_len = k_pad_len;
  mock_format.v_pad_len = v_pad_len;
}

std::string make_mock_val(const std::string &key) {
  if (key.size() != mock_format.key_size)
    throw std::invalid_argument("Incorrect KV format: key length mismatch: " +
                                key);
  if (key[0] != 'K')
    throw std::invalid_argument(
        "Invalid key format: leading char must be 'K': " + key);

  int offset = 0;
  for (int i = 1; i < key.size(); ++i) {
    if (key[i] == 's') break;
    if (key[i] >= '0' && key[i] <= '9')
      offset = offset * 10 + (key[i] - '0');
    else
      throw std::invalid_argument(
          "Invalid key format: non-digit char found in offset: " + key);
  }

  std::ostringstream oss;
  oss << 'V' << std::setw(mock_format.offset_len) << std::setfill('0') << offset
      << 's' << std::setw(mock_format.size_len) << std::setfill('0')
      << mock_format.val_size << std::string(mock_format.v_pad_len, 'A') << 'L';
  return oss.str();
}

void reply_mock_format(RedisModuleCtx *ctx) {
  RedisModule_ReplyWithSimpleString(ctx, "dynamo.mock_format");
  RedisModule_ReplyWithArray(ctx, 6);
  RedisModule_ReplyWithLongLong(ctx, mock_format.key_size);
  RedisModule_ReplyWithLongLong(ctx, mock_format.val_size);
  RedisModule_ReplyWithLongLong(ctx, mock_format.size_len);
  RedisModule_ReplyWithLongLong(ctx, mock_format.offset_len);
  RedisModule_ReplyWithLongLong(ctx, mock_format.k_pad_len);
  RedisModule_ReplyWithLongLong(ctx, mock_format.v_pad_len);
}

} // namespace hopper::storage
