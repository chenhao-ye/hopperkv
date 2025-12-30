#include "params.h"

#include "spdlog/spdlog.h"

namespace hare::params {

namespace policy {

// whether allocate total network bandwidth (Redis-client + Redis-DynamoDB)
// if false, only Redis-client network bandwidth
// Note Redis only report Redis-client network bandwidth, so it's purely up to
// the allocator on what to allocate and throttle
bool alloc_total_net_bw = true;
// NOTE: this can be overwritten at runtime (based on Redis Module's config)

bool get_alloc_total_net_bw() { return alloc_total_net_bw; }

void set_alloc_total_net_bw(bool policy_alloc_total_net_bw) {
  SPDLOG_INFO("hare::params::policy {{ alloc_total_net_bw={} -> {} }}",
              alloc_total_net_bw, policy_alloc_total_net_bw);
  alloc_total_net_bw = policy_alloc_total_net_bw;
}

} // namespace policy

namespace alloc {

uint64_t cache_delta = 4 * 1024 * 1024; // 4 MB

uint64_t min_cache_size = 4 * 1024 * 1024; // 4 MB
double min_db_rcu = 10;
double min_db_wcu = 10;
double min_net_bw = 80 * 1024; // 80 KB/s

void set_cache_delta(uint64_t new_cache_delta) {
  SPDLOG_INFO("hare::params::alloc {{ cache_delta={} -> {} }}", cache_delta,
              new_cache_delta);
  cache_delta = new_cache_delta;
}

void set_min_cache_size(uint64_t new_min_cache_size) {
  SPDLOG_INFO("hare::params::alloc {{ min_cache_size={} -> {} }}",
              min_cache_size, new_min_cache_size);
  min_cache_size = new_min_cache_size;
}

void set_min_db_rcu(double new_min_db_rcu) {
  SPDLOG_INFO("hare::params::alloc {{ min_db_rcu={} -> {} }}", min_db_rcu,
              new_min_db_rcu);
  min_db_rcu = new_min_db_rcu;
}

void set_min_db_wcu(double new_min_db_wcu) {
  SPDLOG_INFO("hare::params::alloc {{ min_db_wcu={} -> {} }}", min_db_wcu,
              new_min_db_wcu);
  min_db_wcu = new_min_db_wcu;
}

void set_min_net_bw(double new_min_net_bw) {
  SPDLOG_INFO("hare::params::alloc {{ min_net_bw={} -> {} }}", min_net_bw,
              new_min_net_bw);
  min_net_bw = new_min_net_bw;
}

uint64_t get_cache_delta() { return cache_delta; }
uint64_t get_min_cache_size() { return min_cache_size; }
double get_min_db_rcu() { return min_db_rcu; }
double get_min_db_wcu() { return min_db_wcu; }
double get_min_net_bw() { return min_net_bw; }

} // namespace alloc

void log_params() {
  SPDLOG_INFO(
      "hare::params::alloc {{ "
      "cache_delta={}, "
      "max_trade_round={}, "
      "min_improve_ratio_delta={}, "
      "min_cache_size={}, "
      "min_db_rcu={}, "
      "min_db_wcu={}, "
      "min_net_bw={} "
      "}}",
      alloc::cache_delta, alloc::max_trade_round,
      alloc::min_improve_ratio_delta, alloc::min_cache_size, alloc::min_db_rcu,
      alloc::min_db_wcu, alloc::min_net_bw);
}

} // namespace hare::params
