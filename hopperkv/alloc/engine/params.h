#pragma once

#include <cstdint>
#include <limits>

// TODO: use meaningful default parameters value; for now just some randomly
// guested placeholders
namespace hare::params {

// policy flags (everything under `policy::` must be a bool flag)
namespace policy {

// whether allocate total network bandwidth (Redis-client + Redis-DynamoDB)
// if false, only Redis-client network bandwidth
// Note Redis only report Redis-client network bandwidth, so it's purely up to
// the allocator on what to allocate and throttle
extern bool alloc_total_net_bw;

bool get_alloc_total_net_bw();
void set_alloc_total_net_bw(bool policy_alloc_total_net_bw);

} // namespace policy

namespace alloc {

// max number of trading round; will terminate trading if exceeds
constexpr static uint32_t max_trade_round = 10000;

constexpr static double min_improve_ratio_delta = 0.0001;

// stop trading cache for rcu/net if the miss ratio is higher too high
// consideration: tenants may have tail latency constraints, or too
// high miss ratios can cause lower utilization due to low queue depth
constexpr static double max_miss_ratio = 1.0;

// stop trading rcu/net for cache if the miss ratio is lower then the threshold:
// consideration: at that point, the inaccuracy of cache miss ratio estimation
// can be significantly amplified, leading to unstable results
constexpr static double min_miss_ratio = 0;

// unit of cache trading
extern uint64_t cache_delta;

// the least amount of resources a tenant can have
extern uint64_t min_cache_size;
extern double min_db_rcu;
extern double min_db_wcu;
extern double min_net_bw;

void set_cache_delta(uint64_t new_cache_delta);
void set_min_cache_size(uint64_t new_min_cache_size);
void set_min_db_rcu(double new_min_db_rcu);
void set_min_db_wcu(double new_min_db_wcu);
void set_min_net_bw(double new_min_net_bw);

uint64_t get_cache_delta();
uint64_t get_min_cache_size();
double get_min_db_rcu();
double get_min_db_wcu();
double get_min_net_bw();

// memshare-related parameters
namespace memshare {

// ratio of memory must be reserved
constexpr static double reserved_ratio = 0.5;

} // namespace memshare

} // namespace alloc

namespace numeric {
// numeric epilson: due to float-point math issue, if a value is smaller than
// epsilon, we generally consider it as zero
constexpr static double db_rcu_epsilon = 0.0001;
constexpr static double db_wcu_epsilon = 0.0001;
constexpr static double net_bw_epsilon = 0.0001;

// if miss ratio is no larger than this, we consider it as zero miss
constexpr static double epilson = std::numeric_limits<double>::epsilon();

// return these value means abort a trading
constexpr static double relinq_abort_offer = 0;
// returning float_max indicates to abort this deal (use float_max instead of
// double_max to avoid potential overflow/underflow...).
// in other words, this client asks for the bandwidth compensation that no
// one could possibly afford.
constexpr static double compen_abort_offer = std::numeric_limits<float>::max();

}; // namespace numeric

namespace mrc {
// if true, when estimate the miss ratio between size A and B where
// miss_ratio(A) == inf, return inf as the interpolation result
// this enable a conservative estimation to reject dangerous trading
static constexpr bool disable_interpolation_near_inf = false;

// if true, when estimating the miss ratio out of range, return the miss ratio
// of the largest cache size this also enables a conservative estimation to
// reject dangerous trading if false, throw an error
static constexpr bool conservative_estimation_if_out_of_range = true;
} // namespace mrc

#ifndef SPDLOG_ACTIVE_LEVEL
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#endif

void log_params();

} // namespace hare::params
