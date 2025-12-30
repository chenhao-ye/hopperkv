#pragma once

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <string>
#include <tuple>

#include "params.h"

namespace hare {

/**
 * Problem Model:
 * Suppose for a resource type R, a request consumes d units upon a cache miss
 * and (1 - alpha) * d units upon a cache hit (0 <= alpha <= 1):
 *   - alpha = 0     -> cache-independent resource
 *   - alpha = 1     -> fully dependent resource, e.g., I/O
 *   - 0 < alpha < 1 -> weakly dependent resource, e.g., network bandwidth
 *
 * Suppose a tenant has r units of resource R with miss ratio m. If given cache
 * Delta_c, the miss ratio becomes (m - Delta_m). To maintain the same
 * throughput, the allocation of R becomes (r - Delta_r).
 *   Delta_r = r * (alpha * Delta_m) / (1 - alpha + alpha * m)
 *            = r * Delta_m / (((1 - alpha) / alpha) + m)) // if alpha > 0
 * Then
 *   - alpha = 0   -> Delta_r = 0
 *   - alpha = 1   -> Delta_r = r * Delta_m / m
 *   - alpha = 0.5 -> Delta_r = r * Delta_m / (1 + m)
 */

/**
 * Resource Model:
 * - read request: upon a cache hit, only consumes net_bw (to client);
 *     otherwise, consumes db_rcu and net_bw (to client + to DynamoDB).
 * - write request: always consumes net_bw and db_wcu.
 *
 * Assumption:
 * - the workload is a fixed ratio of read and write requests
 * - request size is independent of hotness, so that we can use average value to
 *   compute demand vector
 */

struct StatelessResrcVec {
  double db_rcu = 0; // DynamoDB read capacity unit, unit: #req/s
  double db_wcu = 0; // DynamoDB write capacity unit, unit: #req/s
  double net_bw = 0; // unit: bytes/s

  explicit StatelessResrcVec(double db_rcu, double db_wcu, double net_bw)
      : db_rcu(db_rcu), db_wcu(db_wcu), net_bw(net_bw) {}

  explicit StatelessResrcVec(const std::tuple<double, double, double> &t)
      : db_rcu(std::get<0>(t)),
        db_wcu(std::get<1>(t)),
        net_bw(std::get<2>(t)) {}

  StatelessResrcVec() = default;
  StatelessResrcVec(const StatelessResrcVec &) = default;
  StatelessResrcVec(StatelessResrcVec &&) = default;
  StatelessResrcVec &operator=(const StatelessResrcVec &) = default;
  StatelessResrcVec &operator=(StatelessResrcVec &&) = default;

  [[nodiscard]] bool is_empty() const {
    return db_rcu == 0 && db_wcu == 0 && net_bw == 0;
  }

  [[nodiscard]] bool is_almost_empty() const {
    return std::abs(db_rcu) < params::numeric::db_rcu_epsilon &&
           std::abs(db_wcu) < params::numeric::db_wcu_epsilon &&
           std::abs(net_bw) < params::numeric::net_bw_epsilon;
  }

  bool operator==(const StatelessResrcVec &other) const {
    return db_rcu == other.db_rcu && db_wcu == other.db_wcu &&
           net_bw == other.net_bw;
  }
  bool operator!=(const StatelessResrcVec &other) const {
    return !(*this == other);
  }

  [[nodiscard]] bool is_almost_equal(const StatelessResrcVec &other) const {
    return (*this - other).is_almost_empty();
  }

  StatelessResrcVec operator+(const StatelessResrcVec &other) const {
    return StatelessResrcVec{db_rcu + other.db_rcu, db_wcu + other.db_wcu,
                             net_bw + other.net_bw};
  }

  StatelessResrcVec &operator+=(const StatelessResrcVec &other) {
    db_rcu += other.db_rcu;
    db_wcu += other.db_wcu;
    net_bw += other.net_bw;
    return *this;
  }

  StatelessResrcVec operator-(const StatelessResrcVec &other) const {
    return StatelessResrcVec{db_rcu - other.db_rcu, db_wcu - other.db_wcu,
                             net_bw - other.net_bw};
  }

  StatelessResrcVec &operator-=(const StatelessResrcVec &other) {
    db_rcu -= other.db_rcu;
    db_wcu -= other.db_wcu;
    net_bw -= other.net_bw;
    return *this;
  }

  StatelessResrcVec operator/(
      uint32_t div) const { // useful for equally share resource
    return StatelessResrcVec{db_rcu / div, db_wcu / div, net_bw / div};
  }

  double operator/(
      const StatelessResrcVec &other) const { // useful for improve_ratio
    return std::min(
        {db_rcu / other.db_rcu, db_wcu / other.db_wcu, net_bw / other.net_bw});
  }

  StatelessResrcVec operator*(double scale_factor) const {
    return StatelessResrcVec{db_rcu * scale_factor, db_wcu * scale_factor,
                             net_bw * scale_factor};
  }

  StatelessResrcVec &operator*=(double scale_factor) {
    db_rcu *= scale_factor;
    db_wcu *= scale_factor;
    net_bw *= scale_factor;
    return *this;
  }

  /* exposed in pybind for testing and debugging */
  std::string to_string() const {
    std::stringstream ss;
    ss << "{db_rcu=" << db_rcu << ", db_wcu=" << db_wcu << ", net_bw=" << net_bw
       << "}";
    return ss.str();
  }

  std::tuple<double, double, double> to_tuple() const {
    return std::tuple<double, double, double>{db_rcu, db_wcu, net_bw};
  }
};

// generalized demand vector, denote the amount of stateless resources
// allocated
struct ResrcVec {
  uint64_t cache_size = 0; // unit: bytes
  StatelessResrcVec stateless{};

  explicit ResrcVec(uint64_t cache_size, double db_rcu, double db_wcu,
                    double net_bw)
      : cache_size(cache_size), stateless(db_rcu, db_wcu, net_bw) {}

  explicit ResrcVec(uint64_t cache_size, StatelessResrcVec stateless)
      : cache_size(cache_size), stateless(stateless) {}

  explicit ResrcVec(const std::tuple<uint64_t, double, double, double> &t)
      : cache_size(std::get<0>(t)),
        stateless(std::get<1>(t), std::get<2>(t), std::get<3>(t)) {}

  ResrcVec() = default;
  ResrcVec(const ResrcVec &) = default;
  ResrcVec(ResrcVec &&) = default;
  ResrcVec &operator=(const ResrcVec &) = default;
  ResrcVec &operator=(ResrcVec &&) = default;

  bool operator==(const ResrcVec &other) const {
    return cache_size == other.cache_size && stateless == other.stateless;
  }
  bool operator!=(const ResrcVec &other) const { return !(*this == other); }

  ResrcVec operator+(const ResrcVec &other) const {
    return ResrcVec{cache_size + other.cache_size, stateless + other.stateless};
  }

  ResrcVec &operator+=(const ResrcVec &other) {
    cache_size += other.cache_size;
    stateless += other.stateless;
    return *this;
  }

  ResrcVec operator+(const StatelessResrcVec &other_stateless) const {
    return ResrcVec{cache_size, stateless + other_stateless};
  }

  ResrcVec &operator+=(const StatelessResrcVec &other_stateless) {
    stateless += other_stateless;
    return *this;
  }

  ResrcVec operator/(uint32_t div) const { // useful for equally share resource
    return ResrcVec{cache_size / div, stateless / div};
  }

  // there seems to be no use case, but explicitly delete to avoid accident call
  // the one above
  ResrcVec operator/(double div) = delete;

  /* exposed in pybind for testing and debugging */
  std::string to_string() const {
    std::stringstream ss;
    ss << "{cache_size=" << cache_size << ", db_rcu=" << stateless.db_rcu
       << ", db_wcu=" << stateless.db_wcu << ", net_bw=" << stateless.net_bw
       << "}";
    return ss.str();
  }

  std::tuple<uint64_t, double, double, double> to_tuple() const {
    return std::tuple<uint64_t, double, double, double>{
        cache_size, stateless.db_rcu, stateless.db_wcu, stateless.net_bw};
  }
};

} // namespace hare
