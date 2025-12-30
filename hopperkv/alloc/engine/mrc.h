#pragma once

#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "params.h"
#include "spdlog/spdlog.h"

namespace hare {

class MissRatioCurve {
  const std::vector<uint64_t> ticks;
  const std::vector<double> miss_ratios;

  // if we have already computed the hit rate, keep it around
  std::unordered_map<uint64_t, double> miss_ratio_map;

 public:
  MissRatioCurve(std::vector<uint64_t> &&ticks,
                 std::vector<double> &&miss_ratios)
      : ticks(std::move(ticks)), miss_ratios(std::move(miss_ratios)) {}

  // rvalue reference is tricky in pybind; use copy for vector instead
  MissRatioCurve(const std::vector<uint64_t> &ticks,
                 const std::vector<double> &miss_ratios)
      : ticks(ticks), miss_ratios(miss_ratios) {}

  MissRatioCurve(const MissRatioCurve &other) = default;

  // this function will query/update a miss ratio cache if the result has
  // already been computed, so it's not a const function
  [[nodiscard]] double get_miss_ratio(uint64_t cache_size) {
    // check if already computed
    auto it = miss_ratio_map.find(cache_size);
    if (it != miss_ratio_map.end()) return it->second;

    double miss_ratio = get_miss_ratio_const(cache_size);

    miss_ratio_map.emplace(cache_size, miss_ratio);
    return miss_ratio;
  }

  // this function does not modify any internal states of MissRatioCurve, so it
  // is a const function; use get_miss_ratio function instead whenever possible,
  // because that could reuse previously computed results
  [[nodiscard]] double get_miss_ratio_const(uint64_t cache_size) const {
    if (cache_size > ticks.back()) {
      if (params::mrc::conservative_estimation_if_out_of_range) {
        SPDLOG_WARN(
            "MissRatioCurve receives out-of-range cache_size: max={}, "
            "received={}; use conservative estimation: miss_ratio={}",
            ticks.back(), cache_size, miss_ratios.back());
        return miss_ratios.back();
      }

      SPDLOG_ERROR(
          "MissRatioCurve receives out-of-range cache_size: max={}, "
          "received={}",
          ticks.back(), cache_size);
      throw std::runtime_error("cache_size out of range");
    }

    if (cache_size < ticks.front()) {
      double size_ratio = double(cache_size) / ticks.front();
      return interpolate(/*miss_ratio(cache_size=0)*/ 1, miss_ratios.front(),
                         cache_size, ticks.front() - cache_size);
    }
    auto tick_it = std::lower_bound(ticks.begin(), ticks.end(), cache_size);
    auto tick_idx = std::distance(ticks.begin(), tick_it);
    if (cache_size == *tick_it) return miss_ratios[tick_idx];

    assert(tick_idx > 0);
    assert(tick_idx < ticks.size());
    assert(cache_size > ticks[tick_idx - 1]);
    assert(cache_size < ticks[tick_idx]);

    return interpolate(miss_ratios[tick_idx - 1], miss_ratios[tick_idx],
                       cache_size - ticks[tick_idx - 1],
                       ticks[tick_idx] - cache_size);
  }

  // perform sanity check of miss ratio curves (assume monotonic decreasing mrc)
  void check_sanity() { // throw if fail
    if (ticks.empty()) throw std::logic_error("ticks is empty");
    if (ticks.size() != miss_ratios.size())
      throw std::logic_error("ticks.size() and miss_ratios.size() mismatches");
    double min_mr = 0.0;
    double max_mr = 1.0;
    uint64_t min_tick = ticks.front();
    uint64_t max_tick = ticks.back();
    for (size_t i = 0; i < ticks.size(); ++i) {
      uint64_t t = ticks[i];
      double mr = miss_ratios[i];
      if (t < min_tick || t > max_tick)
        throw std::logic_error("tick is out of range");
      if (mr < min_mr || mr > max_mr)
        throw std::logic_error("miss_ratio is out of range");
      // assume monotonic decreasing mrc
      min_tick = t;
      max_mr = mr;
    }
  }

 private:
  static double interpolate(double l_val, double r_val, uint64_t l_dist,
                            uint64_t r_dist) {
    assert(l_val >= r_val);
    // do not interpolate between the min tick and zero
    if (params::mrc::disable_interpolation_near_inf &&
        (1.0 - l_val) < params::numeric::epilson)
      return 1;
    double total_dist = l_dist + r_dist;
    double l_ratio = r_dist / total_dist;
    double r_ratio = l_dist / total_dist;
    return l_val * l_ratio + r_val * r_ratio;
  }
};

} // namespace hare
