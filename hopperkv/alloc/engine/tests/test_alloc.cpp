#include <cstdint>
#include <stdexcept>
#include <vector>

#include "../alloc.h"
#include "../mrc.h"
#include "../params.h"
#include "spdlog/spdlog.h"

using namespace hare;

// this make sure the tests can still work even we change cache_delta
// rounding to 32 because ticks must align with the ghost cache sampling rate
#define TICK_UNIT(x) ((x) * params::alloc::cache_delta)

bool is_resrc_similar(const ResrcVec &expected, const ResrcVec &actual,
                      double epilson) {
  return expected.cache_size == actual.cache_size &&
         std::abs(expected.stateless.db_rcu - actual.stateless.db_rcu) <
             epilson &&
         std::abs(expected.stateless.db_wcu - actual.stateless.db_wcu) <
             epilson &&
         std::abs(expected.stateless.net_bw - actual.stateless.net_bw) <
             epilson;
}

void check_tenant_resrc(const ResrcVec &expected, const Allocator &allocator,
                        size_t t_idx, double epilson = 0.01) {
  ResrcVec actual = allocator.get_tenant(t_idx).get_resrc();
  if (!is_resrc_similar(expected, actual, epilson)) {
    SPDLOG_ERROR("Tenant-{:d} ResrVec mismatch: expected={}, actual={}", t_idx,
                 expected.to_string(), actual.to_string());
    throw std::logic_error("ResrVec mismatch");
  }
}

void test_trivial() {
  SPDLOG_INFO("===== test_trivial =====");
  std::vector<uint64_t> ticks{TICK_UNIT(1), TICK_UNIT(2), TICK_UNIT(4),
                              TICK_UNIT(8), TICK_UNIT(10)};
  std::vector<double> miss_ratios{0.9, 0.8, 0.7, 0.6, 0.4};
  StatelessResrcVec demand{0.5, 0.5, 4};
  ResrcVec base_resrc{20, 2, 1.2, 6};

  Allocator a;
  a.add_tenant(demand, base_resrc, MissRatioCurve(ticks, miss_ratios), 0);

  a.do_alloc(); // nothing should happen

  check_tenant_resrc(base_resrc, a, 0);
}

void test_symmetric() {
  SPDLOG_INFO("===== test_symmetric =====");

  // if all tenants are symmetric, no allocation should happen
  std::vector<uint64_t> ticks{TICK_UNIT(1), TICK_UNIT(2), TICK_UNIT(4),
                              TICK_UNIT(8), TICK_UNIT(10)};
  std::vector<double> miss_ratios{0.9, 0.8, 0.7, 0.6, 0.4};
  StatelessResrcVec demand{0.5, 0.5, 4};
  ResrcVec base_resrc{TICK_UNIT(2), 2, 2, 16};

  Allocator a;
  for (size_t i = 0; i < 4; ++i)
    a.add_tenant(demand, base_resrc, MissRatioCurve(ticks, miss_ratios), 0);

  a.do_alloc(); // nothing should happen

  for (size_t i = 0; i < 4; ++i) check_tenant_resrc(base_resrc, a, i);
}

void test_rw_ratio() { // show DRF works
  SPDLOG_INFO("===== test_rw_ratio =====");

  // if all tenants are symmetric, no allocation should happen
  std::vector<uint64_t> ticks{TICK_UNIT(1), TICK_UNIT(2), TICK_UNIT(4),
                              TICK_UNIT(8), TICK_UNIT(10)};
  std::vector<double> miss_ratios{1, 1, 1, 1, 1};
  StatelessResrcVec demand_1{0.8, 0.2, 4};
  StatelessResrcVec demand_2{0.2, 0.8, 4};
  ResrcVec base_resrc{TICK_UNIT(2), 2, 2, 16};

  Allocator a;
  a.add_tenant(demand_1, base_resrc, MissRatioCurve(ticks, miss_ratios), 0);
  a.add_tenant(demand_2, base_resrc, MissRatioCurve(ticks, miss_ratios), 0);

  SPDLOG_TRACE("----- before alloc -----");
  a.get_tenant(0).report(true);
  a.get_tenant(1).report(true);

  a.do_alloc();

  SPDLOG_TRACE("----- after alloc -----");
  a.get_tenant(0).report(true);
  a.get_tenant(1).report(true);

  check_tenant_resrc(ResrcVec{TICK_UNIT(2), 3.2, 0.8, 16.0}, a, 0);
  check_tenant_resrc(ResrcVec{TICK_UNIT(2), 0.8, 3.2, 16.0}, a, 1);
}

void test_trade_basic() {
  SPDLOG_INFO("===== test_trade_basic =====");

  // if all tenants are symmetric, no allocation should happen
  std::vector<uint64_t> ticks{TICK_UNIT(2), TICK_UNIT(4), TICK_UNIT(6),
                              TICK_UNIT(8), TICK_UNIT(10)};
  std::vector<double> miss_ratios_1{0.9, 0.85, 0.8, 0.7, 0.5};
  std::vector<double> miss_ratios_2{0.8, 0.6, 0.3, 0.2, 0.15};
  StatelessResrcVec demand{0.8, 0.2, 4};
  ResrcVec base_resrc{TICK_UNIT(4), 2, 2, 16};

  Allocator a;
  a.add_tenant(demand, base_resrc, MissRatioCurve(ticks, miss_ratios_1), 0);
  a.add_tenant(demand, base_resrc, MissRatioCurve(ticks, miss_ratios_2), 0);

  SPDLOG_TRACE("----- before alloc -----");
  a.get_tenant(0).report(true);
  a.get_tenant(1).report(true);

  a.do_alloc();

  SPDLOG_TRACE("----- after alloc -----");
  a.get_tenant(0).report(true);
  a.get_tenant(1).report(true);

  check_tenant_resrc(ResrcVec{TICK_UNIT(2), 2.75, 1.69, 13.56}, a, 0);
  check_tenant_resrc(ResrcVec{TICK_UNIT(6), 1.25, 2.31, 18.44}, a, 1);
}

int main() {
  spdlog::set_level(spdlog::level::trace);

  params::policy::set_alloc_total_net_bw(false);

  // fix the params for stable test results
  params::alloc::set_cache_delta(10);
  params::alloc::set_min_cache_size(10);
  params::alloc::set_min_db_rcu(10);
  params::alloc::set_min_db_wcu(10);
  params::alloc::set_min_net_bw(10);

  test_trivial();
  test_symmetric();
  test_rw_ratio();
  test_trade_basic();
  return 0;
}
