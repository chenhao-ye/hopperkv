#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>

#include "mrc.h"
#include "params.h"
#include "resrc.h"
#include "spdlog/spdlog.h"
#include "tenant.h"

class FsProc;

namespace hare {

class Allocator {
  struct {
    // whether enable resource harvest phase; if false, it is a cache-unawared
    // DRF this flag is set by cmdline
    bool harvest;

    // whether perform conserving redistribution; if false, may have some
    // stateless resources unallocated
    bool conserving;

    // whether use memshare' cache allocation algorithm; cannot be both true
    // with harvest
    bool memshare;
  } policy;

  std::vector<Tenant> tenants;
  ResrcVec total_resrc;

 public:
  Allocator(bool harvest = true, bool conserving = true, bool memshare = false)
      : policy{.harvest = harvest,
               .conserving = conserving,
               .memshare = memshare} {};

  // return the improvement ratio
  double do_alloc();

  // this API is more compatible with pybind, as pybind has poor support for
  // rvalue reference
  size_t add_tenant(StatelessResrcVec demand_cacheless, ResrcVec base_resrc,
                    const MissRatioCurve &mrc, double net_bw_alpha);

  const Tenant &get_tenant(size_t t_idx) const { return tenants[t_idx]; }

  std::vector<ResrcVec> get_alloc_result();

 private:
  /**
   * @brief Harvest db_rcu and net_bw by relocating cache.
   *
   * @param resrc_avail Available stateless resources to redistribute; pass by
   * reference and will be updated during harvesting
   */
  void do_harvest(StatelessResrcVec &resrc_avail);

  /**
   * @brief Distribute available stateless resources.
   *
   * @param resrc_avail Available stateless resources to redistribute; pass by
   * reference and will be updated
   * @return double The improvement ratio
   */
  double do_redistribute(StatelessResrcVec &resrc_avail);

  /**
   * @brief Run Memshare's cache allocation across tenants.
   */
  void do_memshare();

  void estimate_bottleneck(const StatelessResrcVec &resrc_avail,
                           double &estimated_improve_ratio,
                           bool &is_rcu_bottleneck,
                           bool &is_net_bottleneck) const;
};

inline size_t Allocator::add_tenant(StatelessResrcVec demand_cacheless,
                                    ResrcVec base_resrc,
                                    const MissRatioCurve &mrc,
                                    double net_bw_alpha) {
  size_t t_idx = tenants.size();
  total_resrc += base_resrc;
  tenants.emplace_back(t_idx, demand_cacheless, base_resrc, mrc, net_bw_alpha);
  SPDLOG_TRACE(
      "Tenant-{:d} demand vector: "
      "{{ db_rcu={:.2f}, db_wcu={:.2f}, net_bw={:.2f} }}, net_bw_alpha={:.2f}",
      t_idx, demand_cacheless.db_rcu, demand_cacheless.db_wcu,
      demand_cacheless.net_bw, net_bw_alpha);
  return t_idx;
}

inline std::vector<ResrcVec> Allocator::get_alloc_result() {
  std::vector<ResrcVec> results;
  for (const auto &t : tenants) results.emplace_back(t.get_resrc());
  return results;
}

inline double Allocator::do_alloc() {
  double improve_ratio = 0;
  params::log_params();

  SPDLOG_INFO(
      "hare::allocator.policy {{ harvest={}, conserving={}, memshare={} }}",
      policy.harvest, policy.conserving, policy.memshare);

  if (tenants.size() <= 1)
    return improve_ratio; // nothing to schedule if there is only one tenant

  // run memshare cache allocation policy if necessary
  if (policy.memshare) do_memshare();

  // available resources (either from collect_idle or harvest)
  StatelessResrcVec resrc_avail;

  // collect idle resources
  for (auto &t : tenants) {
    StatelessResrcVec resrc_idle = t.collect_idle();
    SPDLOG_TRACE(
        "Collect idle resources from Tenant-{:d} "
        "{{ db_rcu={:.2f}, db_wcu={:.2f}, net_bw={:.2f} }}",
        t.t_idx, resrc_idle.db_rcu, resrc_idle.db_wcu, resrc_idle.net_bw);
    resrc_avail += resrc_idle;
  }
  SPDLOG_TRACE(
      "Total idle resources "
      "{{ db_rcu={:.2f}, db_wcu={:.2f}, net_bw={:.2f} }}",
      resrc_avail.db_rcu, resrc_avail.db_wcu, resrc_avail.net_bw);

  // then start harvest
  if (policy.harvest) {
    // if cache_partition is not enabled, we are using global LRU, so there is
    // no per-tenant cache allocation, thus, no harvest
    do_harvest(resrc_avail);
  }

  SPDLOG_TRACE(
      "Total resources to redistribute "
      "{{ db_rcu={:.2f}, db_wcu={:.2f}, net_bw={:.2f} }}",
      resrc_avail.db_rcu, resrc_avail.db_wcu, resrc_avail.net_bw);

  if (resrc_avail.is_almost_empty()) goto done;

  // distribute those harvested resources
  improve_ratio = do_redistribute(resrc_avail);

  if (resrc_avail.is_almost_empty()) goto done;

  // TODO: if necessary, implement other allocation policy that may cause
  // available resources leftover

done:
  for (auto &t : tenants) t.report();
  return improve_ratio;
}

inline void Allocator::do_harvest(StatelessResrcVec &resrc_avail) {
  double prev_estimated_improve_ratio;
  bool is_rcu_bottleneck, is_net_bottleneck;
  estimate_bottleneck(resrc_avail, prev_estimated_improve_ratio,
                      is_rcu_bottleneck, is_net_bottleneck);

  std::vector<Tenant *> rcu_relinq_list; // sorted by db_rcu to relinquish
  std::vector<Tenant *> rcu_compen_list; // sorted by db_rcu to compensate
  std::vector<Tenant *> net_relinq_list; // sorted by net_bw to relinquish
  std::vector<Tenant *> net_compen_list; // sorted by net_bw to compensate

  // compare functions for relinquish and compensate
  constexpr auto less_rcu_relinq = [](const Tenant *lhs, const Tenant *rhs) {
    return lhs->get_rcu_delta_relinq() < rhs->get_rcu_delta_relinq();
  };
  constexpr auto less_rcu_compen = [](const Tenant *lhs, const Tenant *rhs) {
    return lhs->get_rcu_delta_compen() < rhs->get_rcu_delta_compen();
  };
  constexpr auto less_net_relinq = [](const Tenant *lhs, const Tenant *rhs) {
    return lhs->get_net_delta_relinq() < rhs->get_net_delta_relinq();
  };
  constexpr auto less_net_compen = [](const Tenant *lhs, const Tenant *rhs) {
    return lhs->get_net_delta_compen() < rhs->get_net_delta_compen();
  };

  for (auto &t : tenants) {
    t.update_rcu_net_delta();
    rcu_relinq_list.emplace_back(&t);
    rcu_compen_list.emplace_back(&t);
    if (params::policy::alloc_total_net_bw) {
      net_relinq_list.emplace_back(&t);
      net_compen_list.emplace_back(&t);
    }
  }

  uint32_t trade_round = 0;
  auto t0 = std::chrono::high_resolution_clock::now();

  while (true) {
    if (trade_round >= params::alloc::max_trade_round) break;

    std::vector<Tenant *>::iterator it_relinq, it_compen;

    if (is_rcu_bottleneck) { // use db_rcu as the target trading resource
      it_relinq = std::max_element(rcu_relinq_list.begin(),
                                   rcu_relinq_list.end(), less_rcu_relinq);
      it_compen = std::min_element(rcu_compen_list.begin(),
                                   rcu_compen_list.end(), less_rcu_compen);
    } else if (params::policy::alloc_total_net_bw && is_net_bottleneck) {
      // harvest involves net_bw only if alloc_total_net_bw is enabled
      it_relinq = std::max_element(net_relinq_list.begin(),
                                   net_relinq_list.end(), less_net_relinq);
      it_compen = std::min_element(net_compen_list.begin(),
                                   net_compen_list.end(), less_net_compen);
    } else {
      // neither cache-correlated resource is bottleneck;
      // there is no point to continue trading
      break;
    }

    Tenant *t_relinq = *it_relinq;
    Tenant *t_compen = *it_compen;

    if (t_relinq == t_compen) {
      // in a rare case, both relinquish and compensate are from the same
      // client, in which case we need to make a new deal (for simplicity, just
      // use the second instead)
      if (is_rcu_bottleneck) {
        std::iter_swap(it_compen, rcu_compen_list.begin());
        it_compen = std::min_element(rcu_compen_list.begin() + 1,
                                     rcu_compen_list.end(), less_rcu_compen);
        t_compen = *it_compen;
      } else {
        assert(is_net_bottleneck);
        std::iter_swap(it_compen, net_compen_list.begin());
        it_compen = std::min_element(net_compen_list.begin() + 1,
                                     net_compen_list.end(), less_net_compen);
        t_compen = *it_compen;
      }
    }

    double rcu_delta_relinq = t_relinq->get_rcu_delta_relinq();
    double net_delta_relinq = t_relinq->get_net_delta_relinq();
    double rcu_delta_compen = t_compen->get_rcu_delta_compen();
    double net_delta_compen = t_compen->get_net_delta_compen();

    SPDLOG_TRACE(
        "Deal candidates: "
        "Tenant-{:d}: rcu_relinq={:.2f}, net_relinq={:.2f}; "
        "Tenant-{:d}: rcu_compen={:.2f}, net_compen={:.2f}",
        t_relinq->t_idx, rcu_delta_relinq, net_delta_relinq, t_compen->t_idx,
        rcu_delta_compen, net_delta_compen);

    double rcu_profit = rcu_delta_relinq - rcu_delta_compen;
    double net_profit = net_delta_relinq - net_delta_compen;

    StatelessResrcVec resrc_if_deal = resrc_avail;
    resrc_if_deal.db_rcu += rcu_profit;
    resrc_if_deal.net_bw += net_profit;
    double curr_estimated_improve_ratio;
    // this check is necessary to ensure convergence
    estimate_bottleneck(resrc_if_deal, curr_estimated_improve_ratio,
                        is_rcu_bottleneck, is_net_bottleneck);
    if (curr_estimated_improve_ratio - prev_estimated_improve_ratio <
        params::alloc::min_improve_ratio_delta) {
      SPDLOG_TRACE(
          "Deal cancelled due to low improvement gain: {:.1f}% -> {:.1f}%",
          prev_estimated_improve_ratio * 100,
          curr_estimated_improve_ratio * 100);
      break; // likely no further deal can be made
    }

    prev_estimated_improve_ratio = curr_estimated_improve_ratio;
    resrc_avail = resrc_if_deal;

    SPDLOG_TRACE(
        "Deal is made with rcu_profit={:.2f} and net_profit={:.2f}; "
        "estimated_improve_ratio={:.1f}%",
        rcu_profit, net_profit, curr_estimated_improve_ratio * 100);

    Tenant::relocate_resrc(*t_relinq, *t_compen, rcu_delta_relinq,
                           rcu_delta_compen, net_delta_relinq,
                           net_delta_compen);

    // trigger the next round:
    // recompute the prediction of tenants whose resources are updated
    t_relinq->update_rcu_net_delta();
    t_compen->update_rcu_net_delta();

    ++trade_round;
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  [[maybe_unused]] double trading_cost_us =
      std::chrono::duration<double, std::micro>(t1 - t0).count();

  SPDLOG_INFO("Trading takes {} rounds with {:.1f} us", trade_round,
              trading_cost_us);
}

inline double Allocator::do_redistribute(StatelessResrcVec &resrc_avail) {
  StatelessResrcVec resrc_sum = total_resrc.stateless - resrc_avail;
  assert(Tenant::aggregate_resrc(tenants).is_almost_equal(resrc_sum));
  double improve_ratio = resrc_avail / resrc_sum;

  if (policy.conserving) {
    for (auto &t : tenants)
      t.scale_stateless_resrc_by_owned(resrc_avail, resrc_sum, tenants.size());

    SPDLOG_TRACE("Expect to improve tput by {:.1f}%", improve_ratio * 100);

    // no resource available anymore
    resrc_avail = StatelessResrcVec{};
  } else { // conserving redistribution is not enabled
    double scale_factor = 1 + improve_ratio;
    for (auto &t : tenants) t.scale_stateless_resrc(scale_factor);

    // recompute resrc_sum to update resrc_avail
    resrc_sum = Tenant::aggregate_resrc(tenants);
    resrc_avail = total_resrc.stateless - resrc_sum;
  }
  return improve_ratio;
}

inline void Allocator::do_memshare() {
  std::vector<Tenant *> cache_more_list;
  std::vector<Tenant *> cache_less_list;

  constexpr auto less_mr_inc = [](const Tenant *lhs, const Tenant *rhs) {
    return lhs->get_mr_inc_if_more_cache() < rhs->get_mr_dec_if_less_cache();
  };
  constexpr auto less_mr_dec = [](const Tenant *lhs, const Tenant *rhs) {
    return lhs->get_mr_dec_if_less_cache() < rhs->get_mr_dec_if_less_cache();
  };

  for (auto &t : tenants) {
    cache_more_list.emplace_back(&t);
    cache_less_list.emplace_back(&t);
  }

  uint32_t trade_round = 0;
  auto t0 = std::chrono::high_resolution_clock::now();
  while (true) {
    for (auto &t : tenants) t.update_mr_delta();
    std::vector<Tenant *>::iterator it_receiver, it_donator;
    // the cache receiver must be the max one in cache_more_list, but the
    // donator may not be the min because of the lower bound of reserved memory
    it_receiver = std::max_element(cache_more_list.begin(),
                                   cache_more_list.end(), less_mr_inc);
    std::sort(cache_less_list.begin(), cache_less_list.end(), less_mr_dec);
    for (it_donator = cache_less_list.begin();
         it_donator != cache_less_list.end(); ++it_donator) {
      if (*it_receiver == *it_donator) continue;
      if ((*it_donator)->can_donate()) break;
    }

    if (it_donator == cache_less_list.end()) {
      SPDLOG_INFO("Memshare fails to find a donator");
      break; // fail to find a donator
    }

    double mr_inc = (*it_receiver)->get_mr_inc_if_more_cache();
    double mr_dec = (*it_donator)->get_mr_dec_if_less_cache();

    if (mr_inc > mr_dec) {
      Tenant::relocate_cache(**it_receiver, **it_donator);
      SPDLOG_TRACE(
          "Memshare relocates cache from Tenant-{:d} (-{:.1f}%) to Tenant-{:d} "
          "(+{:.1f}%)",
          (*it_donator)->t_idx, mr_dec * 100, (*it_receiver)->t_idx,
          mr_inc * 100);
      ++trade_round;
    } else {
      SPDLOG_TRACE(
          "Memshare terminates, because relocating cache from Tenant-{:d} "
          "(-{:.1f}%) to Tenant-{:d} (+{:.1f}%) does not profit",
          (*it_donator)->t_idx, mr_dec * 100, (*it_receiver)->t_idx,
          mr_inc * 100);
      break;
    }
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  [[maybe_unused]] double trading_cost_us =
      std::chrono::duration<double, std::micro>(t1 - t0).count();
  SPDLOG_INFO("Memshare: trading takes {} rounds with {:.1f} us", trade_round,
              trading_cost_us);
}

inline void Allocator::estimate_bottleneck(const StatelessResrcVec &resrc_avail,
                                           double &estimated_improve_ratio,
                                           bool &is_rcu_bottleneck,
                                           bool &is_net_bottleneck) const {
  StatelessResrcVec resrc_sum = total_resrc.stateless - resrc_avail;
  estimated_improve_ratio = resrc_avail / resrc_sum;
  is_rcu_bottleneck =
      estimated_improve_ratio == resrc_avail.db_rcu / resrc_sum.db_rcu;
  is_net_bottleneck =
      estimated_improve_ratio == resrc_avail.net_bw / resrc_sum.net_bw;
  SPDLOG_TRACE(
      "resrc_avail=[{:.2f}, {:.2f}, {:.2f}], "
      "resrc_sum=[{:.2f}, {:.2f}, {:.2f}], "
      "estimated_improve_ratio={:.1f}%, "
      "is_rcu_bottleneck={}, is_net_bottleneck={}",
      resrc_avail.db_rcu, resrc_avail.db_wcu, resrc_avail.net_bw,
      resrc_sum.db_rcu, resrc_sum.db_wcu, resrc_sum.net_bw,
      estimated_improve_ratio * 100, is_rcu_bottleneck, is_net_bottleneck);
}

} // namespace hare
