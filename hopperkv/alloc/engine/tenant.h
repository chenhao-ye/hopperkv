#pragma once

#include <cassert>
#include <cstdint>
#include <limits>
#include <vector>

#include "mrc.h"
#include "params.h"
#include "resrc.h"
#include "spdlog/spdlog.h"

namespace hare {

class Tenant {
 public:
  const size_t t_idx; // for logging purpose

 private:
  const StatelessResrcVec demand_cacheless; // demand vector without cache hits
  ResrcVec resrc;                           // updated during HARE algorithm
  MissRatioCurve mrc;
  double net_bw_alpha;

  double rcu_delta_relinq = 0;
  double rcu_delta_compen = 0;
  double net_delta_relinq = 0;
  double net_delta_compen = 0;

  // miss ratio changes if given more/less cache
  double mr_inc_if_more_cache = 0;
  double mr_dec_if_less_cache = 0;

  uint64_t reserved_cache_size;

 public:
  Tenant(size_t t_idx, StatelessResrcVec demand_cacheless, ResrcVec base_resrc,
         std::vector<uint64_t> &&ticks, std::vector<double> &&miss_ratios,
         double net_bw_alpha)
      : t_idx(t_idx),
        demand_cacheless(demand_cacheless),
        resrc(base_resrc),
        mrc(std::move(ticks), std::move(miss_ratios)),
        net_bw_alpha(net_bw_alpha),
        reserved_cache_size(base_resrc.cache_size *
                            params::alloc::memshare::reserved_ratio) {}

  // rvalue reference is tricky in pybind; use copy for vector instead
  Tenant(size_t t_idx, StatelessResrcVec demand_cacheless, ResrcVec base_resrc,
         const std::vector<uint64_t> &ticks,
         const std::vector<double> &miss_ratios, double net_bw_alpha)
      : t_idx(t_idx),
        demand_cacheless(demand_cacheless),
        resrc(base_resrc),
        mrc(ticks, miss_ratios),
        net_bw_alpha(net_bw_alpha),
        reserved_cache_size(base_resrc.cache_size *
                            params::alloc::memshare::reserved_ratio) {}

  Tenant(size_t t_idx, StatelessResrcVec demand_cacheless, ResrcVec base_resrc,
         const MissRatioCurve &mrc, double net_bw_alpha)
      : t_idx(t_idx),
        demand_cacheless(demand_cacheless),
        resrc(base_resrc),
        mrc(mrc),
        net_bw_alpha(net_bw_alpha),
        reserved_cache_size(base_resrc.cache_size *
                            params::alloc::memshare::reserved_ratio) {}

  // disable all copy; always take ref during the HARE algorithm
  Tenant(const Tenant &) = delete;
  Tenant &operator=(const Tenant &) = delete;
  Tenant(Tenant &&) = default; // only move ctor is allowed for vector
  Tenant &operator=(Tenant &&other) = delete;

  // useful to show allocation result
  [[nodiscard]] ResrcVec get_resrc() const { return resrc; }
  [[nodiscard]] double get_rcu_delta_relinq() const { return rcu_delta_relinq; }
  [[nodiscard]] double get_rcu_delta_compen() const { return rcu_delta_compen; }
  [[nodiscard]] double get_net_delta_relinq() const { return net_delta_relinq; }
  [[nodiscard]] double get_net_delta_compen() const { return net_delta_compen; }

  [[nodiscard]] double get_mr_inc_if_more_cache() const {
    return mr_inc_if_more_cache;
  }
  [[nodiscard]] double get_mr_dec_if_less_cache() const {
    return mr_dec_if_less_cache;
  }

  StatelessResrcVec collect_idle();

  void update_rcu_net_delta() {
    pred_rcu_net_delta_if_more_cache();
    pred_rcu_net_delta_if_less_cache();
    SPDLOG_TRACE(
        "Tenant-{:d}: rcu_delta_relinq={:.2f}, rcu_delta_compen={:.2f}, "
        "net_delta_relinq={:.2f}, net_delta_compen={:.2f}",
        t_idx, rcu_delta_relinq, rcu_delta_compen, net_delta_relinq,
        net_delta_compen);
  }

  void update_mr_delta() {
    double curr_mr = mrc.get_miss_ratio(resrc.cache_size);
    double more_mr =
        mrc.get_miss_ratio(resrc.cache_size + params::alloc::cache_delta);
    double less_mr =
        mrc.get_miss_ratio(resrc.cache_size - params::alloc::cache_delta);
    mr_inc_if_more_cache = curr_mr - more_mr;
    mr_dec_if_less_cache = less_mr - curr_mr;
    SPDLOG_TRACE(
        "Tenant-{:d}: cache={:d}, curr_mr={:.1f}%, more_mr={:.1f}%, "
        "less_mr={:.1f}%, mr_inc={:.1f}%, mr_dec={:.1f}%",
        t_idx, resrc.cache_size, curr_mr * 100, more_mr * 100, less_mr * 100,
        mr_inc_if_more_cache * 100, mr_dec_if_less_cache * 100);
  }

  [[nodiscard]] bool can_donate(uint64_t delta = params::alloc::cache_delta) {
    return resrc.cache_size >= reserved_cache_size + delta;
  }

  // handy resource operators (so that no need to expose the field `resrc`)
  void scale_stateless_resrc(double scale_factor) {
    resrc.stateless *= scale_factor;
  }

  // will try to scale stateless resource by owned, but in the case that a field
  // in sum is 0.0, will fall back to even division
  void scale_stateless_resrc_by_owned(const StatelessResrcVec &avail,
                                      const StatelessResrcVec &sum,
                                      size_t even_denom) {
    double db_rcu_factor = sum.db_rcu != 0.0
                               ? resrc.stateless.db_rcu / sum.db_rcu
                               : 1.0 / double(even_denom);
    double db_wcu_factor = sum.db_wcu != 0.0
                               ? resrc.stateless.db_wcu / sum.db_wcu
                               : 1.0 / double(even_denom);
    double net_bw_factor = sum.net_bw != 0.0
                               ? resrc.stateless.net_bw / sum.net_bw
                               : 1.0 / double(even_denom);
    resrc.stateless.db_rcu += avail.db_rcu * db_rcu_factor;
    resrc.stateless.db_wcu += avail.db_wcu * db_wcu_factor;
    resrc.stateless.net_bw += avail.net_bw * net_bw_factor;
  }

  static void relocate_cache(Tenant &t_receiver, Tenant &t_donator) {
    t_receiver.resrc.cache_size += params::alloc::cache_delta;
    t_donator.resrc.cache_size -= params::alloc::cache_delta;
  }

  static void relocate_resrc(Tenant &t_relinq, Tenant &t_compen,
                             double rcu_relinq, double rcu_compen,
                             double net_relinq, double net_compen) {
    t_compen.resrc.cache_size -= params::alloc::cache_delta;
    t_relinq.resrc.cache_size += params::alloc::cache_delta;
    t_compen.resrc.stateless.db_rcu += rcu_compen;
    t_relinq.resrc.stateless.db_rcu -= rcu_relinq;
    if (params::policy::alloc_total_net_bw) {
      t_compen.resrc.stateless.net_bw += net_compen;
      t_relinq.resrc.stateless.net_bw -= net_relinq;
    } else {
      assert(net_compen == 0);
      assert(net_relinq == 0);
    }
  }

  static StatelessResrcVec aggregate_resrc(const std::vector<Tenant> &tenants) {
    StatelessResrcVec sum;
    for (auto &t : tenants) sum += t.resrc.stateless;
    return sum;
  }

  void report(bool detailed = false) const {
    if (detailed) {
      StatelessResrcVec demand = demand_cacheless;
      double mr = mrc.get_miss_ratio_const(resrc.cache_size);
      demand.db_rcu *= mr;
      if (params::policy::alloc_total_net_bw) demand.net_bw *= mr;
      double tput = resrc.stateless / demand;
      SPDLOG_TRACE(
          "Tenant-{:d}: cache_size={:d}, db_rcu={:.2f}, db_wcu={:.2f}, "
          "net_bw={:.2f}, tput={:.2f}",
          t_idx, resrc.cache_size, resrc.stateless.db_rcu,
          resrc.stateless.db_wcu, resrc.stateless.net_bw, tput);
    } else {
      SPDLOG_TRACE(
          "Tenant-{:d}: cache_size={:d}, db_rcu={:.2f}, db_wcu={:.2f}, "
          "net_bw={:.2f}",
          t_idx, resrc.cache_size, resrc.stateless.db_rcu,
          resrc.stateless.db_wcu, resrc.stateless.net_bw);
    }
  }

 private:
  // if given/taken cache, how much RCU to release/compensate to keep the
  // the same throughput (may be higher in the case of full cache hit...)
  // the return value must be non-negative (can be zero or infinity)
  void pred_rcu_net_delta_if_more_cache(
      uint64_t cache_delta = params::alloc::cache_delta);
  void pred_rcu_net_delta_if_less_cache(
      uint64_t cache_delta = params::alloc::cache_delta);
};

inline StatelessResrcVec Tenant::collect_idle() {
  StatelessResrcVec demand = demand_cacheless;
  double mr = mrc.get_miss_ratio(resrc.cache_size);
  demand.db_rcu *= mr;
  if (params::policy::alloc_total_net_bw)
    demand.net_bw *= (mr + ((1 - net_bw_alpha) * (1 - mr)));

  // division will take the min across stateless resources
  double tp = resrc.stateless / demand;
  StatelessResrcVec used = demand * tp;
  StatelessResrcVec idle = resrc.stateless - used;
  resrc.stateless = used;
  return idle;
}

inline void Tenant::pred_rcu_net_delta_if_more_cache(uint64_t cache_delta) {
  // returning 0 indicates to abort this deal.
  // this means this client is asking for cache but return with no bandwidth,
  // which is impossible to be accepted.
  double curr_mr, pred_mr, delta_mr;

  curr_mr = mrc.get_miss_ratio(resrc.cache_size);
  if (curr_mr == std::numeric_limits<double>::infinity()) goto abort_offer;

  // no way to make a deal, if the miss ratio is already near zero
  // early return to avoid divided-by-zero error
  if (curr_mr <= params::numeric::epilson) goto abort_offer;

  pred_mr = mrc.get_miss_ratio(resrc.cache_size + cache_delta);
  if (pred_mr == std::numeric_limits<double>::infinity()) goto abort_offer;

  // miss ratio is too low, abort the deal
  if (pred_mr < params::alloc::min_miss_ratio) goto abort_offer;

  // if miss ratios are close enough, it means giving more cache will not lead
  // to any rcu relinquish
  delta_mr = curr_mr - pred_mr;
  if (delta_mr <= params::numeric::epilson) goto abort_offer;

  assert(delta_mr > 0);

  rcu_delta_relinq = resrc.stateless.db_rcu * delta_mr / curr_mr;
  assert(rcu_delta_relinq >= 0);
  if (params::policy::alloc_total_net_bw) {
    net_delta_relinq = resrc.stateless.net_bw * delta_mr * net_bw_alpha /
                       (curr_mr * net_bw_alpha + 1 - net_bw_alpha);
    assert(net_delta_relinq >= 0);
  }

  SPDLOG_TRACE(
      "Tenant-{:d}: if cache {:d} -> {:d}, then miss_ratio {:.3f} -> {:.3f}, "
      "db_rcu {:.2f} -> {:.2f}, rcu_relinq={:.2f}, "
      "net_bw {:.2f} -> {:.2f}, net_relinq={:.2f}",
      t_idx, resrc.cache_size, resrc.cache_size + cache_delta, curr_mr, pred_mr,
      resrc.stateless.db_rcu, resrc.stateless.db_rcu - rcu_delta_relinq,
      rcu_delta_relinq, resrc.stateless.net_bw,
      resrc.stateless.net_bw - net_delta_relinq, net_delta_relinq);
  return;

abort_offer:
  rcu_delta_relinq = params::numeric::relinq_abort_offer;
  if (params::policy::alloc_total_net_bw)
    net_delta_relinq = params::numeric::relinq_abort_offer;
}

inline void Tenant::pred_rcu_net_delta_if_less_cache(uint64_t cache_delta) {
  double curr_mr, pred_mr, delta_mr;

  // we want to check:
  //   resrc.cache_size - cache_delta < params::alloc::min_cache_size
  // use '+' here to avoid unsigned underflow
  if (resrc.cache_size < params::alloc::min_cache_size + cache_delta)
    goto abort_offer;

  curr_mr = mrc.get_miss_ratio(resrc.cache_size);
  if (curr_mr == std::numeric_limits<double>::infinity()) goto abort_offer;

  pred_mr = mrc.get_miss_ratio(resrc.cache_size - cache_delta);
  if (pred_mr == std::numeric_limits<double>::infinity()) goto abort_offer;

  // miss ratio is too high, abort the deal
  if (pred_mr > params::alloc::max_miss_ratio) goto abort_offer;

  // if miss ratios are close enough, it means we can take cache away without
  // any rcu compensation
  delta_mr = pred_mr - curr_mr;
  if (delta_mr <= params::numeric::epilson) goto immediate_offer;
  assert(delta_mr > 0);

  // do not reorder these if-conditions! order matters
  if (pred_mr <= params::numeric::epilson)
    goto immediate_offer; // still no miss
  else if (curr_mr <= params::numeric::epilson)
    goto abort_offer;

  rcu_delta_compen = resrc.stateless.db_rcu * delta_mr / curr_mr;
  assert(rcu_delta_compen >= 0);
  net_delta_compen = 0;
  if (params::policy::alloc_total_net_bw) {
    net_delta_compen = resrc.stateless.net_bw * delta_mr * net_bw_alpha /
                       (curr_mr * net_bw_alpha + 1 - net_bw_alpha);
    assert(net_delta_compen >= 0);
  }
  SPDLOG_TRACE(
      "Tenant-{:d}: if cache {:d} -> {:d}, then miss_ratio {:.3f} -> {:.3f}, "
      "db_rcu {:.2f} -> {:.2f}, rcu_compen={:.2f}, "
      "net_bw {:.2f} -> {:.2f}, net_compen={:.2f}",
      t_idx, resrc.cache_size, resrc.cache_size - cache_delta, curr_mr, pred_mr,
      resrc.stateless.db_rcu, resrc.stateless.db_rcu + rcu_delta_compen,
      rcu_delta_compen, resrc.stateless.net_bw,
      resrc.stateless.net_bw + net_delta_compen, net_delta_compen);
  return;

abort_offer:
  rcu_delta_compen = params::numeric::compen_abort_offer;
  if (params::policy::alloc_total_net_bw)
    net_delta_compen = params::numeric::compen_abort_offer;
  return;

immediate_offer: // meaning ask for nothing for compensation
  rcu_delta_compen = 0;
  if (params::policy::alloc_total_net_bw) net_delta_compen = 0;
}

} // namespace hare
