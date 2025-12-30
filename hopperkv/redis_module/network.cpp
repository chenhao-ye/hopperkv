#include "network.h"

#include <thread>

#include "rate.h"

namespace hopper::network {

static rate::RateLimiter<rate::SingleThreadProgress> net_rate_limiter(
    1'000'000'000);

void set_net_limit(double net_bw) { net_rate_limiter.propose_new_rate(net_bw); }

void consume(double consumption) { net_rate_limiter.consume(consumption); }

// if bottlenecked by network, throttle by forcing the main thread to sleep
// this is suboptimal if multiple tenants share one Redis instance
// but in our use case, one Redis instance is dedicated to one tenant
void wait_until_can_send() {
  double wait_time = net_rate_limiter.check_wait_time();
  if (wait_time > 0) {
    std::this_thread::sleep_for(std::chrono::duration<double>(wait_time));
  }
}

} // namespace hopper::network
