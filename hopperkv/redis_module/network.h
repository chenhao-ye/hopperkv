#pragma once

namespace hopper::network {

void set_net_limit(double net_bw);
void consume(double consumption);
void wait_until_can_send();

} // namespace hopper::network
