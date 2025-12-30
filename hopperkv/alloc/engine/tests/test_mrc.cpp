#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "../mrc.h"
#include "../params.h"
#include "spdlog/spdlog.h"

using namespace hare;

void check_miss_ratio(MissRatioCurve &mrc, uint64_t cache_size, double expected,
                      double epsilon = params::numeric::epilson) {
  double mr = mrc.get_miss_ratio(cache_size);
  if (std::abs(expected - mr) > epsilon) {
    std::stringstream ss;
    SPDLOG_ERROR(
        "miss_ratio mismatch: cache_size={}, expected={:.2f}, actual={:.2f}",
        cache_size, expected, mr);
    throw std::logic_error("miss_ratio mismatch");
  }
}

// test basic functionality
void test_basic() {
  std::vector<uint64_t> ticks{10, 20, 40, 80};
  std::vector<double> miss_ratios{0.9, 0.8, 0.7, 0.6};
  MissRatioCurve mrc(ticks, miss_ratios);

  mrc.check_sanity();
  check_miss_ratio(mrc, 0, 1.0);
  check_miss_ratio(mrc, 5,
                   params::mrc::disable_interpolation_near_inf ? 1.0 : 0.95);
  check_miss_ratio(mrc, 7,
                   params::mrc::disable_interpolation_near_inf ? 1.0 : 0.93);
  check_miss_ratio(mrc, 10, 0.9);
  check_miss_ratio(mrc, 20, 0.8);
  check_miss_ratio(mrc, 25, 0.775);
  check_miss_ratio(mrc, 30, 0.75);
  check_miss_ratio(mrc, 40, 0.7);
  check_miss_ratio(mrc, 50, 0.675);
  check_miss_ratio(mrc, 60, 0.65);
  check_miss_ratio(mrc, 80, 0.6);
}

int main() {
  test_basic();
  return 0;
}
