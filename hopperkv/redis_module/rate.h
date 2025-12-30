#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>

namespace hopper::rate {

struct SingleThreadProgress {
  uint64_t progress = 0;

  uint64_t load() const { return progress; }
  void store(uint64_t x) { progress = x; }
  void add(uint64_t x) { progress += x; }
};

struct ConcurrentProgress {
  std::atomic_uint64_t progress = 0;

  uint64_t load() const { return progress.load(std::memory_order_relaxed); }
  void store(uint64_t x) { progress.store(x, std::memory_order_relaxed); }
  void add(uint64_t x) { progress.fetch_add(x, std::memory_order_relaxed); }
};

template <typename Progress_t>
class RateLimiter {
  // refresh time frame every 0.37 sec (avoid lockstep with other components)
  static constexpr double TIME_FRAME_LEN_SEC = 0.37;

  double rate;
  // progress is measured within a time frame; throttle if exceeds
  Progress_t time_frame_progress;
  std::chrono::time_point<std::chrono::high_resolution_clock> time_frame_begin;

  std::atomic<double> proposed_rate;

  double update_time_frame() {
    auto ts_now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(
                       ts_now - time_frame_begin)
                       .count();

    if (elapsed >= TIME_FRAME_LEN_SEC) {
      elapsed = std::fmod(elapsed, TIME_FRAME_LEN_SEC);
      time_frame_begin =
          ts_now - std::chrono::duration_cast<
                       std::chrono::high_resolution_clock::duration>(
                       std::chrono::duration<double>(elapsed));
      time_frame_progress.store(0);
      // check if there is pending rate update
      double new_rate = proposed_rate.load(std::memory_order_relaxed);
      if (new_rate != rate) rate = new_rate;
    }

    return elapsed;
  }

 public:
  RateLimiter(double rate)
      : rate(rate), time_frame_progress(), proposed_rate(rate) {
    time_frame_begin = std::chrono::high_resolution_clock::now();
  }

  void consume(uint64_t consumption) { time_frame_progress.add(consumption); }

  // return wait time <= 0 means can send requests
  double check_wait_time() {
    auto elapsed = update_time_frame();
    auto permitted_elapsed = time_frame_progress.load() / rate;
    return permitted_elapsed - elapsed;
  }

  // propose a new rate; will be applied in the next time frame; thread-safe
  void propose_new_rate(double new_rate) { proposed_rate.store(new_rate); }
};

} // namespace hopper::rate
