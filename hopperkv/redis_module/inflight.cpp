#include "inflight.h"

#include <cassert>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "task.h"

using namespace hopper;

namespace hopper::inflight {

std::unordered_map<std::string, struct task::TaskGet *> inflight_map;

// check if there is a inflight request for the given key
bool check_inflight(const std::string &key) {
  if constexpr (!config::cache::enable_inflight_dedup) return false;
  return inflight_map.contains(key);
}

// add a blocked client as a dependent on the inflight request
// only valid if check_inflight returns true
void add_dependent(const std::string &key, RedisModuleBlockedClient *bc) {
  assert(config::cache::enable_inflight_dedup);
  assert(inflight_map.contains(key));
  inflight_map[key]->add_dependent(bc);
}

// register a new inflight request with the given key
// only valid if check_inflight returns false
void begin_inflight(const std::string &key, struct task::TaskGet *t) {
  if constexpr (!config::cache::enable_inflight_dedup) return;
  assert(!inflight_map.contains(key));
  inflight_map[key] = t;
}

// complete an inflight request; will unblock all dependents
// only valid if previously begun
// return whether should update the cache (false if marked stale)
bool end_inflight(const std::string &key, struct task::TaskGet *t) {
  if constexpr (!config::cache::enable_inflight_dedup) return true;
  auto it = inflight_map.find(key);
  if (it == inflight_map.end() || it->second != t) return false;
  inflight_map.erase(it);
  return true;
}

// mark an inflight (if exist) as stale
// once that request completes, will not update the cache
void invalidate_inflight(const std::string &key) {
  if constexpr (!config::cache::enable_inflight_dedup) return;
  inflight_map.erase(key);
}

} // namespace hopper::inflight
