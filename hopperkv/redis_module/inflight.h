#pragma once
#include <cstdint>
#include <string>
#include <string_view>

#include "redismodule.h"
#include "task.h"

/**
 * An inflight request is a GET request that has been submitted to the storage
 * but has not been completed yet. If there are multiple GET requests for the
 * same key, we can deduplicate them to a single request to the storage, and
 * unblock all clients when the storage request completes.
 */
namespace hopper::inflight {

// check if there is a inflight request for the given key
bool check_inflight(const std::string &key);

// add a blocked client as a dependent on the inflight request
// only valid if check_inflight returns true
void add_dependent(const std::string &key, RedisModuleBlockedClient *bc);

// start a new inflight request with the given key
// only valid if check_inflight returns false
void begin_inflight(const std::string &key, struct task::TaskGet *t);

// end an inflight request; caller should unblock all dependents
// only valid if previously begun
// return whether should update the cache (false if marked stale)
bool end_inflight(const std::string &key, struct task::TaskGet *t);

// mark an inflight (if exist) as stale
// once that request calls end_inflight, will not update the cache
void invalidate_inflight(const std::string &key);

/**
 * while end_inflight and invalidate_inflight look similar, they have different
 * semantics: end_inflight is called by the same GET client that called
 * begin_inflight, while invalidate_inflight is called by a SET client to mark
 * an inflight GET as stale because the SET client has updated the cache.
 *
 * The dependents of the inflight GET will still receive the value stale value
 * when unblocked.
 *
 * Also note that there can be multiple inflight requests for the same key if
 * the first one is invalidated by a SET and then the key is evicted from the
 * cache before the second GET arrives.
 */

} // namespace hopper::inflight
