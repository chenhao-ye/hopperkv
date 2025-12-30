#pragma once

#include "task.h"

namespace hopper::storage {

void init();
void destroy();

void get_async(task::TaskGet* t);
void set_async(task::TaskSet* t);

void set_rcu_limit(double db_rcu);
void set_wcu_limit(double db_wcu);

void init_mock_image();
int load_mock_image(const char* image_filename);
void update_mock_format(uint32_t key_size, uint32_t val_size);

void reply_mock_format(RedisModuleCtx* ctx);

} // namespace hopper::storage
