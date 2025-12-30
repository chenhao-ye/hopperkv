#include <atomic>
#include <chrono>
#include <iostream>
#include <string>

#include "dynamo.h"

std::string tableName = "test-table";

std::atomic<int> num_inflight_avail_w = 64;
std::atomic<int> num_inflight_avail_r = 16;

void test_basic(const DynamoDB &db) {
  std::string key = "test-key";

  auto start_ts = std::chrono::high_resolution_clock::now();

  {
    std::string val = db.get(tableName, key);
    std::cout << "Get(\"" << key << "\") = \"" << val << "\"" << std::endl;
  }

  {
    std::string val = "new-value";
    db.update(tableName, key, val);
    std::cout << "Update(\"" << key << "\", \"" << val << "\")" << std::endl;
  }

  {
    std::string val = db.get(tableName, key);
    std::cout << "Get(\"" << key << "\") = \"" << val << "\"" << std::endl;
  }

  auto end_ts = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end_ts - start_ts;
  std::cout << "Elapsed time: " << elapsed.count() << "s" << std::endl;
}

int main(int argc, char **argv) {
  DynamoDB db;

  test_basic(db);

  sleep(1); // wait for aws threads pool to finish callbacks
}
