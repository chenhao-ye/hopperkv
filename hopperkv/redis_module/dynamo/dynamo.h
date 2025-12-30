#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/dynamodb/DynamoDBClient.h>

#include <functional>
#include <string>

class AWSEnvironment {
 public:
  AWSEnvironment() {
    if (isInitialized) return;
    Aws::InitAPI(options);
    isInitialized = true;
  }
  ~AWSEnvironment() {
    if (!isInitialized) return;
    Aws::ShutdownAPI(options);
    isInitialized = false;
  }

 private:
  static inline Aws::SDKOptions options;
  static inline bool isInitialized = false;
};

class DynamoDB {
 public:
  DynamoDB()
      : awsEnv(), // must be initialized before dynamoClient
        dynamoClient() {}

  // async methods
  void getAsync(const std::string &tableName, const std::string &key,
                const std::function<void(std::string)> &callback_success,
                const std::function<void(std::string)> &callback_failure) const;
  void putAsync(const std::string &tableName, const std::string &key,
                const std::string &val,
                const std::function<void()> &callback_success,
                const std::function<void(std::string)> &callback_failure) const;
  void updateAsync(
      const std::string &tableName, const std::string &key,
      const std::string &val, const std::function<void()> &callback_success,
      const std::function<void(std::string)> &callback_failure) const;

  // sync methods
  std::string get(const std::string &tableName, const std::string &key) const;
  void put(const std::string &tableName, const std::string &key,
           const std::string &val) const;
  void update(const std::string &tableName, const std::string &key,
              const std::string &val) const;

 private:
  AWSEnvironment awsEnv; // must be initialized before dynamoClient
  Aws::DynamoDB::DynamoDBClient dynamoClient;

  // make it shorter to reduce network bandwidth consumption
  static constexpr char KEY_ATTRIBUTE[] = "k";
  static constexpr char VAL_ATTRIBUTE[] = "v";
};
