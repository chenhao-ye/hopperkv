#include "dynamo.h"

#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/PutItemResult.h>
#include <aws/dynamodb/model/UpdateItemRequest.h>
#include <aws/dynamodb/model/UpdateItemResult.h>

#include <stdexcept>

using Aws::DynamoDB::DynamoDBClient;
using namespace Aws::DynamoDB::Model;

void DynamoDB::getAsync(
    const std::string &tableName, const std::string &key,
    const std::function<void(std::string)> &callback_success,
    const std::function<void(std::string)> &callback_failure) const {
  auto req = GetItemRequest()
                 .WithTableName(tableName)
                 .WithKey({
                     {KEY_ATTRIBUTE, AttributeValue(key)},
                 })
                 .WithConsistentRead(false) // default is false
                 .WithProjectionExpression(VAL_ATTRIBUTE);

  dynamoClient.GetItemAsync(
      req, [=](const DynamoDBClient *client, const GetItemRequest &request,
               const GetItemOutcome &outcome,
               const std::shared_ptr<const Aws::Client::AsyncCallerContext>
                   &context) {
        if (!outcome.IsSuccess())
          return callback_failure(outcome.GetError().GetMessage());

        auto &&item = outcome.GetResult().GetItem();
        if (item.empty())
          return callback_failure(std::string("Item not found for key <") +
                                  key + "> in table <" + tableName + ">");

        return callback_success(item.at(VAL_ATTRIBUTE).GetS());
      });
}

void DynamoDB::putAsync(
    const std::string &tableName, const std::string &key,
    const std::string &val, const std::function<void()> &callback_success,
    const std::function<void(std::string)> &callback_failure) const {
  auto request = PutItemRequest().WithTableName(tableName).WithItem({
      {KEY_ATTRIBUTE, AttributeValue(key)},
      {VAL_ATTRIBUTE, AttributeValue(val)},
  });

  dynamoClient.PutItemAsync(
      request, [=](const DynamoDBClient *client, const PutItemRequest &request,
                   const PutItemOutcome &outcome,
                   const std::shared_ptr<const Aws::Client::AsyncCallerContext>
                       &context) {
        if (!outcome.IsSuccess())
          return callback_failure(outcome.GetError().GetMessage());

        return callback_success();
      });
}

void DynamoDB::updateAsync(
    const std::string &tableName, const std::string &key,
    const std::string &val, const std::function<void()> &callback_success,
    const std::function<void(std::string)> &callback_failure) const {
  auto request = UpdateItemRequest()
                     .WithTableName(tableName)
                     .WithKey({{
                         KEY_ATTRIBUTE,
                         AttributeValue(key),
                     }})
                     .WithAttributeUpdates({{
                         VAL_ATTRIBUTE,
                         AttributeValueUpdate().WithValue(AttributeValue(val)),
                     }});

  dynamoClient.UpdateItemAsync(
      request,
      [&](const DynamoDBClient *client, const UpdateItemRequest &request,
          const UpdateItemOutcome &outcome,
          const std::shared_ptr<const Aws::Client::AsyncCallerContext>
              &context) {
        if (!outcome.IsSuccess())
          return callback_failure(outcome.GetError().GetMessage());

        return callback_success();
      });
}

std::string DynamoDB::get(const std::string &tableName,
                          const std::string &key) const {
  std::promise<std::string> promise;
  getAsync(
      tableName, key, [&](std::string val) { promise.set_value(val); },
      [&](std::string err_msg) {
        throw std::runtime_error("Failed to get item <" + key +
                                 "> from table <" + tableName +
                                 ">: " + err_msg);
      });
  return promise.get_future().get();
}

void DynamoDB::put(const std::string &tableName, const std::string &key,
                   const std::string &val) const {
  std::promise<void> promise;
  putAsync(
      tableName, key, val, [&]() { promise.set_value(); },
      [&](std::string err_msg) {
        throw std::runtime_error("Failed put get item <" + key +
                                 "> from table <" + tableName +
                                 ">: " + err_msg);
      });
  promise.get_future().get();
}

void DynamoDB::update(const std::string &tableName, const std::string &key,
                      const std::string &val) const {
  std::promise<void> promise;
  updateAsync(
      tableName, key, val, [&]() { promise.set_value(); },
      [&](std::string err_msg) {
        throw std::runtime_error("Failed to update item <" + key +
                                 "> from table <" + tableName +
                                 ">: " + err_msg);
      });
  promise.get_future().get();
}
