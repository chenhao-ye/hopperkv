## AWS CLI

1. Install awscli following the instructions in the link below.

    https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html


2. Configure the aws sso login

    ```
    aws configure sso
    ```

3. Check the aws profile

    ```
    cat ~/.aws/config
    ```

4. Set the AWS_PROFILE environment variable to the profile you want to use.

    ```
    export AWS_PROFILE=default
    ```

## S3

1. Create the `hopperkv` bucket if it doesn't exist.

    ```
    aws s3 mb s3://hopperkv
    ```

2. Upload `test_table.csv` to the `s3://hopperkv` bucket.

    ```
    aws s3 cp test_table.csv s3://hopperkv/
    ```

    or 

    ```
    aws s3 sync folder-path s3://hopperkv
    ```

3. List all objects in the `s3://hopperkv` bucket.

    ```
    aws s3 ls s3://hopperkv/
    ```

4. Delete `test_table.csv` from the `s3://hopperkv` bucket.

    ```
    aws s3 rm s3://hopperkv/test_table.csv
    ```

5. Delete the `hopperkv` bucket.

    ```
    aws s3 rb s3://hopperkv
    ```


## DynamoDB

Refer to the following link for the list of commands that can be used with the awscli.

https://awscli.amazonaws.com/v2/documentation/api/latest/reference/dynamodb/index.html

1. Create a table.

    - Create table with provisioned billing mode

    ```
    aws dynamodb \
      create-table \
      --table-name test-table \
      --attribute-definitions AttributeName=k,AttributeType=S \
      --key-schema AttributeName=k,KeyType=HASH \
      --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1
    ```

    - Create table with on-demand billing mode

    ```
    aws dynamodb \
      create-table \
      --table-name test-table \
      --attribute-definitions AttributeName=k,AttributeType=S \
      --key-schema AttributeName=k,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST
    ```

    - Import data from S3

    ```
    aws dynamodb \
      import-table \
      --s3-bucket-source S3Bucket=hopperkv,S3KeyPrefix=test_table.csv \
      --input-format CSV \
      --input-format-options '{
        "Csv": {
          "Delimiter": ",",
          "HeaderList": ["k", "v"]
        }
      }' \
      --table-creation-parameters '{
        "TableName": "test-table",
        "AttributeDefinitions": [
          {
            "AttributeName": "k",
            "AttributeType": "S"
          }
        ],
        "KeySchema": [
          {
            "AttributeName": "k",
            "KeyType": "HASH"
          }
        ],
        "BillingMode": "PAY_PER_REQUEST"
      }'
    ```

2. Other table operations
    - List tables

    ```
    aws dynamodb list-tables
    ```

    - Describe table

    ```
    aws dynamodb describe-table --table-name test-table
    ```

    - List items in table

    ```
    aws dynamodb scan --table-name test-table
    ```

    - Delete table

    ```
    aws dynamodb delete-table --table-name test-table
    ```

3. Item operations

    - Put item in table

    ```
    aws dynamodb put-item --table-name test-table --item '{"k": {"S": "test-key"}, "v": {"S": "test-value"}}'
    ```

    - Get item from table

    ```
    aws dynamodb get-item --table-name test-table --key '{"k": {"S": "test-key"}}'
    ```

    - Update item in table

    ```
    aws dynamodb update-item --table-name test-table --key '{"k": {"S": "test-key"}}' --attribute-updates '{"v": {"Value": {"S": "new-value"}, "Action": "PUT"}}'
    ```
