#!/usr/bin/env python3
import logging
import sys
import time
from typing import List

import boto3
from botocore.exceptions import ClientError


class DynamoDBManager:
    """Manages DynamoDB backup and recovery operations."""

    def __init__(self, region: str = "us-east-2"):
        """Initialize the DynamoDB manager.

        Args:
            region: AWS region for DynamoDB operations
        """
        self.region = region
        try:
            self.dynamodb = boto3.client("dynamodb", region_name=region)
            self.logger = logging.getLogger(__name__)
        except Exception as e:
            logging.error(f"Failed to initialize DynamoDB client: {e}")
            sys.exit(1)

    def create_backup(self, table_name: str, backup_name: str) -> bool:
        """Create a backup of a DynamoDB table.

        Args:
            table_name: Name of the table to backup
            backup_name: Name for the backup

        Returns:
            True if backup creation was successful, False otherwise
        """
        try:
            response = self.dynamodb.create_backup(
                TableName=table_name, BackupName=backup_name
            )
            backup_arn = response["BackupDetails"]["BackupArn"]
            self.logger.info(
                f"Created backup '{backup_name}' for table '{table_name}': {backup_arn}"
            )
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "BackupInUseException":
                self.logger.warning(f"Backup '{backup_name}' already exists")
                return True
            else:
                self.logger.error(f"Failed to create backup '{backup_name}': {e}")
                return False
        except Exception as e:
            self.logger.error(f"Unexpected error creating backup '{backup_name}': {e}")
            return False

    def create_multiple_backups(
        self, table_name: str, backup_count: int, backup_prefix: str | None = None
    ) -> List[bool]:
        """Create multiple backups of a DynamoDB table.

        Args:
            table_name: Name of the table to backup
            backup_count: Number of backups to create
            backup_prefix: Prefix for backup names (defaults to table_name)

        Returns:
            List of boolean results for each backup creation
        """
        if backup_prefix is None:
            backup_prefix = table_name

        results = []
        self.logger.info(f"Creating {backup_count} backups for table '{table_name}'")

        for i in range(backup_count):
            backup_name = f"{backup_prefix}_{i}"
            success = self.create_backup(table_name, backup_name)
            results.append(success)

            # Add small delay to avoid rate limiting
            if i < backup_count - 1:
                time.sleep(0.1)

        successful = sum(results)
        self.logger.info(f"Successfully created {successful}/{backup_count} backups")
        return results

    def list_backups(
        self, table_name: str | None = None, limit: int = 50
    ) -> List[dict]:
        """List DynamoDB backups.

        Args:
            table_name: Filter backups by table name (optional)
            limit: Maximum number of backups to return

        Returns:
            List of backup information dictionaries
        """
        try:
            kwargs = {"Limit": limit}
            if table_name:
                kwargs["TableName"] = table_name

            response = self.dynamodb.list_backups(**kwargs)
            backups = response.get("BackupSummaries", [])

            self.logger.info(f"Found {len(backups)} backups")
            return backups
        except ClientError as e:
            self.logger.error(f"Failed to list backups: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error listing backups: {e}")
            return []

    def restore_from_backup(self, backup_arn: str, target_table_name: str) -> bool:
        """Restore a table from a backup.

        Args:
            backup_arn: ARN of the backup to restore from
            target_table_name: Name for the restored table

        Returns:
            True if restore operation was initiated successfully, False otherwise
        """
        try:
            self.dynamodb.restore_table_from_backup(
                TargetTableName=target_table_name, BackupArn=backup_arn
            )
            self.logger.info(
                f"Restore operation initiated for table '{target_table_name}' from backup {backup_arn}"
            )
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "TableAlreadyExistsException":
                self.logger.error(f"Table '{target_table_name}' already exists")
            else:
                self.logger.error(f"Failed to restore from backup: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error restoring from backup: {e}")
            return False

    def restore_from_backup_name(
        self, backup_name: str, target_table_name: str
    ) -> bool:
        """Restore a table from a backup by backup name.

        Args:
            backup_name: Name of the backup to restore from
            target_table_name: Name for the restored table

        Returns:
            True if restore operation was initiated successfully, False otherwise
        """
        # First, find the backup by name
        backups = self.list_backups()
        backup_arn = None

        for backup in backups:
            if backup["BackupName"] == backup_name:
                backup_arn = backup["BackupArn"]
                break

        if not backup_arn:
            self.logger.error(f"Backup '{backup_name}' not found")
            return False

        return self.restore_from_backup(backup_arn, target_table_name)


def main():
    manager = DynamoDBManager(region="us-east-2")

    try:
        count = 24
        source_prefix = "trace_table"
        target_prefix = "trace_table_4"

        results = []
        print(f"Restoring {count} backups from {source_prefix}_* to {target_prefix}_*")

        for i in range(count):
            source_backup = f"{source_prefix}_{i}"
            target_table = f"{target_prefix}_{i:02d}"

            print(f"Restoring {source_backup} to {target_table}...")
            success = manager.restore_from_backup_name(source_backup, target_table)
            results.append(success)

            if success:
                print(
                    f"✓ Successfully initiated restore: {source_backup} -> {target_table}"
                )
            else:
                print(f"✗ Failed to restore: {source_backup} -> {target_table}")

        successful = sum(results)
        print(f"\nRestore operations completed: {successful}/{count} successful")

        if not all(results):
            sys.exit(1)

    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
