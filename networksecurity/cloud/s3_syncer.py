
# import os


# class S3Sync:
#     def sync_folder_to_s3(self,folder,aws_bucket_url):
#         command = f"aws s3 sync {folder} {aws_bucket_url} "
#         os.system(command)

#     def sync_folder_from_s3(self,folder,aws_bucket_url):
#         command = f"aws s3 sync  {aws_bucket_url} {folder} "
#         os.system(command)

import os
import sys
import boto3
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging


class S3Sync:
    def __init__(self):
        self.s3_client = boto3.client("s3")

    def sync_folder_to_s3(self, folder: str, aws_bucket_url: str):
        try:
            bucket_name = aws_bucket_url.replace("s3://", "").split("/")[0]
            s3_prefix = "/".join(aws_bucket_url.replace("s3://", "").split("/")[1:])

            logging.info(f"Sync local → S3 : {folder} → {aws_bucket_url}")

            for root, _, files in os.walk(folder):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, folder)
                    s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")

                    self.s3_client.upload_file(local_path, bucket_name, s3_key)

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def sync_folder_from_s3(self, folder: str, aws_bucket_url: str):
        try:
            bucket_name = aws_bucket_url.replace("s3://", "").split("/")[0]
            s3_prefix = "/".join(aws_bucket_url.replace("s3://", "").split("/")[1:])

            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
                for obj in page.get("Contents", []):
                    s3_key = obj["Key"]
                    local_path = os.path.join(
                        folder,
                        s3_key.replace(s3_prefix, "").lstrip("/")
                    )
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    self.s3_client.download_file(bucket_name, s3_key, local_path)

        except Exception as e:
            raise NetworkSecurityException(e, sys)
