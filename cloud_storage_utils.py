"""Class to work with cloud storage files
"""
from google.cloud import exceptions as google_cloud_errors
from google.cloud import storage
import google.api_core.exceptions
import os
import tenacity

# ConnectionResetError
MAX_RETRIES_COUNT = 10
RETRY_DELAY = 1

class CloudStorageService:
  def __init__(self, bucket_name):
    self.bucket_name = bucket_name
    self.storage_client = storage.Client()

  @tenacity.retry(wait=tenacity.wait_fixed(RETRY_DELAY), stop=tenacity.stop_after_attempt(MAX_RETRIES_COUNT))
  def merge_files_into_one(self, main_file, prefix):
    bucket = self.storage_client.get_bucket(self.bucket_name)
    main_blob = bucket.blob(main_file)
    for blob in bucket.list_blobs(prefix=prefix):
      if blob.name == main_blob.name:
        continue

      main_blob.compose([main_blob, blob])
      blob.delete()

  @tenacity.retry(wait=tenacity.wait_fixed(RETRY_DELAY), stop=tenacity.stop_after_attempt(MAX_RETRIES_COUNT))
  def upload_from_file(self, file_to_upload, destination_name):
    file_to_upload.seek(0)
    bucket = self.storage_client.get_bucket(self.bucket_name)
    blob = bucket.blob(destination_name)
    return blob.upload_from_file(file_to_upload, content_type='application/octet-stream')


  @tenacity.retry(wait=tenacity.wait_fixed(RETRY_DELAY), stop=tenacity.stop_after_attempt(MAX_RETRIES_COUNT))
  def upload_string(self, string, destination_name):
    bucket = self.storage_client.get_bucket(self.bucket_name)
    blob = bucket.blob(destination_name)
    blob.upload_from_string(string, content_type='application/octet-stream')

  @tenacity.retry(wait=tenacity.wait_fixed(RETRY_DELAY), stop=tenacity.stop_after_attempt(MAX_RETRIES_COUNT))
  def download_to_file(self, file_to_download, destination_file):
    bucket = self.storage_client.get_bucket(self.bucket_name)
    blob = bucket.blob(file_to_download)
    return blob.download_to_file(destination_file)

  @tenacity.retry(wait=tenacity.wait_fixed(RETRY_DELAY), stop=tenacity.stop_after_attempt(MAX_RETRIES_COUNT))
  def download_string(self, file_to_download):
    try:
      bucket = self.storage_client.get_bucket(self.bucket_name)
      blob = bucket.blob(file_to_download)
      return blob.download_as_string().decode("utf-8")
    # file not found, return blank string
    except (google_cloud_errors.NotFound) as e:
      print(e)
      return ''

  @tenacity.retry(wait=tenacity.wait_fixed(RETRY_DELAY), stop=tenacity.stop_after_attempt(MAX_RETRIES_COUNT))
  def file_exists(self, destination_name):
    bucket = self.storage_client.get_bucket(self.bucket_name)
    blob = bucket.blob(destination_name)
    return blob.exists()

  @tenacity.retry(wait=tenacity.wait_fixed(RETRY_DELAY), stop=tenacity.stop_after_attempt(MAX_RETRIES_COUNT))
  def delete_file(self, destination_name):
    bucket = self.storage_client.get_bucket(self.bucket_name)
    blob = bucket.blob(destination_name)
    blob.delete()
