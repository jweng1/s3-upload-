import threading
import boto3
import os
import base64
import sys
from boto3.s3.transfer import TransferConfig


s3_endpoint_url = "http://ceph-route-rook-ceph.apps.jweng-ocp.shiftstack.com"
s3_access_key_id = "QjdOMFdZNEE3NTc3MUwwMDNZT1M="
s3_secret_access_key = "cmlBWFZLa2tIaWhSaTN5Sk5FNGpxaGRlc2ZGWWtwMWZqWFpqR0FrRA=="
s3_bucket = "j-bucket"

s3 = boto3.client('s3',
    '',
    use_ssl = False,
    verify = False,
    endpoint_url = s3_endpoint_url,
    aws_access_key_id = base64.decodebytes(bytes(s3_access_key_id,'utf-8')).decode('utf-8'),
    aws_secret_access_key = base64.decodebytes(bytes(s3_secret_access_key, 'utf-8')).decode('utf-8'),
    )

s3r = boto3.resource('s3',
    '',
    use_ssl = False,
    verify = False,
    endpoint_url = s3_endpoint_url,
    aws_access_key_id = base64.decodebytes(bytes(s3_access_key_id,'utf-8')).decode('utf-8'),
    aws_secret_access_key = base64.decodebytes(bytes(s3_secret_access_key, 'utf-8')).decode('utf-8'),
)

# response = s3.list_buckets()
# # Get a list of all bucket names from the response
# buckets = [bucket['Name'] for bucket in response['Buckets']]

# # Print out the bucket list
# print("Initial bucket List: %s" % buckets)

# #s3r.Bucket("MyBucket").objects.all().delete()
# #s3.delete_bucket(Bucket="MyBucket")

# print("Trying to make 'mybucket'")
# if s3_bucket not in buckets:
#     s3.create_bucket(Bucket=s3_bucket)
# else: 
#     print("Bucket " + s3_bucket + " already exists, deleting and recreating")
#     s3r.Bucket(s3_bucket).objects.all().delete()
#     s3.delete_bucket(Bucket=s3_bucket)
#     s3.create_bucket(Bucket=s3_bucket)

def multi_part_upload_with_s3():
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    file_path ='demo-files/tot_manU.mp4'
    key_path = 'demo-files/tot_manU.mp4'
    s3.upload_file(file_path, s3_bucket, key_path,
                            Config=config,
                            Callback=ProgressPercentage(file_path)
                  )


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

if __name__ == '__main__': 
    multi_part_upload_with_s3()


