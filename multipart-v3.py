import argparse
import boto3
import botocore
import os
import base64
import sys
import threading
from boto3.s3.transfer import TransferConfig

s3_endpoint_url = "http://ceph-route-rook-ceph.apps.jweng-ocp.shiftstack.com"
s3_access_key_id = "QjdOMFdZNEE3NTc3MUwwMDNZT1M="
s3_secret_access_key = "cmlBWFZLa2tIaWhSaTN5Sk5FNGpxaGRlc2ZGWWtwMWZqWFpqR0FrRA=="



def get_files():
    file_path = []
    path = ''
    while path != 'done':
        # ask for file path 
        path = input("Please input the path for file you want to upload or type 'done': ")

        if path != 'done':
            file_path.append(path)
    return file_path



def s3_upload_file(args):     
    file_paths = get_files()
    for path in file_paths:
        try:
            s3 = boto3.resource('s3',
                        '',
                        use_ssl = False,
                        verify = False,
                        endpoint_url = s3_endpoint_url,
                        aws_access_key_id = base64.decodebytes(bytes(s3_access_key_id,'utf-8')).decode('utf-8'),
                        aws_secret_access_key = base64.decodebytes(bytes(s3_secret_access_key, 'utf-8')).decode('utf-8'),
                    )
            
            GB = 1024 ** 3
            
                # Ensure that multipart uploads only happen if the size of a transfer
                # is larger than S3's size limit for nonmultipart uploads, which is 5 GB.
            config = TransferConfig(multipart_threshold=5 * GB, max_concurrency=10, use_threads=True)

            s3.meta.client.upload_file(path, args.bucket, os.path.basename(path),
                                                    Config=config,
                                                    Callback=ProgressPercentage(path))
            print("S3 Uploading successful")
            break
        except botocore.exceptions.EndpointConnectionError:
            print("Network Error: Please Check your Internet Connection")

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
    parser = argparse.ArgumentParser(description='UPLOAD A FILE TO PRE-EXISTING S3 BUCKET')
    parser.add_argument('bucket', metavar='BUCKET_NAME', type=str,
            help='Enter the name of the bucket to which file has to be uploaded')
     
    args = parser.parse_args()
    
    s3_upload_file(args)
