import argparse
import boto3
import botocore
import os, math
import base64
import threading
import sys 
from boto3.s3.transfer import TransferConfig
from filechunkio import FileChunkIO

s3_endpoint_url = "http://ceph-route-rook-ceph.apps.jweng-ocp.shiftstack.com"
s3_access_key_id = "QjdOMFdZNEE3NTc3MUwwMDNZT1M="
s3_secret_access_key = "cmlBWFZLa2tIaWhSaTN5Sk5FNGpxaGRlc2ZGWWtwMWZqWFpqR0FrRA=="




def s3_upload_file(args):     
    while True:
        try:
            s3 = boto3.resource('s3',
                        '',
                        use_ssl = False,
                        verify = False,
                        endpoint_url = s3_endpoint_url,
                        aws_access_key_id = base64.decodebytes(bytes(s3_access_key_id,'utf-8')).decode('utf-8'),
                        aws_secret_access_key = base64.decodebytes(bytes(s3_secret_access_key, 'utf-8')).decode('utf-8'),
                    )
            

            source_size=os.stat(args.path).st_size
        
            b = s3.get_bucket(args.bucket)
            mp = b.initiate_multipart_upload(os.path.basename(args.path))
            chunk_size = 52428800
            chunk_count = int(math.ceil(source_size / float(chunk_size)))

            # Send the file parts, using FileChunkIO to create a file-like object
            # that points to a certain byte range within the original file. We
            # set bytes to never exceed the original file size.
            
            for i in range(chunk_count):
                offset = chunk_size * i
                bytes = min(chunk_size, source_size - offset)
                with FileChunkIO(args.path, 'r', offset=offset, bytes=bytes) as fp:
                    mp.upload_part_from_file(fp, part_num=i + 1)

        except:
            #print error and abort upload so that no orphaned chunks are left taking up space
            print("Error:", sys.exc_info()[0])
            mp.cancel_upload()
            raise        
            
        # Finish the upload
        mp.complete_upload()




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
    parser.add_argument('path', metavar='PATH', type=str,
            help='Enter the Path to file to be uploaded to s3')
    parser.add_argument('bucket', metavar='BUCKET_NAME', type=str,
            help='Enter the name of the bucket to which file has to be uploaded')
     
    args = parser.parse_args()
    
    s3_upload_file(args)