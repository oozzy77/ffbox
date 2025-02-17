#!/usr/bin/env python

from __future__ import with_statement
import os
import shutil
import subprocess
import sys
import errno
import threading
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from urllib.parse import urlparse
from fuse import FUSE, FuseOSError, Operations, fuse_get_context
from collections import defaultdict
import traceback
from botocore.exceptions import ClientError
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from boto3.s3.transfer import TransferConfig
import time

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

if aws_access_key and aws_secret_key:
    # If credentials are found, use them
    config = Config(max_pool_connections=50)
    s3_client = boto3.client('s3', config=config)
else:
    # If no credentials, use unsigned configuration
    config = Config(signature_version=UNSIGNED, max_pool_connections=50)
    s3_client = boto3.client('s3', config=config)

META_DIR = '.ffbox_noot'

uid = os.getuid()
gid = os.getgid()

class Passthrough(Operations):
    def __init__(self, root, mountpoint, s3_url = None):
        self.root = root
        self.mountpoint = mountpoint
        self.s3_url = s3_url
        parsed_url = urlparse(s3_url)
        self.bucket = parsed_url.netloc
        self.prefix = parsed_url.path.strip('/')  # Remove both leading and trailing slashes
        self.locks = defaultdict(threading.Lock)  # Automatically create a lock for each new file path
        self.cached_dir = set()
        print(f'init bucket: {self.bucket}')
        print(f'init prefix: {self.prefix}')

    def start_background_pulling(self):
        # Start a new thread to read from the S3 URL's read_order.log
        threading.Thread(target=self.read_log_and_spawn_threads, daemon=True).start()

    def read_log_and_spawn_threads(self):
        # Download the log file from the S3 URL
        log_key = f"{self.prefix}/.ffbox/read_order.log"
        try:
            response = s3_client.get_object(Bucket=self.bucket, Key=log_key)
            log_content = response['Body'].read().decode('utf-8')
            log_lines = log_content.splitlines()
            print(f'🦄 bg thread finished reading log file')
            # Create a queue and populate it with log lines
            task_queue = Queue()
            for line in log_lines:
                task_queue.put(line)
            print(f'🦄 bg thread finished populating queue')
            # Function for threads to consume tasks from the queue
            def worker():
                while not task_queue.empty():
                    line = task_queue.get()
                    try:
                        self.handle_file_operation(line)
                    except Exception as e:
                        print(f'🦄🟠 bg thread error handling file operation: {e}')
                    # finally:
                    #     task_queue.task_done()

            # Spawn 200 threads
            threads = []
            for _ in range(200):
                thread = threading.Thread(target=worker, daemon=True)
                thread.start()
                threads.append(thread)

            # Wait for all tasks to be completed
            # task_queue.join()

        except self.s3_client.exceptions.NoSuchKey:
            print(f"Log file {log_key} does not exist in bucket {self.bucket}.")
        except Exception as e:
            print(f"Error downloading log file: {e}")

    def handle_file_operation(self, log_entry):
        # Parse the log entry and perform the corresponding file operation
        if log_entry.startswith("openat"):
            rel_path = log_entry.split(" ")[1]
            abs_path = os.path.join(self.mountpoint, rel_path)
            print(f'🦄 bg thread opening file {abs_path}')
            os.open(abs_path,  os.O_RDONLY)
            # self.open(f'/{rel_path}',  os.O_RDONLY)
        elif log_entry.startswith("newfstatat") or log_entry.startswith("stat") or log_entry.startswith("lsstat"):
            rel_path = log_entry.split(" ")[1]
            abs_path = os.path.join(self.mountpoint, rel_path)
            print(f'🦄 bg thread getting attributes of {abs_path}')
            os.lstat(abs_path)
            # print(f'🦄 bg thread getting attributes of {rel_path}')
            # self.getattr(f'/{rel_path}')

    # Helpers
    # =======

    def _full_path(self, partial):
        path = os.path.join(self.root, partial.lstrip('/'))
        return path

    # Cloud s3 file operations
    # ==================

    def cloud_object_key(self, partial):
        partial = partial.strip('/')
        return f'{self.prefix}/{partial}'

    def cloud_folder_key(self, partial):
        if partial.startswith('/'):
            partial = partial[1:]
        key = f'{self.prefix}/{partial}'
        if not key.endswith('/'):
            key += '/'
        return key

    def cloud_getattr(self, path):
        parent_path = os.path.dirname(path)
        print(f'checking parent: {parent_path}')
        if self.is_folder_cached(parent_path):
            raise FuseOSError(errno.ENOENT)
        print(f'🟠 cloud getting attributes of {path}', f'parent: {parent_path}')

        response = s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.cloud_folder_key(parent_path),
            Delimiter='/'  # This makes the operation more efficient for folders
        )

        if response.get('IsTruncated'):
            print(f"🔴Warning: Directory listing for {path} is truncated!")

        # Add directories (common prefixes) to dirents
        parent_path = parent_path.lstrip('/')
        for common_prefix in response.get('CommonPrefixes', []):
            dir_name = common_prefix['Prefix'].rstrip('/').split('/')[-1]
            os.makedirs(os.path.join(self.root, parent_path, dir_name), exist_ok=True)

        # Add files to dirents
        for obj in response.get('Contents', []):
            file_name = obj['Key'].split('/')[-1]
            # Create an empty placeholder file with the attributes
            file_path = os.path.join(self.root, parent_path, file_name)
            if not os.path.exists(file_path):
                # Create a sparse file of the same size as the S3 object
                with open(file_path, 'wb') as f:
                    f.truncate(obj['Size'])  # Create sparse file of exact size
                # Set file attributes
                os.utime(file_path, (obj['LastModified'].timestamp(), obj['LastModified'].timestamp()))
        # mark this path as completed cached
        self.mark_folder_cached(parent_path)

    def cloud_readdir(self, path):
        print(f'🟠reading cloud directory path: {path}')
        # List objects in the S3 bucket with the specified prefix
        response = s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self.cloud_folder_key(path), Delimiter='/')        
        
        yield '.'
        yield '..'

        # Add directories (common prefixes) to dirents
        if 'CommonPrefixes' in response:
            for common_prefix in response['CommonPrefixes']:
                dir_name = common_prefix['Prefix'].rstrip('/').split('/')[-1]
                yield dir_name
                os.makedirs(os.path.join(self.root, path.lstrip('/'), dir_name), exist_ok=True)
                

        # Add files to dirents
        if 'Contents' in response:
            for obj in response['Contents']:
                file_name = obj['Key'].split('/')[-1]
                yield file_name
                
                # Create an empty placeholder file with the attributes
                file_path = os.path.join(self.root, path.lstrip('/'), file_name)
                if not os.path.exists(file_path):
                    # Create a sparse file of the same size as the S3 object
                    with open(file_path, 'wb') as f:
                        f.truncate(obj['Size'])  # Create sparse file of exact size
                    # Set file attributes
                    os.utime(file_path, (obj['LastModified'].timestamp(), obj['LastModified'].timestamp()))

        # mark this path as completed cached
        self.mark_folder_cached(path)

    def is_folder_cached(self, path):
        if path in self.cached_dir:
            return True
        try:
            is_complete = os.getxattr(self._full_path(path), 'user.is_complete')
            return is_complete == b'1'
        except OSError:
            # If the xattr does not exist, proceed with downloading
            return False
    
    def mark_folder_cached(self, path):
        os.setxattr(self._full_path(path), 'user.is_complete', b'1')
        self.cached_dir.add(path)
    
    def is_file_cached(self, path):
        if path in self.cached_dir:
            return True
        try:
            is_complete = os.getxattr(self._full_path(path), 'user.is_complete')
            return is_complete == b'1'
        except OSError:
            # If the xattr does not exist, proceed with downloading
            return False
    
    def mark_file_cached(self, path):
        os.setxattr(self._full_path(path), 'user.is_complete', b'1')
        self.cached_dir.add(path)

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        print(f'👇getting attribute of {path}')
        full_path = self._full_path(path)

        with self.locks[path]:
            if not os.path.exists(full_path):
                self.cloud_getattr(path)
            st = os.lstat(full_path)
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        print(f'👇reading directory {path}')
        
        if self.is_folder_cached(path):
            yield '.'
            yield '..'
            # Add more entries as needed
            for entry in os.listdir(self._full_path(path)):
                yield entry
        else:
            with self.locks[path]:
                yield from self.cloud_readdir(path)

    def readlink(self, path):
        print('👇 reading link', path)
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        print(f'👇making directory {path}')
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(target, self._full_path(name))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(name), self._full_path(target))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        print(f'👇opening file {path}')
        
        if self.is_file_cached(path):
            return os.open(self._full_path(path), flags)

        # Acquire the lock to download the file
        with self.locks[path]:
            # Double-check if the file was downloaded while waiting for the lock
            if self.is_file_cached(path):
                return os.open(self._full_path(path), flags)
                
            full_path = self._full_path(path)
            try:
                # cloud_url = f's3://{self.bucket}/{self.cloud_object_key(path)}'
                print(f'🟠 cloud open file {path}, downloading to {full_path}')
                # Attempt download with retries
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        s3_client.download_file(
                            self.bucket,
                            self.cloud_object_key(path),
                            full_path
                        )
                        # self.download_file(cloud_url, full_path)

                        print(f'🟢 Download successful to {full_path}')
                        break  # Exit retry loop on success
                        
                    except Exception as e:
                        if isinstance(e, ClientError) and e.response['Error']['Code'] == '404':
                            print("🔴 open The object does not exist.")
                            raise FuseOSError(errno.ENOENT)
                        # Clean up partial download
                        if os.path.exists(full_path):
                            os.unlink(full_path)
                        if attempt < max_retries - 1:
                            print(f'🔴Retrying download (attempt {attempt + 2}/{max_retries})')
                        else:
                            raise FuseOSError(errno.EIO)  # Raise error after final attempt
                    
                print(f'🔵 downloaded to {full_path}')
                
                # Mark as cached
                self.mark_file_cached(path)
                    
                return os.open(full_path, flags)
                
            except Exception as e:
                print(f'🔴 error downloading to {full_path}: {e}')
                traceback.print_exc()
                # Clean up any partial downloads
                if os.path.exists(full_path):
                    os.unlink(full_path)
                raise FuseOSError(errno.EIO)
                
    def read(self, path, length, offset, fh):
        print(f'👇reading file {path}')
        # Check file download status
        with self.locks[path]:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)

    def create(self, path, mode, fi=None):
        print('👇 creating file')
        with self.locks[path]:
            uid, gid, pid = fuse_get_context()
            full_path = self._full_path(path)
            fd = os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
            os.chown(full_path,uid,gid) #chown to context uid & gid
            return fd

    def write(self, path, buf, offset, fh):
        print('👇 writing file')
        with self.locks[path]:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        print('👇 truncating file')
        with self.locks[path]:
            full_path = self._full_path(path)
            with open(full_path, 'r+') as f:
                f.truncate(length)

    def flush(self, path, fh):
        print('👇 flushing file')
        with self.locks[path]:
            return os.fsync(fh)

    def release(self, path, fh):
        print('👇 releasing file')
        with self.locks[path]:
            return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        print('👇 fsyncing file')
        with self.locks[path]:
            return self.flush(path, fh)


def check_upload_complete(local_dir, s3_url):
    local_dir = os.path.expanduser(local_dir)
    if not os.path.exists(local_dir):
        print(f'🔴 local directory {local_dir} does not exist')
        return
    if s3_url.startswith('s3://'):
        s3_prefix = '/'.join(s3_url.split('://')[1:])
    s3_bucket_name = s3_prefix.split('/')[0]
    s3_prefix = '/'.join(s3_prefix.split('/')[1:])
    for root, dirs, files in os.walk(local_dir):
        s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix)
        for file in files:
            if file == '.ffbox_dir_meta.json':
                print(f'🔴 .ffbox_dir_meta.json is a reserved file name')
                continue
            child_path = os.path.join(root, file)
            rel_path = os.path.relpath(child_path, local_dir)
            object_key = f'{s3_prefix}/{rel_path}'.strip('/')
            print(f'👇 checking {object_key}')
    return

# Upload file with multipart upload
config = TransferConfig(
    multipart_threshold=1024 * 25,  # 25MB threshold for multipart uploads
    max_concurrency=10,  # Max parallel uploads
    use_threads=True  # Use threading for faster uploads
)
def ffpush(local_dir, s3_url):
    # TODO: to be implemented
    print(f'pushing from {local_dir} to s3 {s3_url}')
    if not aws_access_key or not aws_secret_key:
        print(f'🔴 no aws credentials found, please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY')
        return
    start_time = time.time()
    local_dir = os.path.expanduser(local_dir)
    if not os.path.exists(local_dir):
        print(f'🔴 local directory {local_dir} does not exist')
        return
    if s3_url.startswith('s3://'):
        s3_prefix = '/'.join(s3_url.split('://')[1:])
    else:
        s3_prefix = s3_url
    s3_bucket_name = s3_prefix.split('/')[0]
    s3_prefix = '/'.join(s3_prefix.split('/')[1:])
    
    # Local helper function to upload metadata for one directory
    def upload_meta(root, dirs, files, idx, folder_count):
        children_stats = {}
        for file in files:
            if file == '.ffbox_dir_meta.json':
                print(f'🔴 .ffbox_dir_meta.json is a reserved file name')
                continue
            child_path = os.path.join(root, file)
            stats = os.stat(child_path)
            rel_path = os.path.relpath(child_path, local_dir)
            object_key = f'{s3_prefix}/{rel_path}'.strip('/')
            children_stats[file] = {
                "size": stats.st_size,           # Size in bytes
                "modified_time": stats.st_mtime,   # Last modified time
                "created_time": stats.st_ctime,    # Creation time
                "url": f's3://{s3_bucket_name}/{object_key}',
            }
            print(f'👇 uploading {idx + 1}/{folder_count} {child_path} to s3://{s3_bucket_name}')

            s3_client.upload_file(child_path, s3_bucket_name, object_key, Config=config)

        for d in dirs:
            if d == '.ffbox_dir_meta.json':
                print(f'🔴 .ffbox_dir_meta.json is a reserved file name')
                continue
            child_path = os.path.join(root, d)
            rel_path = os.path.relpath(child_path, local_dir)
            object_key = f'{s3_prefix}/{rel_path}'.strip('/')
            children_stats[d] = {
                "dir": True,
                "url": f's3://{s3_bucket_name}/{object_key}',
            }
        rel_path = os.path.relpath(root, local_dir)
        if rel_path == '.':
            rel_path = ''
        key = '/'.join([x for x in [s3_prefix, rel_path, '.ffbox_dir_meta.json'] if x != ''])
        print(f'👇 putting meta to s3://{s3_bucket_name}/{key}')
        s3_client.put_object(
            Bucket=s3_bucket_name, 
            Key=key, 
            Body=json.dumps(children_stats)
        )

    # Collect all directories using os.walk so we can process them concurrently
    directories = list(os.walk(local_dir))
    folder_count = len(directories)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(upload_meta, root, dirs, files, idx, folder_count)
            for idx, (root, dirs, files) in enumerate(directories)]
        for future in as_completed(futures):
            # This will re-raise any exceptions thrown in upload_meta
            future.result()
    end_time = time.time()
    print(f'👇 folder count: {folder_count}')
    print(f'👇 time taken: {end_time - start_time} seconds')
        

def ffmount(s3_url, mountpoint, cache_dir=None, foreground=True, clean_cache=False):
    fake_path = os.path.abspath(mountpoint)
    if cache_dir is None:
        home_dir = os.path.expanduser("~")
        cache_dir = os.path.join(home_dir, '.cache', 'ffbox')
    if s3_url:
        s3_bucket_name = '/'.join(s3_url.split('://')[1:])
        print(f's3 bucket name: {s3_bucket_name}')
        real_path = os.path.join(cache_dir, s3_bucket_name)
    else:
        if mountpoint.startswith('/'):
            mountpoint = mountpoint[1:]
        real_path = os.path.join(cache_dir, mountpoint)
    if os.path.exists(fake_path):
        print(f"Warning: {fake_path} already exists, do you want to override?")
        if input("y/n: ") != "y":
            print("Exiting")
            return
        else:
            shutil.rmtree(fake_path)
    if clean_cache and os.path.exists(real_path):
        shutil.rmtree(real_path)
    os.makedirs(fake_path, exist_ok=True)
    os.makedirs(real_path, exist_ok=True)

    print(f"real storage path: {real_path}, fake storage path: {fake_path}")
    passthru = Passthrough(real_path, fake_path, s3_url)
    passthru.start_background_pulling()
    FUSE(passthru, fake_path, foreground=foreground)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="ffbox CLI tool for S3 operations.")
    subparsers = parser.add_subparsers(dest="command")

    # Mount command
    parser_mount = subparsers.add_parser("mount", help="Mount an S3 bucket to a local directory")
    parser_mount.add_argument("s3_url", help="URL of the S3 bucket")
    parser_mount.add_argument("mountpoint", help="Local directory to mount the S3 bucket to")
    parser_mount.add_argument("--clean", action="store_true", help="Clean the cache directory before mounting")
    parser_mount.add_argument("--cache-dir", help="Cache directory to use")

    # Push command
    parser_push = subparsers.add_parser("push", help="Push a local directory to an S3 bucket")
    parser_push.add_argument("local_dir", help="Local directory containing files to push")
    parser_push.add_argument("s3_url", help="S3 URL to push files to")


    args = parser.parse_args()

    if args.command == "mount":
        ffmount(args.s3_url, args.mountpoint, cache_dir=args.cache_dir, clean_cache=args.clean)
    elif args.command == "push":
        ffpush(args.local_dir, args.s3_url)
    else:
        parser.print_help()
