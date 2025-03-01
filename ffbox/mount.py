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
DIR_META_FILE = '.ffbox_dir_meta.json'

uid = os.getuid()
gid = os.getgid()

class NsClient: # Network storage client
    def __init__(self, url: str):
        self.root = url

    def get_object(self, relpath: str) -> str:
        raise Exception('Please implement me!')

nsclient:NsClient = None

class S3Client(NsClient):
    def __init__(self, url: str):
        super().__init__(url)
        parsed_url = urlparse(url)
        self.bucket = parsed_url.netloc
        self.prefix = parsed_url.path.strip('/')  # Remove both leading and trailing slashes

    def get_object(self, relpath: str): 
        response = s3_client.get_object(
            Bucket=self.bucket,
            Key=f'{self.prefix}/{relpath}'
        )
        return response['Body'].read().decode('utf-8')

class PathClient(NsClient):
    def __init__(self, url: str):
        super().__init__(url)
        self.source = url.rstrip('/') # source folder

    def get_object(self, relpath: str):
        with open(os.path.join(self.source, relpath), 'r') as file:
            content = file.read()
        return content

class Passthrough(Operations):
    def __init__(self, root, mountpoint, s3_url = None, is_ffbox_folder = False):
        self.root = root
        self.mountpoint = mountpoint
        self.s3_url = s3_url
        self.is_ffbox_folder = is_ffbox_folder
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
            print(f'🟠 parent folder {parent_path} is cached so {path} not exists')
            raise FuseOSError(errno.ENOENT)
        with self.locks[path]:
            self.cloud_readdir(parent_path)


    def cloud_readdir(self, parent_path: str):
        print('🟠 cloud cloud_readdir', parent_path)
        parent_path = parent_path.lstrip('/')
        if self.is_ffbox_folder:
            try:
                url = os.getxattr(self._full_path(parent_path), 'user.url').decode('utf-8').rstrip('/')
                print('🟠 cloud cloud_readdir path', parent_path, 'url', url)
                json_str = nsclient.get_object(f'{url}/{DIR_META_FILE}')
                response = json.loads(json_str)
                for file_name in response:
                    print('filename', file_name, 'parentpath',parent_path)
                    attr = response[file_name]
                    size = attr.get('size')
                    ctime = attr.get('ctime')
                    url = attr.get('url')
                    if url is None:
                        print('🔴 error getting url of file {parent_path}/{file_name}')
                        continue
                    if ctime is None:
                        ctime = time.time()
                    mtime = attr.get('mtime')
                    if mtime is None:
                        mtime = time.time()
                    file_path = os.path.join(self.root, parent_path, file_name)
                    if size is None: # is folder
                        os.makedirs(file_path, exist_ok=True)
                        os.setxattr(file_path, 'user.url', url.encode('utf-8'))
                    else: # is file 
                        # Create a sparse file of the same size as the S3 object
                        print('creating sparse file', file_path)
                        if not os.path.exists(file_path):
                            with open(file_path, 'wb') as f:
                                f.truncate(size)  # Create sparse file of exact size
                            # Set file attributes
                            os.utime(file_path, (mtime, mtime))
                            os.setxattr(file_path, 'user.url', url.encode('utf-8'))
            except Exception as e:
                print('🔴 error reading url of', self._full_path(parent_path))
                traceback.print_exc()
                raise FuseOSError(errno.ENOENT)
        else:
            response = s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.cloud_folder_key(parent_path),
                Delimiter='/'  # This makes the operation more efficient for folders
            )

            if response.get('IsTruncated'):
                print(f"🔴Warning: Directory listing for {parent_path} is truncated!")

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

        if not os.path.exists(full_path):
            self.cloud_getattr(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                    'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        print(f'👇reading directory {path}')
        if not self.is_folder_cached(path):
            with self.locks[path]:
                self.cloud_readdir(path)
        yield '.'
        yield '..'
        for entry in os.listdir(self._full_path(path)):
            yield entry
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
        ret = os.mkdir(self._full_path(path), mode)
        self.mark_folder_cached(path)
        return ret

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
        fullpath = self._full_path(path)
        try:
            url = os.getxattr(fullpath, 'user.url').decode('utf-8').rstrip('/')
        except Exception as e:
            return os.open(fullpath, flags)
        if url.startswith('/'):
            # Check if any write flags are present
            write_flags = os.O_WRONLY | os.O_RDWR | os.O_APPEND
            if flags & write_flags:
                with self.locks[path]:
                    print(f'🟠 Opening local file {path} for writing')
                    # copy content from url to fullpath, but keep the metadata of original fullpath file
                    with open(url, 'rb') as src, open(fullpath, 'wb') as dst:
                        dst.write(src.read())
                    # Mark as cached since we now have the full content
                    self.mark_file_cached(path)
                    # change user.url xattr along fullpath
                    current_path = fullpath
                    while current_path != self.root:
                        current_path = os.path.dirname(current_path)
                        print(f'Setting xattr for {current_path} - before: {os.getxattr(current_path, "user.url").decode("utf-8")}')
                        try:
                            os.setxattr(current_path, 'user.url', current_path.encode('utf-8'))
                        except OSError as e:
                            print(f'Warning: Could not set xattr for {current_path}: {e}')
                        print(f'Setting xattr for {current_path} - after: {os.getxattr(current_path, "user.url").decode("utf-8")}')
                    return os.open(fullpath, flags)
            else:
                print(f'🟢 Opening local file {path} for reading')
                return os.open(url, flags)
        if self.is_file_cached(path):
            return os.open(fullpath, flags)
        
        print(f'🟠 cloud open file {path}, fullpath: {fullpath}, url: {url}')
        # Acquire the lock to download the file
        with self.locks[path]:
            # Double-check if the file was downloaded while waiting for the lock
            if self.is_file_cached(path):
                return os.open(fullpath, flags)
                
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
        print('👇 create', path)
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def write(self, path, buf, offset, fh):
        print('👇 writing file', path, offset)
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
        return self.flush(path, fh)


def check_upload_complete(local_dir, s3_url):
    # TODO: to be implemented
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
            if file == DIR_META_FILE:
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
def ffdeploy_path(local_dir:str):
    start_time = time.time()
    local_dir = os.path.expanduser(local_dir)
    local_dir = os.path.abspath(local_dir)
    print('local_dir', local_dir)
    if not os.path.isdir(local_dir) or not os.path.exists(local_dir):
        print(f'🔴 local directory {local_dir} is not a folder')
        return
    
    # Local helper function to upload metadata for one directory
    def upload_meta(root, dirs, files, idx, folder_count):
        children_stats = {}
        for file in files:
            if file == DIR_META_FILE:
                print(f'🔴 .ffbox_dir_meta.json is a reserved file name')
                continue
            child_path = os.path.join(root, file)
            stats = os.stat(child_path)
            rel_path = os.path.relpath(child_path, local_dir)
            children_stats[file] = {
                "size": stats.st_size,           # Size in bytes
                "mtime": stats.st_mtime,   # Last modified time
                "ctime": stats.st_atime,    # Creation time
                "url": child_path,
            }

        for d in dirs:
            if d == DIR_META_FILE:
                print(f'🔴 .ffbox_dir_meta.json is a reserved file name')
                continue
            child_path = os.path.join(root, d)
            rel_path = os.path.relpath(child_path, local_dir)
            children_stats[d] = {
                "url": child_path,
            }
        json_path = os.path.join(root, DIR_META_FILE)
        with open(json_path, 'w') as f:
            json.dump(children_stats, f)
        print(f'👇 saved {idx + 1}/{folder_count} {json_path}')

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

def ffpush(local_dir, s3_url):
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
            if file == DIR_META_FILE:
                print(f'🔴 .ffbox_dir_meta.json is a reserved file name')
                continue
            child_path = os.path.join(root, file)
            stats = os.stat(child_path)
            rel_path = os.path.relpath(child_path, local_dir)
            object_key = f'{s3_prefix}/{rel_path}'.strip('/')
            children_stats[file] = {
                "size": stats.st_size,           # Size in bytes
                "mtime": stats.st_mtime,   # Last modified time
                "ctime": stats.st_atime,    # Creation time
                "url": f's3://{s3_bucket_name}/{object_key}',
            }
            print(f'👇 uploading {idx + 1}/{folder_count} {child_path} to s3://{s3_bucket_name}')

            s3_client.upload_file(child_path, s3_bucket_name, object_key, Config=config)

        for d in dirs:
            if d == DIR_META_FILE:
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
        key = '/'.join([x for x in [s3_prefix, rel_path, DIR_META_FILE] if x != ''])
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
        
def check_is_ffbox_folder(url: str):
    url = url.strip(' ').rstrip('/')
    print('checking url is ffbox folder', url)
    if url.startswith('s3://'):
        s3_prefix = '/'.join(url.split('://')[1:])
        s3_bucket_name = s3_prefix.split('/')[0]
        s3_root = '/'.join(s3_prefix.split('/')[1:]).strip('/')
        print(f'👇 checking {s3_root}/{DIR_META_FILE} in bucket {s3_bucket_name}')
        try:
            s3_client.head_object(
                Bucket=s3_bucket_name, 
                Key=f'{s3_root}/{DIR_META_FILE}')
            return True
        except ClientError as e:
            # Not found
            if e.response['Error']['Code'] in ('404', 'NotFound'):
                return False
            else:
                raise e
    if url.startswith('/'):
        print('2222 metafile', os.path.join(url, DIR_META_FILE))
        return os.path.exists(os.path.join(url, DIR_META_FILE))
    return False
def ffmount(url:str, mountpoint, cache_dir=None, cache_repo=None, foreground=True, clean_cache=False):
    fake_path = os.path.abspath(mountpoint)
    mountpoint = os.path.expanduser('~')
    mountpoint = os.path.abspath(mountpoint)
    global nsclient
    if cache_dir is None:
        home_dir = os.path.expanduser("~")
        cache_dir = os.path.join(home_dir, '.cache', 'ffbox')
    if url.startswith('/'):
        print('is path source')
        nsclient = PathClient(url)
        if mountpoint.startswith('/'):
            mountpoint = mountpoint[1:]
        #TODO: need to rework on whats the default real_path in path mode
        real_path = os.path.join(cache_dir, mountpoint)
    elif url.startswith('s3://'):
        print('is s3 ')
        nsclient = S3Client(url)
        s3_bucket_name = '/'.join(url.split('://')[1:])
        print(f's3 bucket name: {s3_bucket_name}')
        real_path = os.path.join(cache_dir, s3_bucket_name)
    else:
        raise Exception('Network storage type not supported!')
    if cache_repo:
        cache_repo = os.path.expanduser(cache_repo)
        cache_repo = os.path.abspath(cache_repo)
        print(f'specific cache repo: {cache_repo}')
        real_path = cache_repo
    print(f"real storage path: {real_path}, fake storage path: {fake_path}")

    if os.path.exists(fake_path):
        print(f"Warning: {fake_path} already exists, do you want to override?")
        if input("y/n: ") != "y":
            print("Exiting")
            return
        else:
            shutil.rmtree(fake_path)
    os.makedirs(real_path, exist_ok=True)
    # check if the s3 folder is a ffbox folder
    is_ffbox_folder = check_is_ffbox_folder(url)
    print(f'🦄 is ffbox meta folder:', is_ffbox_folder)

    if clean_cache and os.path.exists(real_path):
        shutil.rmtree(real_path)
    os.makedirs(fake_path, exist_ok=True)
    os.makedirs(real_path, exist_ok=True)
    os.setxattr(real_path, 'user.url', url.strip().rstrip('/').encode('utf-8'))

    passthru = Passthrough(real_path, fake_path, url, is_ffbox_folder)
    # passthru.start_background_pulling()
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
    parser_mount.add_argument("--cache-repo", help="Override specific cache path to store for this repo only")

    # Push command
    parser_push = subparsers.add_parser("push", help="Push a local directory to an S3 bucket")
    parser_push.add_argument("local_dir", help="Local directory containing files to push")
    parser_push.add_argument("s3_url", help="S3 URL to push files to")
    
    # Deploy path command
    parser_deploy = subparsers.add_parser("deploy", help="Deploy a network directory")
    parser_deploy.add_argument("local_dir", help="Local directory containing files to push")

    args = parser.parse_args()

    if args.command == "mount":
        ffmount(args.s3_url, args.mountpoint, cache_dir=args.cache_dir, cache_repo=args.cache_repo, clean_cache=args.clean)
    elif args.command == "push":
        ffpush(args.local_dir, args.s3_url)
    elif args.command == "deploy":
        ffdeploy_path(args.local_dir)
    else:
        parser.print_help()
