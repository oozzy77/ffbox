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
import concurrent.futures

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

if aws_access_key and aws_secret_key:
    # If credentials are found, use them
    s3_client = boto3.client('s3')
else:
    # If no credentials, use unsigned configuration
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

META_DIR = '.ffbox_noot'

uid = os.getuid()
gid = os.getgid()

import os
import math
import mmap
import threading
import concurrent.futures
from botocore.exceptions import ClientError

class MmapChunkedReader:
    """
    Downloads an S3 object in parallel chunks and writes them directly 
    into a memory-mapped local file. Provides a `read(offset, length)` 
    method that blocks until the needed chunks are downloaded.
    """

    def __init__(self, s3_client, bucket, key, local_path, 
                 file_size, chunk_size=5*1024*1024, max_workers=10):
        """
        :param s3_client: Boto3 S3 client
        :param bucket: S3 bucket name
        :param key: Key (path) of the file in S3
        :param local_path: Full path to local file (already truncated to file_size)
        :param file_size: Size of the file in bytes
        :param chunk_size: Chunk size in bytes for each parallel range-get
        :param max_workers: Maximum number of download threads
        """
        self.s3_client = s3_client
        self.bucket = bucket
        self.key = key
        self.local_path = local_path
        self.file_size = file_size
        self.chunk_size = chunk_size
        self.max_workers = max_workers

        # Calculate how many chunks needed
        self.num_chunks = math.ceil(file_size / chunk_size) if file_size > 0 else 0

        # Track which chunks have been downloaded
        self.chunk_downloaded = [False] * self.num_chunks
        # For concurrency
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

        # Memory-map the local file in read/write mode
        self._file_obj = open(self.local_path, 'r+b')
        self._mmap = mmap.mmap(self._file_obj.fileno(), self.file_size, access=mmap.ACCESS_WRITE)

        # Flag to track if entire file is cached
        self.is_fully_cached = (self.num_chunks == 0)  # true if file_size==0

        # Start background download thread
        self._downloader_thread = threading.Thread(target=self._download_all_chunks, daemon=True)
        self._downloader_thread.start()

    def _download_all_chunks(self):
        """ Spin up a thread pool to fetch all chunks in parallel. """
        if self.num_chunks == 0:
            # Edge case: zero-length file
            with self._lock:
                self.is_fully_cached = True
                self._cond.notify_all()
            return

        def download_chunk(idx):
            start_offset = idx * self.chunk_size
            end_offset = min(start_offset + self.chunk_size - 1, self.file_size - 1)
            range_header = f'bytes={start_offset}-{end_offset}'

            # Basic retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    resp = self.s3_client.get_object(
                        Bucket=self.bucket,
                        Key=self.key,
                        Range=range_header
                    )
                    data = resp['Body'].read()
                    # Write data directly into the mapped file
                    self._mmap.seek(start_offset)
                    self._mmap.write(data)
                    break  # success
                except ClientError as e:
                    if attempt == max_retries - 1:
                        raise
                except Exception as e2:
                    if attempt == max_retries - 1:
                        raise
            
            with self._lock:
                self.chunk_downloaded[idx] = True
                self._cond.notify_all()

        # Download chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(download_chunk, i) for i in range(self.num_chunks)]
            for f in concurrent.futures.as_completed(futures):
                # Raise any exception
                _ = f.result()

        # Mark fully cached
        with self._lock:
            self.is_fully_cached = True
            self._cond.notify_all()

    def read(self, offset, length):
        """
        If the file is fully cached, read directly from the mmap.
        Otherwise, block until each needed chunk is downloaded.
        """
        if offset >= self.file_size:
            return b""

        if offset + length > self.file_size:
            length = self.file_size - offset

        # If fully cached, just read
        with self._lock:
            if self.is_fully_cached:
                self._mmap.seek(offset)
                return self._mmap.read(length)

        # Otherwise, figure out which chunks we need
        first_chunk = offset // self.chunk_size
        last_chunk  = (offset + length - 1) // self.chunk_size

        # Wait for chunks
        with self._lock:
            for cidx in range(first_chunk, last_chunk + 1):
                while not self.chunk_downloaded[cidx]:
                    self._cond.wait()
        
        # Now read from mmap
        self._mmap.seek(offset)
        return self._mmap.read(length)

    def close(self):
        """
        Clean up. Call this when you know you're done with the file, e.g. in FUSE release().
        """
        if not self._mmap.closed:
            self._mmap.close()
        if not self._file_obj.closed:
            self._file_obj.close()


class Passthrough(Operations):
    CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
    MAX_WORKERS = 10
    def __init__(self, root, mountpoint, s3_url = None):
        self.root = root
        self.mountpoint = mountpoint
        self.s3_url = s3_url
        parsed_url = urlparse(s3_url)
        self.bucket = parsed_url.netloc
        self.prefix = parsed_url.path.strip('/')  # Remove both leading and trailing slashes
        self.locks = defaultdict(threading.Lock)  # Automatically create a lock for each new file path
        self.cached_dir = set()
        self.chunk_readers = {}     # path -> MmapChunkedReader

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
    CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
    MAX_THREAD = 20

    def cloud_download(self, path, full_path):
        with self.locks[path]:
            if self.is_file_cached(path):
                return

            try:
                print(f'🟠 cloud open file {path}, parallel downloading to {full_path}')

                object_size = os.stat(full_path).st_size

                def download_chunk(start_offset, end_offset, attempt=0):
                    max_retries = 3
                    for attempt_i in range(max_retries):
                        try:
                            response = s3_client.get_object(
                                Bucket=self.bucket,
                                Key=self.cloud_object_key(path),
                                Range=f'bytes={start_offset}-{end_offset}'
                            )
                            chunk_data = response['Body'].read()
                            
                            # write chunk into file at the correct offset
                            with open(full_path, 'r+b') as f:
                                f.seek(start_offset)
                                f.write(chunk_data)
                            return

                        except ClientError as e2:
                            # If 404 or other errors
                            if e2.response['Error']['Code'] == '404':
                                print("🔴 open The object does not exist.")
                                raise FuseOSError(errno.ENOENT)

                            if attempt_i < max_retries - 1:
                                print(f'🔴Retrying chunk download (attempt {attempt_i + 2}/{max_retries}) for bytes {start_offset}-{end_offset}')
                            else:
                                raise FuseOSError(errno.EIO)
                        except Exception as e3:
                            print(f'🔴 error fetching file range {start_offset}-{end_offset} from s3 {path}: {e3}')
                            traceback.print_exc()
                            if attempt_i < max_retries - 1:
                                print(f'🔴Retrying chunk download (attempt {attempt_i + 2}/{max_retries}) for bytes {start_offset}-{end_offset}')
                            else:
                                raise FuseOSError(errno.EIO)

                chunk_size = self.CHUNK_SIZE
                ranges = []
                start = 0
                while start < object_size:
                    end = min(start + chunk_size - 1, object_size - 1)
                    ranges.append((start, end))
                    start += chunk_size

                with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_THREAD) as executor:
                    future_to_range = {
                        executor.submit(download_chunk, rng[0], rng[1]): rng
                        for rng in ranges
                    }
                    for future in concurrent.futures.as_completed(future_to_range):
                        future.result()

                print(f'🟢 Parallel download successful to {full_path}')
                # os.chmod(full_path, 0o755)

                self.mark_file_cached(path)

            except Exception as e:
                # TODO: add error handling on download fail (maybe reflect on task status to notify consumer)
                print(f'🔴 error downloading to {full_path}: {e}')
                traceback.print_exc()
                # Clean up any partial downloads
                if os.path.exists(full_path):
                    os.unlink(full_path)
                raise FuseOSError(errno.EIO)

    def open(self, path, flags):
        full_path = self._full_path(path)
        print(f"👀 open() called for {path}")

        # If file is fully cached, do a normal open
        if self.is_file_cached(path):
            return os.open(full_path, flags)
        else:
            # Otherwise, create or retrieve an MmapChunkedReader
            with self.locks[path]:
                if path not in self.chunk_readers:
                    # We know the local file is created/truncated to the correct size 
                    # (you do that in your "cloud_getattr" or "cloud_readdir" logic).
                    file_size = os.stat(full_path).st_size
                    reader = MmapChunkedReader(
                        s3_client=s3_client,
                        bucket=self.bucket,
                        key=self.cloud_object_key(path),
                        local_path=full_path,
                        file_size=file_size,
                        chunk_size=self.CHUNK_SIZE,
                        max_workers=self.MAX_WORKERS
                    )
                    self.chunk_readers[path] = reader
            return os.open(full_path, flags)

    def read(self, path, length, offset, fh):
        # If the file is fully cached, read from local disk. Otherwise, delegate to MmapChunkedReader.
        print(f"👀 read() called for {path} offset={offset}, length={length}")

        # Otherwise, we have an MmapChunkedReader
        reader = self.chunk_readers.get(path)
        if reader:
            data = reader.read(offset, length)
            # If the reader says it's fully cached, mark the file as cached
            if reader.is_fully_cached:
                self.mark_file_cached(path)
            return data
        # TODO: remove this check, since we already checked in open()
        if self.is_file_cached(path):
            print(f'🟠 file {path} is fully cached, reading from local disk')
            # Normal local file read
            with open(self._full_path(path), 'rb') as f:
                f.seek(offset)
                return f.read(length)
        else: # this should never happen, since we already checked in open()
            print(f'🔴 file {path} is not fully cached in disk, and not being downloading!!')
            raise FuseOSError(errno.ENOENT)

    # ...
    # Optionally, in your release() or close() logic, you might do:
    def release(self, path, fh):
        print(f'👀 release() called for {path}')
        # If you want to close the MmapChunkedReader after the last close
        # you'd need a reference count or similar. For simplicity:
        reader = self.chunk_readers.get(path)
        if reader and reader.is_fully_cached:
            reader.close()
            del self.chunk_readers[path]
        return os.close(fh)

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

    def fsync(self, path, fdatasync, fh):
        print('👇 fsyncing file')
        with self.locks[path]:
            return self.flush(path, fh)

def ffmount(s3_url, mountpoint, prefix=None, foreground=True, clean_cache=False):
    fake_path = os.path.abspath(mountpoint)
    if prefix is None:
        home_dir = os.path.expanduser("~")
        prefix = os.path.join(home_dir, '.cache', 'ffbox')
    if s3_url:
        s3_bucket_name = '/'.join(s3_url.split('://')[1:])
        print(f's3 bucket name: {s3_bucket_name}')
        real_path = os.path.join(prefix, s3_bucket_name)
    else:
        if mountpoint.startswith('/'):
            mountpoint = mountpoint[1:]
        real_path = os.path.join(prefix, mountpoint)
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

    args = parser.parse_args()

    if args.command == "mount":
        ffmount(args.s3_url, args.mountpoint, clean_cache=args.clean)
    else:
        parser.print_help()
