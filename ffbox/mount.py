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

class Passthrough(Operations):
    def __init__(self, root, s3_url = None):
        self.root = root
        self.s3_url = s3_url
        parsed_url = urlparse(s3_url)
        self.bucket = parsed_url.netloc
        self.prefix = parsed_url.path.strip('/')  # Remove both leading and trailing slashes
        self.locks = defaultdict(threading.Lock)  # Automatically create a lock for each new file path
        print(f'init bucket: {self.bucket}')
        print(f'init prefix: {self.prefix}')

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
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
                os.chmod(file_path, 0o100755)  # Set file mode to executable
                os.chown(file_path, uid, gid)  # Set ownership to current user
        # mark this path as completed cached
        os.makedirs(os.path.join(self.root, META_DIR, parent_path), exist_ok=True)

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
                    os.chmod(file_path, 0o100755)  # Set file mode to executable
                    os.chown(file_path, uid, gid)  # Set ownership to current user

        # mark this path as completed cached
        os.makedirs(os.path.join(self.root, META_DIR, path.strip('/')), exist_ok=True)
    
    def download_file(self, cloud_url, full_path):
        print(f"Running command: s5cmd cp {cloud_url} {full_path}")
        try:
            result = subprocess.run(['s5cmd', 'cp', cloud_url, full_path], capture_output=True, text=True, check=True)
            if result.stdout:
                print(f"Command output: {result.stdout}")
            if result.stderr:
                print(f"Command error: {result.stderr}")
        except subprocess.CalledProcessError as e:
            print(f"Command failed with error: {e.stderr}")
            raise FuseOSError(errno.EIO)

    def is_folder_cached(self, path):
        cache_path = os.path.join(self.root, META_DIR, path.lstrip('/'))
        if os.path.exists(cache_path):
            return True
        else:
            print(f'🟠 parent folder {path} is NOT cached', cache_path)
            return False
    
    # Filesystem methods
    # ==================

    def access(self, path, mode):
        print('👇 getting access to path', path, mode)
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
        if path.startswith(f'/{META_DIR}'):
            raise FuseOSError(errno.ENOENT)
        print(f'👇getting attribute of {path}')
        full_path = self._full_path(path)
        if not os.path.exists(full_path):
            self.cloud_getattr(path)

        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                    'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        if path.startswith(f'/{META_DIR}'):
            raise FuseOSError(errno.EIO)
        print(f'👇reading directory {path}')
        
        cache_path = os.path.join(self.root, META_DIR, path.strip('/'))
        if os.path.exists(cache_path):
            yield '.'
            yield '..'
            # Add more entries as needed
            for entry in os.listdir(self._full_path(path)):
                yield entry
        else:
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
        cache_path = os.path.join(self.root, META_DIR, path.strip('/'))
        print(f'👇opening file {path}')
        
        # Check if the file is cached without holding the lock
        if os.path.exists(cache_path):
            print(f'🟢 open file cache exists for {path}')
            return os.open(self._full_path(path), flags)
        
        # Acquire the lock to download the file
        with self.locks[path]:
            # Double-check if the file was downloaded while waiting for the lock
            if os.path.exists(cache_path):
                print(f'🟢 open file cache exists for {path} after waiting for lock')
                return os.open(self._full_path(path), flags)
            
            full_path = self._full_path(path)
            try:
                cloud_url = f's3://{self.bucket}/{self.cloud_object_key(path)}'
                print(f'🟠 cloud open file {path}, downloading {cloud_url} to {full_path}')
                
                # Create parent directories if they don't exist
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                
                # Download file with s5cmd
                result = subprocess.run(
                    ['s5cmd', 'cp', '--concurrency', '8', '-sp', cloud_url, full_path], 
                    capture_output=True, 
                    text=True, 
                    check=True
                )
                
                # Verify file was downloaded completely
                if not os.path.exists(full_path):
                    raise Exception("File download failed - file does not exist")
                    
                # Set proper permissions
                os.chmod(full_path, 0o755)  # Make the file executable
                
                if result.stdout:
                    print(f"Command output: {result.stdout}")
                if result.stderr:
                    print(f"Command error: {result.stderr}")
                    
                print(f'🔵 downloaded {cloud_url} to {full_path}')
                
                # Mark as cached
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                with open(cache_path, 'wb') as f:
                    pass  # Create marker file
                    
                return os.open(full_path, flags)
                
            except Exception as e:
                print(f'🔴 error downloading {cloud_url} to {full_path}: {e}')
                traceback.print_exc()
                # Clean up any partial downloads
                if os.path.exists(full_path):
                    os.unlink(full_path)
                raise FuseOSError(errno.EIO)
                
    def read(self, path, length, offset, fh):
        print('👇reading file', path)
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def create(self, path, mode, fi=None):
        uid, gid, pid = fuse_get_context()
        full_path = self._full_path(path)
        fd = os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
        os.chown(full_path,uid,gid) #chown to context uid & gid
        return fd

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

def ffmount(s3_url, mountpoint, prefix='/home/ec2-user/.cache/ffbox', foreground=True, clean_cache=False):
    fake_path = os.path.abspath(mountpoint)
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
    if clean_cache:
        shutil.rmtree(real_path)
    os.makedirs(fake_path, exist_ok=True)
    os.makedirs(real_path, exist_ok=True)

    print(f"real storage path: {real_path}, fake storage path: {fake_path}")
    FUSE(Passthrough(real_path, s3_url), fake_path, foreground=foreground)

def local_mount(mountpoint,  foreground=True):
    mountpoint = os.path.abspath(mountpoint)
    os.makedirs(mountpoint, exist_ok=True)
    fake_path = mountpoint + '_realstore'
    os.makedirs(fake_path, exist_ok=True)
    print(f"real storage path: {fake_path}, fake storage path: {mountpoint}")
    FUSE(Passthrough(mountpoint, ''), fake_path, foreground=foreground)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        local_mount(sys.argv[1])
    elif len(sys.argv) == 3:
        ffmount(sys.argv[1], sys.argv[2])
    else:
        print('usage: python ffbox/ffbox/mount.py [s3_url]  <mountpoint>')
