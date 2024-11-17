#!/usr/bin/env python

from __future__ import with_statement

import json
import os
import shutil
import subprocess
import sys
import errno
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from urllib.parse import urlparse
from fuse import FUSE, FuseOSError, Operations, fuse_get_context
import botocore

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

if aws_access_key and aws_secret_key:
    # If credentials are found, use them
    s3_client = boto3.client('s3')
else:
    # If no credentials, use unsigned configuration
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

META_DIR = '.ffbox/tree'

class Passthrough(Operations):
    def __init__(self, root, s3_url = None):
        self.root = root
        self.s3_url = s3_url
        parsed_url = urlparse(s3_url)
        self.bucket = parsed_url.netloc
        self.prefix = parsed_url.path.strip('/')  # Remove both leading and trailing slashes
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

    def cloud_getattr(self, partial):
        print(f'🟠 cloud getting attributes of {partial} ,', f'bucket: {self.bucket}, key: {self.cloud_object_key(partial)}')
        key = self.cloud_object_key(partial)
        
        try:
            # First, try to get the object metadata
            response = s3_client.head_object(Bucket=self.bucket, Key=key)
            
            # Map S3 metadata to getattr structure for a file
            attr_data = {
                'st_atime': response['LastModified'].timestamp(),  # Access time
                'st_ctime': response['LastModified'].timestamp(),  # Creation time
                'st_mtime': response['LastModified'].timestamp(),  # Modification time
                'st_size': response['ContentLength'],              # Size of the object
                # 'st_mode': 0o100644,                               # File mode (non-executable)
                'st_mode': 0o100755,                               # File mode (executable)
                'st_nlink': 1,                                     # Number of hard links
                'st_uid': os.getuid(),                             # User ID of owner
                'st_gid': os.getgid(),                             # Group ID of owner
            }
            return attr_data
        except Exception as e:
            # If the object is not found, check if it's a directory
            print(f'🟠object not found, checking if directory {key}')
            response = s3_client.list_objects_v2(Bucket=self.bucket, Prefix=key)
            if 'Contents' in response or 'CommonPrefixes' in response:
                # Return default attributes for a directory
                return {
                    'st_atime': 0,
                    'st_ctime': 0,
                    'st_mtime': 0,
                    'st_size': 0,
                    'st_mode': 0o040755,  # Directory mode
                    'st_nlink': 2,
                    'st_uid': os.getuid(),
                    'st_gid': os.getgid(),
                }
            else:
                raise FuseOSError(errno.ENOENT)
    
    def cloud_readdir(self, path):
        prefix = self.cloud_object_key(path)
        if os.path.exists(os.path.join(self.root, META_DIR, path, 'dirents.json')):
            print(f'🔵path cache exists for {path}')
            with open(os.path.join(self.root, META_DIR, path, 'dirents.json'), 'r') as f:
                return json.load(f)
        print(f'🟠reading cloud directory {prefix}')
        # List objects in the S3 bucket with the specified prefix
        response = s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        print(f'🟠response: {response}')
        
        dirents = ['.', '..']
        
        # Add directories (common prefixes) to dirents
        if 'CommonPrefixes' in response:
            for common_prefix in response['CommonPrefixes']:
                dir_name = common_prefix['Prefix'].rstrip('/').split('/')[-1]
                dirents.append(dir_name)
        
        # Add files to dirents
        if 'Contents' in response:
            for obj in response['Contents']:
                file_name = obj['Key'].split('/')[-1]
                dirents.append(file_name)
        print(f'🟠dirents: {dirents}')
        # with open(os.path.join(self.root, META_DIR, path, 'dirents.json'), 'w') as f:
        #     json.dump(dirents, f)
        
        for r in dirents:
            yield r

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
        print(f'👇getting attributes of {path}')
        full_path = self._full_path(path)
        if os.path.exists(full_path):
            st = os.lstat(full_path)
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        else:
            return self.cloud_getattr(path)

    def readdir(self, path, fh):
        print(f'👇reading directory {path}')
        # full_path = self._full_path(path)

        # dirents = ['.', '..']
        # if os.path.isdir(full_path):
        #     dirents.extend(os.listdir(full_path))
        # for r in dirents:
        #     yield r
        yield from self.cloud_readdir(path)

    def readlink(self, path):
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
        full_path = self._full_path(path)
        print(f'👇opening file {full_path}')
        if not os.path.exists(full_path):
            # download from s3
            rel_path = os.path.relpath(full_path, self.root)
            cloud_url = f'{self.s3_url}/{rel_path}'
            print(f'downloading {cloud_url} to {full_path}')
            # s5cmd cp s3://bucket/object.gz .
            subprocess.run(['s5cmd', 'cp', cloud_url, full_path])
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        uid, gid, pid = fuse_get_context()
        full_path = self._full_path(path)
        fd = os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
        os.chown(full_path,uid,gid) #chown to context uid & gid
        return fd

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

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

def ffmount(s3_url, mountpoint, prefix='/home/ec2-user/.cache/ffbox', foreground=True):
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
