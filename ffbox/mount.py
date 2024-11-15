#!/usr/bin/env python

from __future__ import with_statement

import json
import os
import shutil
import subprocess
import sys
import errno
# import boto3
from fuse import FUSE, FuseOSError, Operations, fuse_get_context

from ffmount.fileops import META_DIR, get_getattr_dir_save_path, get_getattr_file_save_path, getattr_from_cloud, restore_file_attributes

class Passthrough(Operations):
    def __init__(self, root, s3_url):
        self.root = root
        self.s3_url = s3_url
        print(f'init s3_url: {s3_url}')
        print(f'init root: {root}')

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # Cloud s3 file operations
    # ==================

    def cloud_getattr(self, full_path):
        getattr_path = get_getattr_dir_save_path(full_path)
        file_attr_path = get_getattr_file_save_path(full_path)
        if not os.path.exists(getattr_path):
            relpath = os.path.relpath(full_path, self.root)
            cloud_url = f'{self.s3_url}/{relpath}/{META_DIR}/getattr.json'
            print(f'⏩downloading {cloud_url} to {full_path}')
            try:
                subprocess.run(['s5cmd', 'cp', cloud_url, getattr_path])
            except Exception as e:
                print(f'error downloading {cloud_url} to {full_path}: {e}')
        if not os.path.exists(file_attr_path):
            relpath = os.path.relpath(os.path.dirname(full_path), self.root)
            cloud_url = f'{self.s3_url}/{relpath}/{META_DIR}/{os.path.basename(full_path)}'
            print(f'⏩downloading {cloud_url} to {full_path}')
            try:
                subprocess.run(['s5cmd', 'cp', cloud_url, file_attr_path])
            except Exception as e:
                print(f'error downloading {cloud_url} to {full_path}: {e}')
        attr_data = {}
        if os.path.exists(getattr_path):
            with open(getattr_path, 'r') as getattr_file:
                attr_data = json.load(getattr_file)
        elif os.path.exists(file_attr_path):
            with open(file_attr_path, 'r') as file_attr_file:
                attr_data = json.load(file_attr_file)
        return attr_data

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
        if not os.path.exists(full_path):
            attr_data = self.cloud_getattr(full_path).get('attr')
        else:
            st = os.lstat(full_path)
            attr_data = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        return attr_data
    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

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


# def main(mountpoint, root):
#     FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

# if __name__ == '__main__':
#     main(sys.argv[2], sys.argv[1])

# mkdir -p /fake_path1 && mkdir -p /real_path1
# python ffmount/mount.py /real_path1 /fake_path1

def main(s3_url, mountpoint, prefix='/home/ec2-user/realer22', foreground=True):
    fake_path = os.path.abspath(mountpoint)
    s3_bucket_name = '/'.join(s3_url.split('://')[1:])
    print(f's3 bucket name: {s3_bucket_name}')
    real_storage_path = os.path.join(prefix, s3_bucket_name)
    if os.path.exists(fake_path):
        print(f"Warning: {fake_path} already exists, do you want to override?")
        if input("y/n: ") != "y":
            print("Exiting")
            return
        else:
            shutil.rmtree(fake_path)
    os.makedirs(fake_path, exist_ok=True)
    os.makedirs(real_storage_path, exist_ok=True)
    print(f"real storage path: {real_storage_path}, fake storage path: {fake_path}")
    FUSE(Passthrough(real_storage_path, s3_url), fake_path, nothreads=True, foreground=foreground)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])