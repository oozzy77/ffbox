import os
import json
import boto3

META_DIR = '.ffmount_meta'
# s3 = boto3.client('s3')

def get_getattr_dir_save_path(dir_path):
    return os.path.join(dir_path, META_DIR, 'getattr.json')

def get_getattr_file_save_path(file_path):
    parent_dir = os.path.dirname(file_path)
    return os.path.join(parent_dir, META_DIR, os.path.basename(file_path))

def get_readdir_save_path(dir_path):
    return os.path.join(dir_path, META_DIR, 'readdir.json')

# Returns attr_data
def restore_file_attributes(file_path, getattr_path) -> dict:
    with open(getattr_path, 'r') as getattr_file:
        attr_data = json.load(getattr_file)

    # Restore attributes using the correct order
    os.chmod(file_path, attr_data['attr'][0])  # st_mode
    os.chown(file_path, attr_data['attr'][4], attr_data['attr'][5])  # st_uid, st_gid
    os.utime(file_path, (attr_data['attr'][7], attr_data['attr'][8]))  # st_atime, st_mtime
    return attr_data

def save_attributes(file_path):
    stat_info = os.lstat(file_path)
    attr_data = {
        'attr': [
            stat_info.st_mode,   # 0
            stat_info.st_ino,    # 1
            stat_info.st_dev,    # 2
            stat_info.st_nlink,  # 3
            stat_info.st_uid,     # 4
            stat_info.st_gid,     # 5
            stat_info.st_size,    # 6
            stat_info.st_atime,   # 7
            stat_info.st_mtime,   # 8
            stat_info.st_ctime,   # 9
        ]
    }
    getattr_path = get_getattr_dir_save_path(file_path)
    with open(getattr_path, 'w') as getattr_file:
        json.dump(attr_data, getattr_file)