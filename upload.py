import os
import json
import subprocess
from fileops import META_DIR, get_readdir_save_path, get_getattr_dir_save_path, get_getattr_file_save_path

def build_vfs(real_storage_path):
    def traverse_directory(dir_path):
        if os.path.basename(dir_path) == 'venv':
            return
        if META_DIR in dir_path:
            # otherwise we'll get infinite loop
            return
        # Get directory attributes
        dir_attr = {
            'path': dir_path,
            'attr': os.stat(dir_path)  # Get attributes using os.stat
        }
        print('dir_path', dir_path)

        # List children
        try:
            children = os.listdir(dir_path)  # List children in the directory
            
            # Create JSON file for directory children
            readdir_meta_path = get_readdir_save_path(dir_path)
            os.makedirs(os.path.dirname(readdir_meta_path), exist_ok=True)
            dir_attr['children'] = children
            # # Save children (also included in attr['children'])
            # with open(readdir_meta_path, 'w') as readdir_meta_file:
            #     json.dump(children, readdir_meta_file)

            # # Save directory attributes, children
            getattr_path = get_getattr_dir_save_path(dir_path)
            os.makedirs(os.path.dirname(getattr_path), exist_ok=True)
            
            with open(getattr_path, 'w') as getattr_file:
                json.dump(dir_attr, getattr_file, default=str)  # Convert non-serializable types to string
            # Traverse children directories
            for child in children:
                child_path = os.path.join(dir_path, child)
                if os.path.isdir(child_path):
                    traverse_directory(child_path)
                else:
                    print(f'👉file: {child_path}')
                    # save file attr
                    file_attr = os.stat(child_path)
                    file_meta =  {
                        'attr': file_attr,
                        'path': child_path
                    }
                    file_attr_path = get_getattr_file_save_path(child_path)
                    with open(file_attr_path, 'w') as file_attr_file:
                        json.dump(file_meta, file_attr_file, default=str)
        except PermissionError:
            print(f"Permission denied: {dir_path}")

    # Start traversing from the root directory
    traverse_directory(real_storage_path)

def push_to_s3(real_path, s3_url):
    subprocess.run(['s5cmd', 'sync', real_path, s3_url])

# Example usage
if __name__ == '__main__':
    real_storage_path = '/home/ec2-user/image_gen'
    build_vfs(real_storage_path)
    # subprocess.run(['s5cmd', 'sync', real_storage_path, 's3://ff-image-gen'])
