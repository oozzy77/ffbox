#!/usr/bin/env python


import os
import shutil
import subprocess

def export_portable_venv_sh(original_venv_path, dest_venv_path = None):
    original_venv_path = os.path.abspath(original_venv_path)
    if dest_venv_path is None:
        dest_venv_path = os.path.join(os.path.dirname(original_venv_path), "venv_portable")
    # directly call the shell script
    subprocess.run([os.path.join(os.path.dirname(__file__), "cp_venv_to_portable.sh"), original_venv_path, dest_venv_path])

def export_portable_venv(original_venv_path, dest_venv_path):
    if not os.path.exists(original_venv_path):
        print("Error: The specified virtual environment does not exist.")
        return

    print(f"Original VENV_PATH: {original_venv_path}")
    print(f"Creating a copy of the virtual environment at: {dest_venv_path}")

    # Step 1: Copy the entire virtual environment to a new directory
    shutil.copytree(original_venv_path, dest_venv_path)

    # Step 2: Replace symbolic links with actual binaries in bin/
    for py_bin in ["python", "python3", "python3.12"]:
        py_bin_path = os.path.join(dest_venv_path, "bin", py_bin)
        if os.path.islink(py_bin_path):
            real_bin = os.path.realpath(py_bin_path)
            os.remove(py_bin_path)
            shutil.copy(real_bin, py_bin_path)
            print(f"Replaced symbolic link {py_bin_path} with actual binary from {real_bin}")

    # Step 3: Find all remaining symbolic links and replace them with actual files
    for root, dirs, files in os.walk(dest_venv_path):
        for name in files:
            file_path = os.path.join(root, name)
            if os.path.islink(file_path):
                real_path = os.path.realpath(file_path)
                if not os.path.exists(real_path):
                    print(f"Warning: Target {real_path} does not exist for symlink {file_path}")
                    continue
                os.remove(file_path)
                shutil.copy(real_path, file_path)
                print(f"Replaced symbolic link {file_path} with actual file from {real_path}")

    # Step 4: Update paths in the activate script
    activate_script = os.path.join(dest_venv_path, "bin", "activate")
    with open(activate_script, 'r') as file:
        data = file.read()
    data = data.replace(original_venv_path, '$(dirname "$(dirname "$BASH_SOURCE")")')
    with open(activate_script, 'w') as file:
        file.write(data)

    # Step 5: Verify symbolic links have been replaced
    print(f"Checking for remaining symbolic links in {dest_venv_path}:")
    subprocess.run(["find", dest_venv_path, "-type", "l"])

    print(f"Portable virtual environment created at {dest_venv_path}")

    

def push_to_cloud(local_dir, bucket_url):
    # rclone sync image_gen_pyinstaller test-conda:ff-image-gen/image_gen_pyinstaller --create-empty-src-dirs --progress --copy-links --transfers=16 --checkers=16 --multi-thread-streams=4
    if not bucket_url.startswith("s3://"):
        bucket_url = bucket_url.replace("s3://", "")
        bucket_url = f's3:{bucket_url}'
    else:
        print(f"🔴only s3 bucket is supported, bucket url must start with s3://, got {bucket_url}")
        return
    subprocess.run([
        "rclone", "sync", local_dir, bucket_url,
        "--create-empty-src-dirs", "--progress", "--copy-links", "--transfers=16", "--checkers=16", "--multi-thread-streams=4",
        "--config", os.path.join(os.path.dirname(__file__), "rclone.conf")
    ])

def pull_from_cloud(bucket_url, mountpoint):
    # to readonly mount: rclone mount s3:some-bucket /local/project --read-only --vfs-cache-mode full
    # to 2-way sync write: rclone mount s3:some-bucket /local/project --vfs-cache-mode full
    # mkdir sdvenv_pulled && rclone mount test-conda:ff-image-gen/sdvenv sdvenv_pulled --vfs-cache-mode full --file-perms 0755 --cache-dir ~/rclone_cache --dir-cache-time 24h --vfs-cache-max-age 24h

    # cd sdvenv_pulled && chmod +x activate && ./activate
    subprocess.run(["rclone", "mount", "test-conda:ff-image-gen", "/sdvenv_pulled", "--read-only", "--vfs-cache-mode", "full", "--file-perms", "0755", "--cache-dir", "~/rclone_cache"])
    # subprocess.run(["cd", "image_gen", "&&", "conda", "activate", "./conda_env", "&&", "python", "main.py"])

def clear_all_ram_page_cache():
    # sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    subprocess.run(["sudo", "sync"])
    subprocess.run(["sudo", "echo", "3", ">", "/proc/sys/vm/drop_caches"])

def main():
    print("start syncing with image-gen")
    # rclone sync test-conda:test-conda /home/ec2-user/test-conda --create-empty-src-dirs
    subprocess.run(["rclone", "sync", "image-gen:image-gen", "/home/ec2-user/image-gen", "--create-empty-src-dirs"])


def main():
    import argparse
    parser = argparse.ArgumentParser(description="ffbox CLI tool for S3 operations.")
    subparsers = parser.add_subparsers(dest="command")

    # export venv command
    parser_export_venv = subparsers.add_parser("portvenv", help="Export a virtual environment to a portable directory")
    parser_export_venv.add_argument("original_venv_path", help="Path to the original virtual environment")
    parser_export_venv.add_argument("dest_venv_path", nargs='?', default=None, help="Path to the destination directory")

    # Push command
    parser_push = subparsers.add_parser("push", help="Push data to an S3 bucket")
    parser_push.add_argument("local_dir", help="Local directory to push")
    parser_push.add_argument("bucket_url", help="URL of the S3 bucket")

    # Pull command
    parser_pull = subparsers.add_parser("pull", help="Pull data from an S3 bucket")
    parser_pull.add_argument("bucket_url", help="URL of the S3 bucket")
    parser_pull.add_argument("mountpoint", help="Local directory to pull data into")

    args = parser.parse_args()

    if args.command == "push":
        push_to_cloud(args.bucket_url)
    elif args.command == "pull":
        pull_from_cloud(args.bucket_url, args.mountpoint)
    elif args.command == "portvenv":
        export_portable_venv_sh(args.original_venv_path, args.dest_venv_path)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()