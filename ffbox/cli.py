#!/usr/bin/env python


import os
import shutil
import subprocess
import json

def export_portable_venv_sh(original_venv_path, dest_venv_path = None):
    original_venv_path = os.path.abspath(original_venv_path)
    if dest_venv_path is None:
        dest_venv_path = os.path.join(os.path.dirname(original_venv_path), "venv_portable")
    # directly call the shell script
    subprocess.run([os.path.join(os.path.dirname(__file__), "cp_venv_to_portable.sh"), original_venv_path, dest_venv_path])
    

def push_to_cloud(local_dir, bucket_url):
    # rclone sync image_gen_pyinstaller test-conda:ff-image-gen/image_gen_pyinstaller --create-empty-src-dirs --progress --copy-links --transfers=16 --checkers=16 --multi-thread-streams=4
    if local_dir is None:
        local_dir = os.getcwd()
    
    if bucket_url.startswith("s3://"):
        bucket_url = bucket_url.replace("s3://", "s3:")
    else:
        print(f"🔴only s3 bucket is supported, bucket url must start with s3://, got {bucket_url}")
        return

    env = os.environ.copy()

    # check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    if 'AWS_ACCESS_KEY_ID' not in env or 'AWS_SECRET_ACCESS_KEY' not in env:
        print("🔴AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY is not set, please set them")
        return

    print(f"🔵pushing {local_dir} to {bucket_url}")
    ffbox_config = json.load(open(os.path.join(local_dir, ".ffbox/config.json")))
    rclone_cmd = [
        "rclone", "sync", local_dir, bucket_url,
        "--create-empty-src-dirs", "--progress", "--copy-links", "--transfers=8", "--checkers=8", "--multi-thread-streams=4",
        "--config", os.path.join(os.path.dirname(__file__), "rclone.conf")
    ]
    print(f"🔵excluding {ffbox_config.get('exclude', [])}")
    for pattern in ffbox_config.get("exclude", []):
        rclone_cmd.extend(["--exclude", pattern])

    subprocess.run(rclone_cmd)

CACHE_DIR = os.environ.get("FFBOX_CACHE_DIR", "~/ffbox_cache")
def pull_from_cloud(bucket_url, mountpoint = None):
    # to readonly mount: rclone mount s3:some-bucket /local/project --read-only --vfs-cache-mode full
    # to 2-way sync write: rclone mount s3:some-bucket /local/project --vfs-cache-mode full
    # mkdir sdvenv_pulled && rclone mount test-conda:ff-image-gen/sdvenv sdvenv_pulled --vfs-cache-mode full --file-perms 0755 --cache-dir ~/rclone_cache --dir-cache-time 24h --vfs-cache-max-age 24h
    # clear AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    if bucket_url.startswith("s3://"):
        bucket_url = bucket_url.replace("s3://", "s3:")
    else:
        print(f"🔴only s3 bucket is supported, bucket url must start with s3://, got {bucket_url}")
        return
    if mountpoint is None:
        bucket_name = bucket_url.split(":")[1]
        mountpoint = os.path.join(os.getcwd(), bucket_name.replace("/", "-"))
    env = os.environ.copy()
    env.pop('AWS_ACCESS_KEY_ID', None)
    env.pop('AWS_SECRET_ACCESS_KEY', None)
    print(f"🔵mounting {bucket_url} to {mountpoint}")
    subprocess.run([
        "rclone", "mount", bucket_url, mountpoint,
        "--vfs-cache-mode", "full",
        "--file-perms", "0755",
        "--cache-dir", CACHE_DIR,
        "--dir-cache-time", "24h",
        "--vfs-cache-max-age", "24h",
        "--config", os.path.join(os.path.dirname(__file__), "rclone.conf")
    ], env=env)

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
    parser_push.add_argument("local_dir", nargs='?', default=None, help="Local directory to push")
    parser_push.add_argument("bucket_url", help="URL of the S3 bucket")

    # Pull command
    parser_pull = subparsers.add_parser("pull", help="Pull data from an S3 bucket")
    parser_pull.add_argument("bucket_url", help="URL of the S3 bucket")
    parser_pull.add_argument("mountpoint", help="Local directory to pull data into")

    args = parser.parse_args()

    if args.command == "push":
        push_to_cloud(args.local_dir, args.bucket_url)
    elif args.command == "pull":
        pull_from_cloud(args.bucket_url, args.mountpoint)
    elif args.command == "portvenv":
        export_portable_venv_sh(args.original_venv_path, args.dest_venv_path)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()