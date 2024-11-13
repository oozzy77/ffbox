#!/usr/bin/env python

import os
import shlex
import shutil
import subprocess
import json
import time
import shlex
import traceback
import threading

CACHE_DIR = os.environ.get("FFBOX_CACHE_DIR", os.path.expanduser("~/ffbox_cache"))
MOUNT_DIR = os.environ.get("FFBOX_MOUNT_DIR", os.path.expanduser("~/ffbox_mount"))
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(MOUNT_DIR, exist_ok=True)

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
    else:
        local_dir = os.path.abspath(local_dir)
    
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
    ffbox_config_path = os.path.join(local_dir, ".ffbox/config.json")
    if not os.path.exists(ffbox_config_path):
        ffbox_config = {}
    else:
        ffbox_config = json.load(open(ffbox_config_path))

    run_cmd = ffbox_config.get("scripts", {}).get("example_run") or ffbox_config.get("scripts", {}).get("run")
    if run_cmd is not None:
        try:
            log_file_read_order(run_cmd, local_dir)
        except Exception as e:
            print(f"🟠failed to log file read order: {e}")
            traceback.print_exc()
    
    rclone_cmd = [
        "rclone", "sync", local_dir, bucket_url,
        "--create-empty-src-dirs", "--progress", "--copy-links", "--transfers=8", "--checkers=8", "--multi-thread-streams=4",
        "--config", os.path.join(os.path.dirname(__file__), "rclone.conf"),
        "--cache-dir", CACHE_DIR,
    ]
    print(f"🔵excluding {ffbox_config.get('exclude', [])}")
    for pattern in ffbox_config.get("exclude", []):
        rclone_cmd.extend(["--exclude", pattern])

    subprocess.run(rclone_cmd)


def mount_from_cloud(bucket_url, mountpoint = None):
    if bucket_url.startswith("s3://"):
        # rclone anonymous access to public buckets
        bucket_url = bucket_url.replace("s3://", "s3-public:")
    else:
        print(f"🔴only s3 bucket is supported, bucket url must start with s3://, got {bucket_url}")
        return
    if mountpoint is None:
        bucket_name = bucket_url.split(":")[1]
        if bucket_name.endswith("/"):
            bucket_name = bucket_name[:-1]
        mountpoint = os.path.join(MOUNT_DIR, bucket_name.replace("/", "-"))
    os.makedirs(mountpoint, exist_ok=True)
    print(f"🔵mounting {bucket_url} to {mountpoint}")
    # buffer size info: https://forum.rclone.org/t/whats-the-suitable-value-to-set-for-buffer-size-with-vfs-read-ahead/39971/4
    process = subprocess.Popen([
        "rclone", "mount", bucket_url, mountpoint,
        "--vfs-cache-mode", "full",
        "--vfs-write-back", "9999h", 
        "--file-perms", "0755",
        "--cache-dir", CACHE_DIR,
        "--dir-cache-time", "24h",
        "--vfs-cache-max-age", "24h",
        "--config", os.path.join(os.path.dirname(__file__), "rclone.conf"),
        # OPTIONAL - TESTING FOR PERFORMANCE OPTIMIZATION
        "--vfs-cache-max-size", "100G",  # max size for cache
        "--buffer-size", "0", 
        "--vfs-read-ahead", "1024M",
        "--low-level-retries", "1",  # reduce retries
        "--retries", "1",  # lower retry count to avoid delay on failing connections
        "--bwlimit", "10M",  # optional: limit bandwidth to avoid saturation
        # "--log-level", "DEBUG", 
        # "--log-file", os.path.join(os.path.dirname(__file__), "rclone_debug.log"),
    ])
    # Wait for the mount to be ready
    timeout = 10  # seconds
    start_time = time.time()

    while True:
        if os.path.ismount(mountpoint):
            print(f"🔵 Mount is ready at {mountpoint}")
            break
        elif time.time() - start_time > timeout:
            print("🔴 Timeout waiting for mount to be ready")
            process.terminate()
            return
        else:
            time.sleep(0.01) 
    return mountpoint   

def run_python_project(bucket_url, extra_args):
    mountpoint = mount_from_cloud(bucket_url)
    ffbox_config_path = os.path.join(mountpoint, ".ffbox/config.json")
    if not os.path.exists(ffbox_config_path):
        print(f"🔴no ffbox config file found in {os.getcwd()}, please add ffbox/config.json first")
        return
    ffbox_config = json.load(open(ffbox_config_path))
    run_cmd = ffbox_config.get("scripts", {}).get("run")
    if not run_cmd:
        print(f"🔴no run command found in {ffbox_config_path}, please add a run command in the config file")
        return
    # Append extra arguments directly to the run command string
    run_cmd += ' ' + ' '.join(extra_args)
    
    # Start background thread pool to pull files in a non-blocking way
    threading.Thread(target=background_pulling_read_order, args=(mountpoint,), daemon=True).start()
    
    print(f"🔵running {run_cmd} in {mountpoint}")
    subprocess.run(run_cmd, shell=True, cwd=mountpoint)

def background_pulling_read_order(mountpoint, num_threads=8):
    read_order_log_path = os.path.join(mountpoint, ".ffbox/read_order.log")
    
    if not os.path.exists(read_order_log_path):
        print(f"🟠 No read order log found at {read_order_log_path}")
        return

    with open(read_order_log_path, 'r') as log_file:
        file_paths = [line.strip() for line in log_file.readlines()]

    def cache_file(file_path):
        try:
            with open(file_path, 'rb') as f:
                f.read()  # Read the file to cache it
            print(f"🔵 Cached {file_path}")
        except Exception as e:
            print(f"🟠 Failed to cache {file_path}: {e}")
            pass

    def worker():
        while file_paths:
            file_path = file_paths.pop(0)
            cache_file(os.path.join(mountpoint, file_path))

    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=worker)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

def clear_all_ram_page_cache():
    # sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    subprocess.run(["sudo", "sync"])
    subprocess.run(["sudo", "echo", "3", ">", "/proc/sys/vm/drop_caches"])


def log_file_read_order(run_cmd, push_dir):
    log_file_path = os.path.join(push_dir, ".ffbox/read_order.log")
    # Quote the run_cmd to handle any special characters, including single quotes
    quoted_run_cmd = shlex.quote(run_cmd)
    
    # Define the full strace command
    full_cmd = f"strace -e trace=open,openat -f bash -c {quoted_run_cmd} 2>&1 | awk -F '\"' '/openat/ {{print $2}}' > {log_file_path}"
    print(f"🔵logging file read order to {log_file_path}")
    # Run the command using shell=True to process the entire string as a single shell command
    subprocess.run(full_cmd, shell=True, executable="/bin/bash")
    
    # Set to track unique paths
    paths_set = set()
    filtered_lines = []

    with open(log_file_path, 'r') as log_file:
        lines = log_file.readlines()
    
    for line in lines:
        if line.startswith(push_dir):
            rel_path = os.path.relpath(line.strip(), push_dir)
            if rel_path not in paths_set:
                paths_set.add(rel_path)
                filtered_lines.append(rel_path)

    # Overwrite the original log file with the filtered and unique relative paths
    with open(log_file_path, 'w') as log_file:
        log_file.writelines(f"{line}\n" for line in filtered_lines)
    
    print(f"🔵filtered file read order logged to {log_file_path}")

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
    parser_pull.add_argument("mountpoint", nargs='?', default=None, help="Local directory to pull data into")

    # Run command
    parser_run = subparsers.add_parser("run", help="Run a python inference project")
    parser_run.add_argument("bucket_url", help="URL of the S3 bucket of the python project")
    parser_run.add_argument('extra_args', nargs=argparse.REMAINDER, help="Additional arguments for the project")

    args = parser.parse_args()

    if args.command == "push":
        push_to_cloud(args.local_dir, args.bucket_url)
    elif args.command == "pull":
        mount_from_cloud(args.bucket_url, args.mountpoint)
    elif args.command == "portvenv":
        export_portable_venv_sh(args.original_venv_path, args.dest_venv_path)
    elif args.command == "run":
        run_python_project(args.bucket_url, args.extra_args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()