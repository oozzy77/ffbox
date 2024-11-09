#!/usr/bin/env python


import subprocess

def push_to_cloud():
    # rclone sync image_gen_pyinstaller test-conda:ff-image-gen/image_gen_pyinstaller --create-empty-src-dirs --progress --copy-links --transfers=16 --checkers=16 --multi-thread-streams=4

    subprocess.run(["rclone", "sync", "image_gen", "test-conda:ff-image-gen", "--create-empty-src-dirs", "--progress", "--partial"])

def pull_from_cloud_and_run(s3_url):
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


if __name__ == "__main__":
    main()
