#!/usr/bin/env python


import subprocess

def push_to_cloud():
    # rclone sync image_gen test-conda:ff-image-gen --create-empty-src-dirs --progress --copy-links
    subprocess.run(["rclone", "sync", "image_gen", "test-conda:ff-image-gen", "--create-empty-src-dirs", "--progress", "--partial"])

def pull_from_cloud_and_run():
    # to readonly mount: rclone mount s3:some-bucket /local/project --read-only --vfs-cache-mode full
    # to 2-way sync write: rclone mount s3:some-bucket /local/project --vfs-cache-mode full
    # mkdir my_image_gen_clone3 && rclone mount test-conda:ff-image-gen my_image_gen_clone3 --vfs-cache-mode full
    # cd my_image_gen_clone3 && conda activate ./conda_env && python main.py
    subprocess.run(["rclone", "mount", "test-conda:ff-image-gen", "/my_image_gen2", "--read-only", "--vfs-cache-mode", "full"])
    subprocess.run(["cd", "image_gen", "&&", "conda", "activate", "./conda_env", "&&", "python", "main.py"])

def main():
    print("start syncing with image-gen")
    # rclone sync test-conda:test-conda /home/ec2-user/test-conda --create-empty-src-dirs
    subprocess.run(["rclone", "sync", "image-gen:image-gen", "/home/ec2-user/image-gen", "--create-empty-src-dirs"])


if __name__ == "__main__":
    main()
