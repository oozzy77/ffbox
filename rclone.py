#!/usr/bin/env python


import subprocess

def push_to_cloud():
    # rclone sync image_gen test-conda:ff-image-gen --create-empty-src-dirs --progress
    subprocess.run(["rclone", "sync", "image_gen", "test-conda:ff-image-gen", "--create-empty-src-dirs", "--progress", "--partial"])

def pull_from_cloud_and_run():
    # mkdir my_image_gen_clone1 && rclone mount test-conda:ff-image-gen my_image_gen_clone1 --vfs-cache-mode full --progress
    # cd my_image_gen_clone1 && conda activate ./conda_env && python main.py
    subprocess.run(["rclone", "mount", "test-conda:ff-image-gen", "/my_image_gen1", "--vfs-cache-mode", "full", "--progress"])
    subprocess.run(["cd", "image_gen", "&&", "conda", "activate", "./conda_env", "&&", "python", "main.py"])

def main():
    print("start syncing with image-gen")
    # rclone sync test-conda:test-conda /home/ec2-user/test-conda --create-empty-src-dirs
    subprocess.run(["rclone", "sync", "image-gen:image-gen", "/home/ec2-user/image-gen", "--create-empty-src-dirs"])


if __name__ == "__main__":
    main()
