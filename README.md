## ffmount
Work in progress.

LLM container fast mount and streaming run using rclone and conda..

### rclone

- to readonly mount: rclone mount s3:some-bucket /local/project --read-only --vfs-cache-mode full
- to 2-way sync write: rclone mount s3:some-bucket /local/project --vfs-cache-mode full
