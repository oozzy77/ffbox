MOUNT_POINT=~/rclone_bedrock
fusermount -uz $MOUNT_POINT
rm -rf "$HOME/.cache/rclone"
mkdir -p "$HOME/.cache/rclone"
mkdir -p $MOUNT_POINT

yes |  rclone mount  "s3://ffbox-ea1/pyinstall_sdxl_gen/" $MOUNT_POINT \
        --vfs-cache-mode "full" \
        --vfs-write-back "9999h"  \
        --file-perms "0755" \
        --cache-dir "$HOME/.cache/rclone" \
        --dir-cache-time "24h" \
        --vfs-cache-max-age "24h" \
        --config ~/ffbox/ffbox/rclone.conf \
        --vfs-cache-max-size "100G" \
        --vfs-read-ahead "10G"  \
        > "$HOME/rclone_mount.log" 2>&1 &
# --buffer-size "0" 
sleep 3  # Wait for 3 seconds to ensure the mount is available

start_time=$(date +%s)
echo "Start time: $(date)"

cd $MOUNT_POINT
# ./main/main
strace -tt -T -e trace=file -o "$HOME/pyinstall_rclone_bench11.log" main/main
# strace -tt -T -f -e trace=all -e signal=all -o "$HOME/pyinstall_rclone_bench11.log" main/main

end_time=$(date +%s)
echo "End time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Execution time: $minutes min $seconds seconds"

# Unmount the bedrock directory
fusermount -uz $MOUNT_POINT
