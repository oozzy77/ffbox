# Define a variable for the bedrock directory
MOUNT_POINT="$HOME/bedrock"
S3_URL="s3://ffbox-ea1/pyinstall_sdxl_with_meta2/"
LOG_FILE="$HOME/ffbox_mount.log"

fusermount -uz $MOUNT_POINT
rm -rf "$HOME/.cache/ffbox"
mkdir -p "$HOME/.cache/ffbox"
mkdir -p $MOUNT_POINT


yes | ffbox mount $S3_URL $MOUNT_POINT --clean --cache-dir /data/ffbox > $LOG_FILE 2>&1 &
sleep 3  # Wait for 3 seconds to ensure the mount is available

start_time=$(date +%s)
echo "Start time: $(date)"

cd $MOUNT_POINT
chmod +x ./main/main

# ./main/main
strace -tt -T -e trace=file -o "$HOME/pyinstall_ffbox_bench11.log" main/main
# strace -tt -T -f -e trace=all -e signal=all -o "$HOME/pyinstall_ffbox_bench11.log" main/main

end_time=$(date +%s)
echo "End time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Execution time: $minutes min $seconds seconds"

# Unmount the bedrock directory
fusermount -uz $MOUNT_POINT
