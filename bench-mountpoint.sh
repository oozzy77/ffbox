# Define a variable for the bedrock directory
MOUNT_POINT="$HOME/bedrock"
S3_URL="ffbox-ea1"
LOG_FILE="$HOME/ffbox_mount.log"

fusermount -uz $MOUNT_POINT
mkdir -p $MOUNT_POINT


mount-s3 $S3_URL $MOUNT_POINT --file-mode=755 
sleep 3  # Wait for 3 seconds to ensure the mount is available

start_time=$(date +%s)
echo "Start time: $(date)"

cd $MOUNT_POINT/pyinstall_sdxl_gen/

# ./main/main
strace -tt -T -e trace=file -o "$HOME/strace_mountpoint.log" main/main
# strace -tt -T -f -e trace=all -e signal=all -o "$HOME/pyinstall_ffbox_bench11.log" main/main

end_time=$(date +%s)
echo "End time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Execution time: $minutes min $seconds seconds"

# Unmount the bedrock directory
fusermount -uz $MOUNT_POINT
