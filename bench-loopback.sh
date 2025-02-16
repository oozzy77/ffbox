# Define a variable for the bedrock directory
MOUNT_POINT="$HOME/bedrock"
S3_URL="ffbox-ea1"
LOG_FILE="$HOME/ffbox_mount.log"

fusermount -uz $MOUNT_POINT
mkdir -p $MOUNT_POINT


~/ffbox/mycli/mycli $MOUNT_POINT $S3_URL 
sleep 3  # Wait for 3 seconds to ensure the mount is available
cd $MOUNT_POINT/pyinstall_sdxl_gen/

start_time=$(date +%s)
echo "Start time: $(date)"

# ./main/main
strace -tt -T -e trace=read,file -o "$HOME/strace_goofys.log" main/main
# strace -tt -T -f -e trace=all -e signal=all -o "$HOME/pyinstall_ffbox_bench11.log" main/main

end_time=$(date +%s)
echo "End time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Execution time: $minutes min $seconds seconds"

# Unmount the bedrock directory
fusermount -uz $MOUNT_POINT
