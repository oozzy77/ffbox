# Define a variable for the bedrock directory
MOUNT_POINT="$HOME/bedrock"
SOURCE_URL="$HOME/.cache/ffbox/comfyui1"
LOG_FILE="/home/ec2-user/ffbox11.log"

fusermount -uz $MOUNT_POINT
rm -rf $MOUNT_POINT
rm -rf $HOME/.cache/ffbox/tina-comfy
mkdir -p $MOUNT_POINT

# ffbox mount /home/ec2-user/.cache/ffbox/comfy11 $MOUNT_POINT > "$HOME/ffbox11.log" 2>&1 &
yes | ffbox mount $SOURCE_URL $MOUNT_POINT --cache-repo "$HOME/.cache/ffbox/tina-comfy"
mount_pid=$!
echo "Mount process PID: $mount_pid"

# Follow the log file in real-time
# tail -f "$HOME/ffbox11.log" &
sleep 5  # Wait for 5 seconds to ensure the mount is available

start_time=$(date +%s)
echo "Start time: $(date)"

cd ~/hello  # Change to the mount point directory
python main.py  # Run the Python script

end_time=$(date +%s)
echo "End time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Execution time: $minutes min $seconds seconds"

# Unmount the bedrock directory
# fusermount -uz $MOUNT_POINT
