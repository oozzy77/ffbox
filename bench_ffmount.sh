yes | ffbox mount "s3://ff-image-gen/pyinstaller_sdxl/" ~/bedrock --clean > mount.log 2>&1 &
sleep 3  # Wait for 3 seconds to ensure the mount is available

start_time=$(date +%s)
echo "Start time: $(date)"

cd ~/bedrock
./main/main

end_time=$(date +%s)
echo "End time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Execution time: $minutes min $seconds seconds"

# Unmount the bedrock directory
umount ~/bedrock