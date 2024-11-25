fusermount -uz ~/bedrock
yes | ffbox mount "s3://ffbox-ea1/pyinstall_sdxl_gen/" "$HOME/bedrock" --clean > "$HOME/ffbox_mount.log" 2>&1 &
sleep 4  # Wait for 3 seconds to ensure the mount is available

start_time=$(date +%s)
echo "Start time: $(date)"

cd ~/bedrock
./main/main
# strace -tt -T -e trace=file -o "$HOME/pyinstall_ffbox_bench11.log" main/main

end_time=$(date +%s)
echo "End time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Execution time: $minutes min $seconds seconds"

# Unmount the bedrock directory
fusermount -uz ~/bedrock