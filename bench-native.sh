start_time=$(date +%s)
echo "Start time: $(date)"

cd ~/pyinstall_sdxl_gen
./main/main

end_time=$(date +%s)
echo "End time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Execution time: $minutes min $seconds seconds"

