start_time=$(date +%s)
echo "Docker pull start time: $(date)"

sudo docker pull nozyio77/pyinstall_sdxl_gen

end_time=$(date +%s)
echo "Docker pull end time: $(date)"

time_diff=$((end_time - start_time))
minutes=$((time_diff / 60))
seconds=$((time_diff % 60))
echo "Docker pull execution time: $minutes min $seconds seconds"

start_time_docker_run=$(date +%s)
echo "Docker run start time: $(date)"

sudo docker run --gpus all -it nozyio77/pyinstall_sdxl_gen

end_time_docker_run=$(date +%s)
echo "Docker run end time: $(date)"

time_diff_docker_run=$((end_time_docker_run - start_time_docker_run))
minutes_docker_run=$((time_diff_docker_run / 60))
seconds_docker_run=$((time_diff_docker_run % 60))
echo "Docker run execution time: $minutes_docker_run min $seconds_docker_run seconds"


time_diff_total=$((end_time_docker_run - start_time))
minutes_total=$((time_diff_total / 60))
seconds_total=$((time_diff_total % 60))
echo "Total execution time: $minutes_total min $seconds_total seconds"