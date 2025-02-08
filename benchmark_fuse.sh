#!/bin/bash
# benchmark.sh: Compare write, read, and listdir performance on two directories.
#
# Usage: ./benchmark.sh <mountpath> <realpath> [iterations]
#   - <mountpath>:   Directory for the mounted filesystem.
#   - <realpath>:    Directory for the local filesystem.
#   - [iterations]:  (Optional) Number of iterations per test (default: 100)
#
# Note:
#   - The script writes and reads 1 MB of data for write/read tests.
#   - For listdir, it creates a temporary subdirectory with 100 dummy files.
#   - Timing is done in nanoseconds using "date +%s%N".
#   - Not all systems support nanosecond resolution.

# Check for required arguments.
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <mountpath> <realpath> [iterations]"
    exit 1
fi

MOUNTPATH="$1"
REALPATH="$2"
ITERATIONS=${3:-100}

# Function: write_test
#   Writes 1 MB of random data to a temporary file and removes it.
#   Sums the elapsed time (in ns) over a number of iterations.
write_test() {
    local directory="$1"
    local iterations="$2"
    local total_time_ns=0

    for i in $(seq 1 "$iterations"); do
        local tempfile="${directory}/writetest_$$_${i}.tmp"
        local start=$(date +%s%N)
        # Write 1 MB (1048576 bytes) from /dev/urandom.
        head -c 1048576 </dev/urandom > "$tempfile"
        local end=$(date +%s%N)
        local diff=$(( end - start ))
        total_time_ns=$(( total_time_ns + diff ))
        rm -f "$tempfile"
    done
    echo "$total_time_ns"
}

# Function: read_test
#   Creates a 1 MB file, then reads its contents in each iteration.
#   Sums the elapsed time (in ns) over a number of iterations.
read_test() {
    local directory="$1"
    local iterations="$2"
    local tempfile="${directory}/readtest_$$.tmp"
    head -c 1048576 </dev/urandom > "$tempfile"
    local total_time_ns=0

    for i in $(seq 1 "$iterations"); do
        local start=$(date +%s%N)
        # Read the file and discard the output.
        cat "$tempfile" > /dev/null
        local end=$(date +%s%N)
        local diff=$(( end - start ))
        total_time_ns=$(( total_time_ns + diff ))
    done
    rm -f "$tempfile"
    echo "$total_time_ns"
}

# Function: listdir_test
#   Creates a temporary subdirectory with 100 dummy files, then lists its contents.
#   Sums the elapsed time (in ns) over a number of iterations.
listdir_test() {
    local directory="$1"
    local iterations="$2"
    # Create a temporary subdirectory.
    local subdir
    subdir=$(mktemp -d "${directory}/listdirtest.XXXXXX")
    
    # Populate the temporary directory with 100 dummy files.
    for i in $(seq 1 100); do
        touch "${subdir}/dummy_${i}"
    done

    local total_time_ns=0
    for i in $(seq 1 "$iterations"); do
        local start=$(date +%s%N)
        # List the subdirectory; output is discarded.
        ls "$subdir" > /dev/null
        local end=$(date +%s%N)
        local diff=$(( end - start ))
        total_time_ns=$(( total_time_ns + diff ))
    done

    # Clean up.
    rm -f "${subdir}"/*
    rmdir "$subdir"
    echo "$total_time_ns"
}

# Helper: Convert nanoseconds to seconds (with six decimal places) using awk.
ns_to_sec() {
    awk "BEGIN {printf \"%.6f\", $1/1000000000}"
}

echo "Running benchmark tests with $ITERATIONS iterations each..."
echo ""

# Perform write tests.
echo "Performing write tests..."
mount_write_ns=$(write_test "$MOUNTPATH" "$ITERATIONS")
real_write_ns=$(write_test "$REALPATH" "$ITERATIONS")

# Perform read tests.
echo "Performing read tests..."
mount_read_ns=$(read_test "$MOUNTPATH" "$ITERATIONS")
real_read_ns=$(read_test "$REALPATH" "$ITERATIONS")

# Perform listdir tests.
echo "Performing listdir tests..."
mount_list_ns=$(listdir_test "$MOUNTPATH" "$ITERATIONS")
real_list_ns=$(listdir_test "$REALPATH" "$ITERATIONS")

# Compute average times in seconds.
avg_mount_write=$(ns_to_sec "$(( mount_write_ns / ITERATIONS ))")
avg_real_write=$(ns_to_sec "$(( real_write_ns / ITERATIONS ))")
avg_mount_read=$(ns_to_sec "$(( mount_read_ns / ITERATIONS ))")
avg_real_read=$(ns_to_sec "$(( real_read_ns / ITERATIONS ))")
avg_mount_list=$(ns_to_sec "$(( mount_list_ns / ITERATIONS ))")
avg_real_list=$(ns_to_sec "$(( real_list_ns / ITERATIONS ))")

echo ""
echo "Benchmark Results (average time per operation in seconds):"
printf "%-12s %12s %12s\n" "Operation" "Mount Path" "Real Path"
echo "----------------------------------------------"
printf "%-12s %12s %12s\n" "Write" "$avg_mount_write" "$avg_real_write"
printf "%-12s %12s %12s\n" "Read"  "$avg_mount_read"  "$avg_real_read"
printf "%-12s %12s %12s\n" "Listdir" "$avg_mount_list" "$avg_real_list"
