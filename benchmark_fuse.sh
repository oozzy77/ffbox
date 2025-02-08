#!/bin/bash
# benchmark_fuse.sh
#
# Usage: ./benchmark_fuse.sh <fuse_mount_point> <native_directory>
#
# This script benchmarks basic file operations on a mounted FUSE filesystem and
# on the native underlying directory. It performs:
#   - A "write test": writing a 100MB file using dd.
#   - A "read test": reading that 100MB file.
#   - A "listing test": recursively listing all entries.
#
# Each test’s elapsed time (in seconds) is printed.
#
# Requirements: dd, ls, bc, pushd/popd

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <fuse_mount_point> <native_directory>"
    exit 1
fi

FUSE_DIR=$1
NATIVE_DIR=$2

if [ ! -d "$FUSE_DIR" ]; then
    echo "Error: FUSE directory '$FUSE_DIR' does not exist."
    exit 1
fi

if [ ! -d "$NATIVE_DIR" ]; then
    echo "Error: Native directory '$NATIVE_DIR' does not exist."
    exit 1
fi

# Function to benchmark a given operation in a specified directory.
benchmark_operation() {
    local dir="$1"
    local test_name="$2"
    local cmd="$3"

    echo "Benchmarking '$test_name' in '$dir'..."
    pushd "$dir" > /dev/null

    # Remove any pre-existing test file
    rm -f testfile

    # Measure time in nanoseconds
    start=$(date +%s%N)
    eval "$cmd"
    end=$(date +%s%N)

    elapsed_ns=$((end - start))
    elapsed_sec=$(echo "scale=3; $elapsed_ns/1000000000" | bc)

    echo "$test_name in '$dir' took $elapsed_sec seconds"
    echo "-------------------------"
    popd > /dev/null
}

echo "Starting benchmarks..."
echo "========================="

# Define commands for benchmarking operations.
# Write test: Create a 100MB file by dumping zeros.
WRITE_CMD="dd if=/dev/zero of=testfile bs=1M count=100 oflag=dsync &> /dev/null"
# Read test: Read the 100MB file.
READ_CMD="dd if=testfile of=/dev/null bs=1M count=100 &> /dev/null"
# Listing test: Recursively list directory entries.
LIST_CMD="ls -lR > /dev/null"

### Benchmark on FUSE mount ###
echo "FUSE Benchmark (directory: $FUSE_DIR)"
benchmark_operation "$FUSE_DIR" "Write Test" "$WRITE_CMD"
benchmark_operation "$FUSE_DIR" "Read Test" "$READ_CMD"
benchmark_operation "$FUSE_DIR" "Listing Test" "$LIST_CMD"
rm -f "$FUSE_DIR/testfile"

echo ""

### Benchmark on Native Directory ###
echo "Native Benchmark (directory: $NATIVE_DIR)"
benchmark_operation "$NATIVE_DIR" "Write Test" "$WRITE_CMD"
benchmark_operation "$NATIVE_DIR" "Read Test" "$READ_CMD"
benchmark_operation "$NATIVE_DIR" "Listing Test" "$LIST_CMD"
rm -f "$NATIVE_DIR/testfile"

echo "Benchmarking complete."