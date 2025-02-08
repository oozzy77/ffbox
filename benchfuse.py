#!/usr/bin/env python3

import os
import time
import random
import shutil
import statistics
from concurrent.futures import ThreadPoolExecutor

class FSBenchmark:
    def __init__(self, test_dir):
        if not os.path.exists(test_dir):
            raise ValueError(f"Test directory {test_dir} does not exist")
        self.test_dir = test_dir
        self.results = {}
        
    def setup(self):
        """Create test directory if it doesn't exist"""
        os.makedirs(self.test_dir, exist_ok=True)
        
    def cleanup(self):
        """Clean up test files"""
        shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir, exist_ok=True)

    def benchmark_write(self, file_size=1024*1024, num_files=100):
        """Benchmark write performance"""
        data = b'x' * 10240  # 1KB chunk
        times = []
        
        for i in range(num_files):
            filepath = os.path.join(self.test_dir, f'test_write_{i}.dat')
            start_time = time.time()
            
            with open(filepath, 'wb') as f:
                for _ in range(file_size // 1024):
                    f.write(data)
            
            times.append(time.time() - start_time)
        
        return {
            'operation': 'write',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_read(self, num_files=100):
        """Benchmark read performance"""
        times = []
        
        for i in range(num_files):
            filepath = os.path.join(self.test_dir, f'test_write_{i}.dat')
            start_time = time.time()
            
            with open(filepath, 'rb') as f:
                while f.read(8192):  # Read in 8KB chunks
                    pass
            
            times.append(time.time() - start_time)
        
        return {
            'operation': 'read',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_random_access(self, num_operations=1000):
        """Benchmark random access performance"""
        filepath = os.path.join(self.test_dir, 'test_write_0.dat')
        file_size = os.path.getsize(filepath)
        times = []
        
        with open(filepath, 'rb') as f:
            for _ in range(num_operations):
                offset = random.randint(0, file_size - 1024)
                start_time = time.time()
                f.seek(offset)
                f.read(1024)
                times.append(time.time() - start_time)
        
        return {
            'operation': 'random_access',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_metadata(self, num_operations=1000):
        """Benchmark metadata operations (stat)"""
        filepath = os.path.join(self.test_dir, 'test_write_0.dat')
        times = []
        
        for _ in range(num_operations):
            start_time = time.time()
            os.stat(filepath)
            times.append(time.time() - start_time)
        
        return {
            'operation': 'metadata',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_file_copy(self, num_files=100):
        """Benchmark file copy performance"""
        times = []
        
        for i in range(num_files):
            src = os.path.join(self.test_dir, f'test_write_{i}.dat')
            dst = os.path.join(self.test_dir, f'test_copy_{i}.dat')
            start_time = time.time()
            
            shutil.copy(src, dst)
            
            times.append(time.time() - start_time)
        
        return {
            'operation': 'file_copy',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_dir_create(self, num_dirs=100):
        """Benchmark directory creation performance"""
        times = []
        
        for i in range(num_dirs):
            dir_path = os.path.join(self.test_dir, f'test_dir_{i}')
            start_time = time.time()
            
            os.makedirs(dir_path, exist_ok=True)
            
            times.append(time.time() - start_time)
        
        return {
            'operation': 'dir_create',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_dir_switch(self, num_switches=1000):
        """Benchmark directory switching performance"""
        times = []
        original_dir = os.getcwd()
        
        for _ in range(num_switches):
            start_time = time.time()
            
            os.chdir(self.test_dir)
            os.chdir(original_dir)
            
            times.append(time.time() - start_time)
        
        return {
            'operation': 'dir_switch',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_dir_copy(self, num_dirs=10):
        """Benchmark directory copy performance"""
        times = []
        
        for i in range(num_dirs):
            src = os.path.join(self.test_dir, f'test_dir_{i}')
            dst = os.path.join(self.test_dir, f'test_dir_copy_{i}')
            os.makedirs(src, exist_ok=True)  # Ensure source directory exists
            start_time = time.time()
            
            shutil.copytree(src, dst)
            
            times.append(time.time() - start_time)
        
        return {
            'operation': 'dir_copy',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_dir_rename(self, num_dirs=10):
        """Benchmark directory rename performance"""
        times = []
        
        for i in range(num_dirs):
            src = os.path.join(self.test_dir, f'test_dir_{i}')
            dst = os.path.join(self.test_dir, f'test_dir_renamed_{i}')
            os.makedirs(src, exist_ok=True)  # Ensure source directory exists
            start_time = time.time()
            
            os.rename(src, dst)
            
            times.append(time.time() - start_time)
        
        return {
            'operation': 'dir_rename',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def benchmark_dir_move(self, num_dirs=10):
        """Benchmark directory move performance"""
        times = []
        
        for i in range(num_dirs):
            src = os.path.join(self.test_dir, f'test_dir_renamed_{i}')
            dst = os.path.join(self.test_dir, f'test_dir_moved_{i}')
            os.makedirs(src, exist_ok=True)  # Ensure source directory exists
            start_time = time.time()
            
            shutil.move(src, dst)
            
            times.append(time.time() - start_time)
        
        return {
            'operation': 'dir_move',
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }

    def run_all_benchmarks(self):
        """Run all benchmarks and return results"""
        print(f"Running benchmarks on {self.test_dir}")
        
        self.setup()
        
        # Run write benchmark
        write_results = self.benchmark_write()
        print(f"\nWrite Performance:")
        print(f"Mean: {write_results['mean']:.6f} seconds")
        print(f"Median: {write_results['median']:.6f} seconds")
        print(f"Std Dev: {write_results['std_dev']:.6f} seconds")
        
        # Run read benchmark
        read_results = self.benchmark_read()
        print(f"\nRead Performance:")
        print(f"Mean: {read_results['mean']:.6f} seconds")
        print(f"Median: {read_results['median']:.6f} seconds")
        print(f"Std Dev: {read_results['std_dev']:.6f} seconds")
        
        # Run random access benchmark
        random_results = self.benchmark_random_access()
        print(f"\nRandom Access Performance:")
        print(f"Mean: {random_results['mean']:.6f} seconds")
        print(f"Median: {random_results['median']:.6f} seconds")
        print(f"Std Dev: {random_results['std_dev']:.6f} seconds")
        
        # Run metadata benchmark
        metadata_results = self.benchmark_metadata()
        print(f"\nMetadata Operation Performance:")
        print(f"Mean: {metadata_results['mean']:.6f} seconds")
        print(f"Median: {metadata_results['median']:.6f} seconds")
        print(f"Std Dev: {metadata_results['std_dev']:.6f} seconds")

        # Run file copy benchmark
        file_copy_results = self.benchmark_file_copy()
        print(f"\nFile Copy Performance:")
        print(f"Mean: {file_copy_results['mean']:.6f} seconds")
        print(f"Median: {file_copy_results['median']:.6f} seconds")
        print(f"Std Dev: {file_copy_results['std_dev']:.6f} seconds")

        # Run directory creation benchmark
        dir_create_results = self.benchmark_dir_create()
        print(f"\nDirectory Creation Performance:")
        print(f"Mean: {dir_create_results['mean']:.6f} seconds")
        print(f"Median: {dir_create_results['median']:.6f} seconds")
        print(f"Std Dev: {dir_create_results['std_dev']:.6f} seconds")

        # Run directory switch benchmark
        dir_switch_results = self.benchmark_dir_switch()
        print(f"\nDirectory Switch Performance:")
        print(f"Mean: {dir_switch_results['mean']:.6f} seconds")
        print(f"Median: {dir_switch_results['median']:.6f} seconds")
        print(f"Std Dev: {dir_switch_results['std_dev']:.6f} seconds")

        # Run directory copy benchmark
        dir_copy_results = self.benchmark_dir_copy()
        print(f"\nDirectory Copy Performance:")
        print(f"Mean: {dir_copy_results['mean']:.6f} seconds")
        print(f"Median: {dir_copy_results['median']:.6f} seconds")
        print(f"Std Dev: {dir_copy_results['std_dev']:.6f} seconds")

        # Run directory rename benchmark
        dir_rename_results = self.benchmark_dir_rename()
        print(f"\nDirectory Rename Performance:")
        print(f"Mean: {dir_rename_results['mean']:.6f} seconds")
        print(f"Median: {dir_rename_results['median']:.6f} seconds")
        print(f"Std Dev: {dir_rename_results['std_dev']:.6f} seconds")

        # Run directory move benchmark
        dir_move_results = self.benchmark_dir_move()
        print(f"\nDirectory Move Performance:")
        print(f"Mean: {dir_move_results['mean']:.6f} seconds")
        print(f"Median: {dir_move_results['median']:.6f} seconds")
        print(f"Std Dev: {dir_move_results['std_dev']:.6f} seconds")

        self.cleanup()
        
        return {
            'write': write_results,
            'read': read_results,
            'random_access': random_results,
            'metadata': metadata_results,
            'file_copy': file_copy_results,
            'dir_create': dir_create_results,
            'dir_switch': dir_switch_results,
            'dir_copy': dir_copy_results,
            'dir_rename': dir_rename_results,
            'dir_move': dir_move_results
        }

if __name__ == '__main__':
    import argparse
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Benchmark filesystem performance')
    parser.add_argument('--native', default='/tmp/native_fs_test',
                      help='Path for testing native filesystem')
    parser.add_argument('--fuse', default='/bench/fake_folder',
                      help='Path for testing FUSE filesystem')
    args = parser.parse_args()
    
    # Test native filesystem
    native_benchmark = FSBenchmark(args.native)
    print(f"Testing Native Filesystem at {args.native}...")
    native_results = native_benchmark.run_all_benchmarks()
    
    # Test FUSE filesystem
    fuse_benchmark = FSBenchmark(args.fuse)
    print(f"\nTesting FUSE Filesystem at {args.fuse}...")
    fuse_results = fuse_benchmark.run_all_benchmarks()
    
    # Print comparison
    print("\nPerformance Comparison (FUSE vs Native):")
    for operation in ['write', 'read', 'random_access', 'metadata', 'file_copy', 'dir_create', 'dir_switch', 'dir_copy', 'dir_rename', 'dir_move']:
        ratio = fuse_results[operation]['mean'] / native_results[operation]['mean']
        print(f"\n{operation.upper()}:")
        print(f"FUSE/Native ratio: {ratio:.2f}x slower")