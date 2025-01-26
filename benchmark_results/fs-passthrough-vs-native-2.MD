Testing Native Filesystem at /home/ec2-user/my_native_fs_nomount...
Running benchmarks on /home/ec2-user/my_native_fs_nomount

Write Performance:
Mean: 0.000846 seconds
Median: 0.000674 seconds
Std Dev: 0.000327 seconds

Read Performance:
Mean: 0.000282 seconds
Median: 0.000221 seconds
Std Dev: 0.000179 seconds

Random Access Performance:
Mean: 0.000002 seconds
Median: 0.000002 seconds
Std Dev: 0.000001 seconds

Metadata Operation Performance:
Mean: 0.000002 seconds
Median: 0.000002 seconds
Std Dev: 0.000001 seconds

Testing FUSE Filesystem at /home/ec2-user/bench/fake_folder...
Running benchmarks on /home/ec2-user/bench/fake_folder

Write Performance:
Mean: 0.000841 seconds
Median: 0.000678 seconds
Std Dev: 0.000287 seconds

Read Performance:
Mean: 0.000291 seconds
Median: 0.000221 seconds
Std Dev: 0.000232 seconds

Random Access Performance:
Mean: 0.000002 seconds
Median: 0.000002 seconds
Std Dev: 0.000000 seconds

Metadata Operation Performance:
Mean: 0.000003 seconds
Median: 0.000002 seconds
Std Dev: 0.000024 seconds

Performance Comparison (FUSE vs Native):

WRITE:
FUSE/Native ratio: 0.99x slower

READ:
FUSE/Native ratio: 1.03x slower

RANDOM_ACCESS:
FUSE/Native ratio: 0.97x slower

METADATA:
FUSE/Native ratio: 1.35x slower
fusermount: entry for /home/ec2-user/bench/fake_folder not found in /etc/mtab
rm: cannot remove '/home/ec2-user/bench/real_folder': Is a directory