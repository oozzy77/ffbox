Testing Native Filesystem at /home/ec2-user/my_native_fs_nomount...
Running benchmarks on /home/ec2-user/my_native_fs_nomount

Write Performance:
Mean: 0.000843 seconds
Median: 0.000667 seconds
Std Dev: 0.000317 seconds

Read Performance:
Mean: 0.000280 seconds
Median: 0.000219 seconds
Std Dev: 0.000173 seconds

Random Access Performance:
Mean: 0.000003 seconds
Median: 0.000002 seconds
Std Dev: 0.000006 seconds

Metadata Operation Performance:
Mean: 0.000002 seconds
Median: 0.000002 seconds
Std Dev: 0.000003 seconds

Testing FUSE Filesystem at /home/ec2-user/bench/fake_folder...
Running benchmarks on /home/ec2-user/bench/fake_folder

Write Performance:
Mean: 0.000878 seconds
Median: 0.000682 seconds
Std Dev: 0.000324 seconds

Read Performance:
Mean: 0.000282 seconds
Median: 0.000219 seconds
Std Dev: 0.000190 seconds

Random Access Performance:
Mean: 0.000003 seconds
Median: 0.000002 seconds
Std Dev: 0.000013 seconds

Metadata Operation Performance:
Mean: 0.000004 seconds
Median: 0.000002 seconds
Std Dev: 0.000034 seconds

Performance Comparison (FUSE vs Native):

WRITE:
FUSE/Native ratio: 1.04x slower

READ:
FUSE/Native ratio: 1.01x slower

RANDOM_ACCESS:
FUSE/Native ratio: 1.05x slower

METADATA:
FUSE/Native ratio: 1.60x slower
fusermount: entry for /home/ec2-user/bench/fake_folder not found in /etc/mtab
rm: cannot remove '/home/ec2-user/bench/real_folder': Is a directory