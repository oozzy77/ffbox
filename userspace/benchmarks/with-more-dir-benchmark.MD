[ec2-user@ip-172-31-30-104 ffmount]$ ./benchmark.sh
s3 bucket name: 
Warning: /home/ec2-user/bench/real_folder11 already exists, do you want to override?
y/n: y
real storage path: /home/ec2-user/realer22/, fake storage path: /home/ec2-user/bench/real_folder11
init s3_url: /home/ec2-user/bench/fake_folder11
init root: /home/ec2-user/realer22/
^CTesting Native Filesystem at /home/ec2-user/my_native_fs_nomount...
Running benchmarks on /home/ec2-user/my_native_fs_nomount

Write Performance:
Mean: 0.000879 seconds
Median: 0.000704 seconds
Std Dev: 0.000294 seconds

Read Performance:
Mean: 0.000299 seconds
Median: 0.000234 seconds
Std Dev: 0.000200 seconds

Random Access Performance:
Mean: 0.000003 seconds
Median: 0.000002 seconds
Std Dev: 0.000001 seconds

Metadata Operation Performance:
Mean: 0.000002 seconds
Median: 0.000002 seconds
Std Dev: 0.000000 seconds

File Copy Performance:
Mean: 0.000621 seconds
Median: 0.000480 seconds
Std Dev: 0.000312 seconds

Directory Creation Performance:
Mean: 0.000019 seconds
Median: 0.000014 seconds
Std Dev: 0.000040 seconds

Directory Switch Performance:
Mean: 0.000004 seconds
Median: 0.000003 seconds
Std Dev: 0.000031 seconds

Directory Copy Performance:
Mean: 0.000048 seconds
Median: 0.000040 seconds
Std Dev: 0.000024 seconds

Directory Rename Performance:
Mean: 0.000010 seconds
Median: 0.000008 seconds
Std Dev: 0.000008 seconds

Directory Move Performance:
Mean: 0.000012 seconds
Median: 0.000010 seconds
Std Dev: 0.000005 seconds

Testing FUSE Filesystem at /home/ec2-user/bench/fake_folder11...
Running benchmarks on /home/ec2-user/bench/fake_folder11

Write Performance:
Mean: 0.000900 seconds
Median: 0.000697 seconds
Std Dev: 0.000358 seconds

Read Performance:
Mean: 0.000305 seconds
Median: 0.000232 seconds
Std Dev: 0.000200 seconds

Random Access Performance:
Mean: 0.000003 seconds
Median: 0.000002 seconds
Std Dev: 0.000013 seconds

Metadata Operation Performance:
Mean: 0.000003 seconds
Median: 0.000002 seconds
Std Dev: 0.000014 seconds

File Copy Performance:
Mean: 0.000633 seconds
Median: 0.000492 seconds
Std Dev: 0.000310 seconds

Directory Creation Performance:
Mean: 0.000018 seconds
Median: 0.000014 seconds
Std Dev: 0.000039 seconds

Directory Switch Performance:
Mean: 0.000004 seconds
Median: 0.000003 seconds
Std Dev: 0.000024 seconds

Directory Copy Performance:
Mean: 0.000051 seconds
Median: 0.000042 seconds
Std Dev: 0.000023 seconds

Directory Rename Performance:
Mean: 0.000009 seconds
Median: 0.000008 seconds
Std Dev: 0.000002 seconds

Directory Move Performance:
Mean: 0.000062 seconds
Median: 0.000011 seconds
Std Dev: 0.000156 seconds

Performance Comparison (FUSE vs Native):

WRITE:
FUSE/Native ratio: 1.02x slower

READ:
FUSE/Native ratio: 1.02x slower

RANDOM_ACCESS:
FUSE/Native ratio: 1.14x slower

METADATA:
FUSE/Native ratio: 1.26x slower

FILE_COPY:
FUSE/Native ratio: 1.02x slower

DIR_CREATE:
FUSE/Native ratio: 0.99x slower

DIR_SWITCH:
FUSE/Native ratio: 1.04x slower

DIR_COPY:
FUSE/Native ratio: 1.06x slower

DIR_RENAME:
FUSE/Native ratio: 0.86x slower

DIR_MOVE:
FUSE/Native ratio: 5.16x slower
fusermount: entry for /home/ec2-user/bench/fake_folder11 not found in /etc/mtab