if [ -d /home/ec2-user/bench/fake_folder ]; then
    # delete folder
    echo "Deleting /home/ec2-user/bench/fake_folder"
    rm -rf /home/ec2-user/bench/fake_folder
fi

if [ ! -d /home/ec2-user/bench/fake_folder ]; then
    mkdir -p /home/ec2-user/bench/fake_folder
fi

if [ -d /home/ec2-user/bench/real_folder ]; then
    # delete folder
    echo "Deleting /home/ec2-user/bench/real_folder"
    rm -rf /home/ec2-user/bench/real_folder
fi

if [ ! -d /home/ec2-user/bench/real_folder ]; then
    mkdir -p /home/ec2-user/bench/real_folder
fi

python3 mount.py /home/ec2-user/bench/fake_folder /home/ec2-user/bench/real_folder
python3 benchmark_fs.py --fuse /home/ec2-user/bench/fake_folder --native /home/ec2-user/my_native_fs_nomount

# unmount
fusermount -u /home/ec2-user/bench/fake_folder
# delete folder
rm -rf /home/ec2-user/bench/fake_folder
rm -rf /home/ec2-user/bench/real_folder
