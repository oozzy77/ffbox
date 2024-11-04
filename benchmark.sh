FAKE_FOLDER="/home/ec2-user/bench/fake_folder11"
REAL_FOLDER="/home/ec2-user/bench/real_folder11"

if [ -d "$FAKE_FOLDER" ]; then
    # delete folder
    echo "Deleting $FAKE_FOLDER"
    rm -rf "$FAKE_FOLDER"
fi

if [ ! -d "$FAKE_FOLDER" ]; then
    mkdir -p "$FAKE_FOLDER"
fi

if [ -d "$REAL_FOLDER" ]; then
    # delete folder
    echo "Deleting $REAL_FOLDER"
    rm -rf "$REAL_FOLDER"
fi

if [ ! -d "$REAL_FOLDER" ]; then
    mkdir -p "$REAL_FOLDER"
fi

python3 mount.py "$FAKE_FOLDER" "$REAL_FOLDER"
python3 benchmark_fs.py --fuse "$FAKE_FOLDER" --native /home/ec2-user/my_native_fs_nomount

# unmount
fusermount -u "$FAKE_FOLDER"
# delete folder
rm -rf "$FAKE_FOLDER"
rm -rf "$REAL_FOLDER"
