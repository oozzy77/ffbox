#include <linux/module.h>
#include <linux/fs.h>
#include <linux/pagemap.h>
#include <linux/namei.h>

#define MYFS_MAGIC 0x12345678

// Original file operations
static const struct file_operations *original_fops;

// Custom read operation
static ssize_t myfs_read(struct file *file, char __user *buf, size_t count, loff_t *pos) {
    pr_info("myfs: entering read operation\n");
    if (!original_fops || !original_fops->read) {
        pr_err("myfs: original read operation is NULL\n");
        return -EIO;
    }
    pr_info("myfs: reading file at position %lld with count %zu\n", *pos, count);
    ssize_t result = original_fops->read(file, buf, count, pos);
    pr_info("myfs: read operation completed with result %zd\n", result);
    return result;
}

// Custom file operations
static struct file_operations myfs_file_operations;

// Fill superblock with custom operations
static int myfs_fill_super(struct super_block *sb, void *data, int silent) {
    pr_info("myfs: filling superblock\n");
    struct inode *inode = new_inode(sb);
    if (!inode) {
        pr_err("myfs: failed to allocate new inode\n");
        return -ENOMEM;
    }

    // Initialize myfs_file_operations at runtime
    myfs_file_operations.read = myfs_read;
    myfs_file_operations.write = original_fops->write;
    myfs_file_operations.open = original_fops->open;
    myfs_file_operations.release = original_fops->release;
    myfs_file_operations.llseek = original_fops->llseek;
    myfs_file_operations.iterate = original_fops->iterate;
    myfs_file_operations.fsync = original_fops->fsync;
    myfs_file_operations.mmap = original_fops->mmap;
    myfs_file_operations.unlocked_ioctl = original_fops->unlocked_ioctl;
    myfs_file_operations.compat_ioctl = original_fops->compat_ioctl;
    myfs_file_operations.splice_read = original_fops->splice_read;
    myfs_file_operations.splice_write = original_fops->splice_write;
    myfs_file_operations.fasync = original_fops->fasync;
    myfs_file_operations.lock = original_fops->lock;
    myfs_file_operations.flock = original_fops->flock;
    myfs_file_operations.check_flags = original_fops->check_flags;
    myfs_file_operations.setlease = original_fops->setlease;
    myfs_file_operations.fallocate = original_fops->fallocate;
    myfs_file_operations.show_fdinfo = original_fops->show_fdinfo;

    inode->i_ino = 1;
    inode->i_sb = sb;
    inode->i_op = &simple_dir_inode_operations;
    inode->i_fop = &myfs_file_operations; // Use custom file operations

    // Use default dentry operations
    sb->s_d_op = NULL; // NULL indicates using default dentry operations

    sb->s_root = d_make_root(inode);
    if (!sb->s_root) {
        pr_err("myfs: failed to create root dentry\n");
        return -ENOMEM;
    }

    pr_info("myfs: superblock filled successfully\n");
    return 0;
}

// Mount function
static struct dentry *myfs_mount(struct file_system_type *fs_type,
                                 int flags, const char *dev_name,
                                 void *data) {
    pr_info("myfs: mounting filesystem\n");
    struct dentry *dentry = mount_nodev(fs_type, flags, data, myfs_fill_super);
    if (IS_ERR(dentry)) {
        pr_err("myfs: failed to mount filesystem\n");
    } else {
        pr_info("myfs: filesystem mounted successfully\n");
    }
    return dentry;
}

// Filesystem type
static struct file_system_type myfs_type = {
    .owner = THIS_MODULE,
    .name = "myfs",
    .mount = myfs_mount,
    .kill_sb = kill_litter_super,
};

// Function to get file operations from an existing inode
static const struct file_operations *get_current_fops(void) {
    struct inode *inode;
    struct path path;
    int err;

    // Example path to an existing file on the disk
    err = kern_path("/home/ec2-user/ffmm/ffmount.c", LOOKUP_FOLLOW, &path);
    if (err) {
        pr_err("myfs: failed to get path to existing file\n");
        return NULL;
    }

    inode = path.dentry->d_inode;
    if (!inode) {
        pr_err("myfs: inode is NULL\n");
        return NULL;
    }

    return inode->i_fop;
}

// Module initialization
static int __init myfs_init(void) {
    pr_info("myfs: initializing\n");

    // Initialize original_fops with file operations from an existing inode
    original_fops = get_current_fops();
    if (!original_fops) {
        pr_err("myfs: failed to get original file operations\n");
        return -EINVAL;
    }

    int ret = register_filesystem(&myfs_type);
    if (ret == 0) {
        pr_info("myfs: filesystem registered\n");
    } else {
        pr_err("myfs: failed to register filesystem\n");
    }
    return ret;
}

// Module exit
static void __exit myfs_exit(void) {
    pr_info("myfs: exiting\n");
    int ret = unregister_filesystem(&myfs_type);
    if (ret == 0) {
        pr_info("myfs: filesystem unregistered\n");
    } else {
        pr_err("myfs: failed to unregister filesystem\n");
    }
}

module_init(myfs_init);
module_exit(myfs_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Simple Virtual Filesystem with Logging");
MODULE_AUTHOR("Your Name");