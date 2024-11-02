#include <linux/module.h>
#include <linux/fs.h>
#include <linux/uaccess.h>
#include <linux/kernel.h>

static ssize_t my_read(struct file *file, char __user *buf, size_t count, loff_t *pos) {
    pr_info("⭐️nfs: reading file...\n");
    return vfs_read(file, buf, count, pos);
}

static struct file_operations fops = {
    .read = my_read,
};

// This function is called when the module is loaded
static int __init my_module_init(void) {
    // Register your file operations here
    // This will require finding the target file's original operations
    // and replacing them with your custom fops.
    return 0;
}

// This function is called when the module is unloaded
static void __exit my_module_exit(void) {
    // Restore the original file operations here
}

module_init(my_module_init);
module_exit(my_module_exit);

// MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Custom NFS Module with Logging");
MODULE_AUTHOR("Your Name");
