#define FUSE_USE_VERSION 31
#include <fuse.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <string.h>
#include <sys/xattr.h>

// Store the path to the underlying filesystem
static char *underlying_path = NULL;

// Helper to convert FUSE path to underlying path
static void get_full_path(char *full_path, const char *path) {
    sprintf(full_path, "%s%s", underlying_path, path);
}

// Pass through implementations
static int passthrough_getattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    return stat(full_path, statbuf);
}

static int passthrough_read(const char *path, char *buf, size_t size, off_t offset,
                          struct fuse_file_info *fi) {
    int fd = fi->fh;
    return pread(fd, buf, size, offset);
}

static int passthrough_write(const char *path, const char *buf, size_t size,
                           off_t offset, struct fuse_file_info *fi) {
    int fd = fi->fh;
    return pwrite(fd, buf, size, offset);
}

static int passthrough_open(const char *path, struct fuse_file_info *fi) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    int fd = open(full_path, fi->flags);
    if (fd < 0)
        return -errno;
    fi->fh = fd;
    return 0;
}

static int passthrough_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                             off_t offset, struct fuse_file_info *fi,
                             enum fuse_readdir_flags flags) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    
    DIR *dp = opendir(full_path);
    if (!dp)
        return -errno;

    struct dirent *de;
    while ((de = readdir(dp))) {
        filler(buf, de->d_name, NULL, 0, 0);
    }
    closedir(dp);
    return 0;
}

static int passthrough_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    int fd = open(full_path, fi->flags, mode);
    if (fd < 0)
        return -errno;
    fi->fh = fd;
    return 0;
}

static int passthrough_mkdir(const char *path, mode_t mode) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    return mkdir(full_path, mode) == 0 ? 0 : -errno;
}

static int passthrough_unlink(const char *path) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    return unlink(full_path) == 0 ? 0 : -errno;
}

static int passthrough_rmdir(const char *path) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    return rmdir(full_path) == 0 ? 0 : -errno;
}

static int passthrough_rename(const char *from, const char *to, unsigned int flags) {
    char full_from[PATH_MAX], full_to[PATH_MAX];
    get_full_path(full_from, from);
    get_full_path(full_to, to);
    return rename(full_from, full_to) == 0 ? 0 : -errno;
}

static int passthrough_chmod(const char *path, mode_t mode, struct fuse_file_info *fi) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    return chmod(full_path, mode) == 0 ? 0 : -errno;
}

static int passthrough_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    return chown(full_path, uid, gid) == 0 ? 0 : -errno;
}

static int passthrough_truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    return truncate(full_path, size) == 0 ? 0 : -errno;
}

static int passthrough_utimens(const char *path, const struct timespec ts[2], struct fuse_file_info *fi) {
    char full_path[PATH_MAX];
    get_full_path(full_path, path);
    return utimensat(AT_FDCWD, full_path, ts, AT_SYMLINK_NOFOLLOW) == 0 ? 0 : -errno;
}

static int passthrough_release(const char *path, struct fuse_file_info *fi) {
    return close(fi->fh) == 0 ? 0 : -errno;
}

static int passthrough_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
    if (datasync)
        return fdatasync(fi->fh) == 0 ? 0 : -errno;
    else
        return fsync(fi->fh) == 0 ? 0 : -errno;
}

static struct fuse_operations passthrough_ops = {
    .getattr     = passthrough_getattr,
    .mkdir       = passthrough_mkdir,
    .unlink      = passthrough_unlink,
    .rmdir       = passthrough_rmdir,
    .rename      = passthrough_rename,
    .chmod       = passthrough_chmod,
    .chown       = passthrough_chown,
    .truncate    = passthrough_truncate,
    .open        = passthrough_open,
    .read        = passthrough_read,
    .write       = passthrough_write,
    .create      = passthrough_create,
    .release     = passthrough_release,
    .fsync       = passthrough_fsync,
    .readdir     = passthrough_readdir,
    .utimens     = passthrough_utimens,
};

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <underlying_path> <mount_point> [FUSE options]\n", argv[0]);
        return 1;
    }

    underlying_path = realpath(argv[1], NULL);
    // Remove the underlying path from argv
    argv[1] = argv[2];
    argc--;

    return fuse_main(argc, argv, &passthrough_ops, NULL);
}