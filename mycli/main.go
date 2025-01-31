package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// We’ll store our meta-dir under the local cache.
const metaDir = ".ffbox_noot"

// FFBox represents a directory node in FUSE, holding S3 + local cache info.
type FFBox struct {
	fs.Inode

	// LocalCacheRoot is the real local directory where we store downloaded files.
	LocalCacheRoot string

	// S3 client & bucket info
	S3Client *s3.Client
	Bucket   string
	Prefix   string

	// For concurrency: locks by path
	fileLocks sync.Map // map[string]*sync.Mutex

	// Keep track of which directories have been enumerated (cached listing)
	dirCache sync.Map // map[string]bool
}

// FFBoxFile represents a file node in FUSE.
type FFBoxFile struct {
	fs.Inode
	ffbox *FFBox
	path  string // relative path within the mount
}

// FileHandle wraps an os.File for reading/writing.
type FileHandle struct {
	file *os.File
}

// Ensure FFBox and FFBoxFile implement relevant interfaces:
var (
	_ fs.NodeLookuper   = (*FFBox)(nil)
	_ fs.NodeReaddirer  = (*FFBox)(nil)
	_ fs.NodeGetattrer  = (*FFBox)(nil)
	_ fs.NodeOpener     = (*FFBoxFile)(nil)
	_ fs.NodeReader     = (*FFBoxFile)(nil)
	_ fs.NodeGetattrer  = (*FFBoxFile)(nil)
	_ fs.FileHandle     = (*FileHandle)(nil)
	_ fs.FileReleaser   = (*FileHandle)(nil)
)

// -------------------------
//    Helpers
// -------------------------

// localPath returns the absolute local path on disk for a FUSE path.
func (f *FFBox) localPath(fusePath string) string {
	return filepath.Join(f.LocalCacheRoot, fusePath)
}

// objectKey returns the S3 key (bucket path) for a fuse path.
func (f *FFBox) objectKey(fusePath string) string {
	trimmed := strings.TrimPrefix(fusePath, "/")
	trimmed = strings.TrimPrefix(trimmed, f.Prefix)
	trimmed = strings.TrimPrefix(trimmed, "/")

	if f.Prefix == "" {
		return trimmed
	}
	// Use filepath.Join, then replace backslashes with forward slashes
	key := filepath.Join(f.Prefix, trimmed)
	return strings.ReplaceAll(key, `\`, `/`)
}

// isDirCached checks if a directory's contents have already been enumerated.
func (f *FFBox) isDirCached(p string) bool {
	_, ok := f.dirCache.Load(p)
	return ok
}

// markDirCached marks a directory as enumerated/cached.
func (f *FFBox) markDirCached(p string) {
	f.dirCache.Store(p, true)
}

// isFileCached checks if we have previously fully downloaded a file.
// This code is a placeholder; in practice, you might check file existence
// plus a special extended attribute, etc.
func (f *FFBox) isFileCached(rel string) bool {
	lp := f.localPath(rel)
	if _, err := os.Lstat(lp); err == nil {
		// Could also check xattrs or a .cached marker file.
		return false // Simplify to false if you want to force re-download.
	}
	return false
}

// markFileCached sets an attribute or marker that we have the file.
// This is a no-op placeholder here.
func (f *FFBox) markFileCached(rel string) error {
	// For example, set an xattr or create a .cached marker:
	// ...
	return nil
}

// getFileLock returns a mutex for the given relative path (for concurrency).
func (f *FFBox) getFileLock(rel string) *sync.Mutex {
	val, _ := f.fileLocks.LoadOrStore(rel, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// downloadIfNeeded fetches the file from S3 if not cached locally.
func (f *FFBox) downloadIfNeeded(rel string) error {
	if f.isFileCached(rel) {
		return nil // Already good
	}

	lock := f.getFileLock(rel)
	lock.Lock()
	defer lock.Unlock()

	// Check again after we have the lock:
	if f.isFileCached(rel) {
		return nil
	}

	localPath := f.localPath(rel)
	key := f.objectKey(rel)

	// Ensure the parent directory exists
	err := os.MkdirAll(filepath.Dir(localPath), 0755)
	if err != nil {
		return err
	}

	fmt.Printf("Downloading s3://%s/%s --> %s\n", f.Bucket, key, localPath)

	getObjInput := &s3.GetObjectInput{
		Bucket: &f.Bucket,
		Key:    &key,
	}
	resp, err := f.S3Client.GetObject(context.TODO(), getObjInput)
	if err != nil {
		// Could check for 404:
		// var noSuchKey *types.NoSuchKey
		// if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "NoSuchKey") ||
		// 	strings.Contains(err.Error(), "404") || (err != nil && isNoSuchKey(err)) {
		// 	return syscall.ENOENT
		// }
		return syscall.EIO
	}
	defer resp.Body.Close()

	outFile, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	if _, err := io.Copy(outFile, resp.Body); err != nil {
		_ = os.Remove(localPath) // remove partial
		return err
	}

	if err := outFile.Sync(); err != nil {
		return err
	}

	// Mark as downloaded
	_ = f.markFileCached(rel)
	return nil
}

// isNoSuchKey is a little helper to detect NoSuchKey from the error chain.
func isNoSuchKey(err error) bool {
	var nsk *types.NoSuchKey
	if ok := errorAs(err, &nsk); ok {
		return true
	}
	return false
}

// errorAs is a helper to handle AWS v2 typed errors (like errors.As).
func errorAs(src error, target interface{}) bool {
	// If using Go 1.13+, you can do errors.As(src, &target)
	// but let's keep it simple:
	return false
}

// listObjects returns subdirectories and files for the given S3 prefix
func (f *FFBox) listObjects(rel string) ([]string, []string, error) {
	// Convert to S3 prefix
	prefixKey := f.objectKey(rel)
	if prefixKey != "" && !strings.HasSuffix(prefixKey, "/") {
		prefixKey += "/"
	}

	input := &s3.ListObjectsV2Input{
		Bucket:    &f.Bucket,
		Prefix:    &prefixKey,
		Delimiter: aws.String("/"),
	}

	var dirs []string
	var files []string

	paginator := s3.NewListObjectsV2Paginator(f.S3Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, nil, err
		}
		for _, cp := range page.CommonPrefixes {
			dirFull := *cp.Prefix
			dirName := strings.TrimSuffix(strings.TrimPrefix(dirFull, prefixKey), "/")
			if dirName != "" {
				dirs = append(dirs, dirName)
			}
		}
		for _, content := range page.Contents {
			fileFull := *content.Key
			fileName := strings.TrimPrefix(fileFull, prefixKey)
			if fileName != "" {
				files = append(files, fileName)
			}
		}
	}

	return dirs, files, nil
}

// -------------------------
//    FFBox (Dir) methods
// -------------------------

// Lookup is called when the kernel wants to look up a child in a directory.
func (f *FFBox) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if strings.HasPrefix(name, metaDir) {
		return nil, syscall.ENOENT
	}

	fullRel := filepath.Join(f.Path(nil), name)
	localPath := f.localPath(fullRel)

	fi, err := os.Lstat(localPath)
	if os.IsNotExist(err) {
		// Possibly not enumerated yet. Let's readdir the parent.
		parent := filepath.Dir(f.Path(nil))
		if !f.isDirCached(parent) {
			_, _ = f.Readdir(ctx)
			fi, err = os.Lstat(localPath)
		}
	}

	if err != nil {
		return nil, fs.ToErrno(err)
	}
	if fi.IsDir() {
		child := &FFBox{
			LocalCacheRoot: f.LocalCacheRoot,
			S3Client:       f.S3Client,
			Bucket:         f.Bucket,
			Prefix:         f.Prefix,
		}
		stable := fs.StableAttr{Mode: fuse.S_IFDIR}
		return f.NewInode(ctx, child, stable), 0
	}
	// It's a file
	child := &FFBoxFile{
		ffbox: f,
		path:  fullRel,
	}
	stable := fs.StableAttr{Mode: fuse.S_IFREG}
	return f.NewInode(ctx, child, stable), 0
}

// Readdir lists directory contents.
func (f *FFBox) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dirPath := f.Path(nil)
	if dirPath == "/" {
		dirPath = ""
	}
	if f.isDirCached(dirPath) {
		return fs.NewLoopbackDirStream(f.localPath(dirPath))
	}

	// Otherwise, list S3
	dirs, files, err := f.listObjects(dirPath)
	if err != nil {
		log.Printf("Error in listObjects: %v", err)
		return nil, fs.ToErrno(err)
	}

	// Create local directories
	for _, d := range dirs {
		lp := filepath.Join(f.localPath(dirPath), d)
		_ = os.MkdirAll(lp, 0755)
	}

	// Create local placeholder files
	for _, fi := range files {
		lp := filepath.Join(f.localPath(dirPath), fi)
		if _, err := os.Stat(lp); os.IsNotExist(err) {
			fh, _ := os.OpenFile(lp, os.O_CREATE|os.O_RDWR, 0755)
			_ = fh.Close()
		}
	}

	f.markDirCached(dirPath)

	return fs.NewLoopbackDirStream(f.localPath(dirPath))
}

// Getattr fetches attributes of the directory itself.
func (f *FFBox) Getattr(ctx context.Context, fh fs.FileHandle, fga *fuse.AttrOut) syscall.Errno {
    if strings.HasSuffix(f.Path(nil), metaDir) {
        return syscall.ENOENT
    }

    lp := f.localPath(f.Path(nil))

    // Use syscall.Lstat to get a Stat_t rather than an os.FileInfo
    var st syscall.Stat_t
    err := syscall.Lstat(lp, &st)
    if os.IsNotExist(err) {
        // Attempt to list from S3
        parent := filepath.Dir(f.Path(nil))
        if !f.isDirCached(parent) {
            _, _ = f.Readdir(ctx)
        }
        // Re-try syscall.Lstat
        err = syscall.Lstat(lp, &st)
    }

    if err != nil {
        return fs.ToErrno(err) // convert Go error to FUSE errno
    }
    fga.FromStat(&st) // now we have *syscall.Stat_t
    return fs.OK
}

// -------------------------
//    FFBoxFile (File)
// -------------------------

// Open is called when we open a file.
func (f *FFBoxFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	err := f.ffbox.downloadIfNeeded(f.path)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	lp := f.ffbox.localPath(f.path)

	osFile, err2 := os.OpenFile(lp, int(flags), 0755)
	if err2 != nil {
		return nil, 0, fs.ToErrno(err2)
	}
	return &FileHandle{file: osFile}, fuse.FOPEN_KEEP_CACHE, fs.OK
}

// Read returns file data.
func (f *FFBoxFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	handle, ok := fh.(*FileHandle)
	if !ok {
		return nil, syscall.EIO
	}
	n, err := handle.file.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, fs.ToErrno(err)
	}
	return fuse.ReadResultData(dest[:n]), fs.OK
}

// Getattr for the file.
func (f *FFBoxFile) Getattr(ctx context.Context, fh fs.FileHandle, fga *fuse.AttrOut) syscall.Errno {
    lp := f.ffbox.localPath(f.path)

    // Use syscall.Lstat to get the raw struct
    var st syscall.Stat_t
    err := syscall.Lstat(lp, &st)
    if err != nil {
        if os.IsNotExist(err) {
            // If not found locally, try downloading from S3
            _ = f.ffbox.downloadIfNeeded(f.path)
            err = syscall.Lstat(lp, &st)
        }
        if err != nil {
            return fs.ToErrno(err)
        }
    }

    // Use the raw syscall.Stat_t pointer
    fga.FromStat(&st)
    return fs.OK
}

// -------------------------
//    FileHandle methods
// -------------------------

// Release is called when a file is closed.
func (h *FileHandle) Release(ctx context.Context) syscall.Errno {
	if err := h.file.Close(); err != nil {
		return fs.ToErrno(err)
	}
	return fs.OK
}

// -------------------------
//    Mount logic
// -------------------------

func mountFFBox(s3URL, mountpoint string, cleanCache bool) error {
	// Parse the S3 URL
	s3URL = strings.TrimPrefix(s3URL, "s3://")
	var bucket, prefix string
	if idx := strings.Index(s3URL, "/"); idx != -1 {
		bucket = s3URL[:idx]
		prefix = s3URL[idx+1:]
	} else {
		bucket = s3URL
	}

	if bucket == "" {
		return fmt.Errorf("invalid S3 URL %q: missing bucket", s3URL)
	}

	// Prepare cache path
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	cacheBase := filepath.Join(homeDir, ".cache", "ffbox")
	cachePath := filepath.Join(cacheBase, bucket)
	if prefix != "" {
		cachePath = filepath.Join(cachePath, prefix)
	}

	// Optionally clean the cache
	if cleanCache {
		log.Printf("Cleaning local cache at %s", cachePath)
		os.RemoveAll(cachePath)
	}

	// Ensure directories exist
	err = os.MkdirAll(mountpoint, 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(cachePath, 0755)
	if err != nil {
		return err
	}

	// Load AWS config (v2)
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}
	// If you need to force no credentials, you can do something like:
	//   cfg.Credentials = aws.AnonymousCredentials{}

	svc := s3.NewFromConfig(cfg)

	root := &FFBox{
		LocalCacheRoot: cachePath,
		S3Client:       svc,
		Bucket:         bucket,
		Prefix:         prefix,
	}

	opts := &fs.Options{}
	opts.Debug = false // Set to true for FUSE debug logs

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		return err
	}

	fmt.Printf("Mounted s3://%s/%s at %s\n", bucket, prefix, mountpoint)
	server.Wait()
	return nil
}

// main provides a simple CLI: ffbox mount s3://bucket[/prefix] /mountpoint --clean
func main() {
	mountCmd := flag.NewFlagSet("mount", flag.ExitOnError)
	cleanCache := mountCmd.Bool("clean", false, "Clean the cache directory before mounting")

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> [<args>]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n  mount s3://bucket[/prefix] /path/to/mountpoint [--clean]\n")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "mount":
		_ = mountCmd.Parse(os.Args[2:])
		if mountCmd.NArg() < 2 {
			fmt.Fprintf(os.Stderr, "Usage: %s mount s3://bucket[/prefix] <mountpoint> [--clean]\n", os.Args[0])
			os.Exit(1)
		}
		s3URL := mountCmd.Arg(0)
		mountpoint := mountCmd.Arg(1)
		err := mountFFBox(s3URL, mountpoint, *cleanCache)
		if err != nil {
			log.Fatalf("Mount failed: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
