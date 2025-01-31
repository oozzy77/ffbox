package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const metaDir = ".ffbox_noot"

type S3FS struct {
	fs.Inode
	bucket string
	prefix string
	client *s3.Client
	root   string
}

// Ensure S3FS implements the go-fuse Node interface
var _ = (fs.NodeLookuper)((*S3Node)(nil))
var _ = (fs.NodeOpener)((*S3Node)(nil))
var _ = (fs.NodeReaddirer)((*S3Node)(nil))

// S3Node represents a file or directory in the FUSE filesystem
type S3Node struct {
	fs.Inode
	fs     *S3FS
	path   string
	isDir  bool
	size   int64
	modify time.Time
}

func (n *S3Node) GetAttr(ctx context.Context, a *fuse.Attr) syscall.Errno {
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())

	if n.isDir {
		a.Mode = syscall.S_IFDIR | 0755
	} else {
		a.Mode = 0644
		a.Size = uint64(n.size)
		a.Mtime = uint64(n.modify.Unix())
	}
	return 0
}

func (n *S3Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fullPath := filepath.Join(n.path, name)

	// Check if directory exists in S3
	s3Key := n.fs.prefix + "/" + fullPath
	log.Println("Checking S3 key:", s3Key)

	outData, err := n.fs.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(n.fs.bucket),
		Prefix: aws.String(s3Key),
	})
	if err != nil {
		return nil, syscall.ENOENT
	}

	// Determine if it's a file or folder
	for _, obj := range outData.Contents {
		if *obj.Key == s3Key {
			child := &S3Node{
				fs:     n.fs,
				path:   fullPath,
				isDir:  false,
				size:   *obj.Size,
				modify: *obj.LastModified,
			}
			childInode := n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG})
			return childInode, 0
		}
	}

	// Check if it's a folder
	for _, prefix := range outData.CommonPrefixes {
		if *prefix.Prefix == s3Key+"/" {
			child := &S3Node{
				fs:    n.fs,
				path:  fullPath,
				isDir: true,
			}
			childInode := n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR})
			return childInode, 0
		}
	}

	return nil, syscall.ENOENT
}

func (n *S3Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	s3Key := n.fs.prefix + "/" + n.path

	out, err := n.fs.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(n.fs.bucket),
		Prefix:    aws.String(s3Key),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return nil, syscall.EIO
	}

	var entries []fuse.DirEntry
	for _, obj := range out.Contents {
		name := filepath.Base(*obj.Key)
		entries = append(entries, fuse.DirEntry{Name: name, Mode: syscall.S_IFREG})
	}
	for _, prefix := range out.CommonPrefixes {
		name := filepath.Base(*prefix.Prefix)
		entries = append(entries, fuse.DirEntry{Name: name, Mode: syscall.S_IFDIR})
	}

	return fs.NewListDirStream(entries), 0
}

func (n *S3Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	localPath := filepath.Join(n.fs.root, n.path)
	s3Key := n.fs.prefix + "/" + n.path
	log.Println("Downloading:", s3Key, "to", localPath)

	// Ensure local directory exists
	err := os.MkdirAll(filepath.Dir(localPath), 0755)
	if err != nil {
		return nil, 0, syscall.EIO
	}

	// Download from S3
	out, err := n.fs.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(n.fs.bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return nil, 0, syscall.EIO
	}
	defer out.Body.Close()

	// Save to file
	file, err := os.Create(localPath)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	defer file.Close()

	_, err = file.ReadFrom(out.Body)
	if err != nil {
		return nil, 0, syscall.EIO
	}

	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func mountS3(s3URL, mountpoint string) {
	// Parse S3 URL
	bucket, prefix := parseS3URL(s3URL)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("Failed to load AWS config:", err)
	}

	client := s3.NewFromConfig(cfg)

	// Local cache directory
	user, err := user.Current()
	if err != nil {
		log.Fatal("Failed to get current user:", err)
	}
	cacheDir := filepath.Join(user.HomeDir, ".cache", "ffbox")

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug: true,
		},
	}

	root := &S3FS{
		bucket: bucket,
		prefix: prefix,
		client: client,
		root:   cacheDir,
	}

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		log.Fatal(err)
	}
	server.Wait()
}

func parseS3URL(s3URL string) (string, string) {
	parsedURL, err := url.Parse(s3URL)
	if err != nil {
		log.Fatal("Invalid S3 URL:", err)
	}

	bucket := parsedURL.Host
	prefix := parsedURL.Path
	if len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}

	return bucket, prefix
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: ./ffbox_mount mount s3://your-bucket /mnt/mountpoint")
		os.Exit(1)
	}

	command := os.Args[1]
	if command == "mount" {
		s3URL := os.Args[2]
		mountpoint := os.Args[3]
		mountS3(s3URL, mountpoint)
	} else {
		fmt.Println("Unknown command")
		os.Exit(1)
	}
}
