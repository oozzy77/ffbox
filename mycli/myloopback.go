// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	// "github.com/hanwen/go-fuse/v2/internal/renameat"
	"golang.org/x/sys/unix"
)

// FfboxNodeRoot holds the parameters for creating a new loopback
// filesystem. Loopback filesystem delegate their operations to an
// underlying POSIX file system.
var s3Client *s3.Client
var bucketName string
var rootPath string

type FfboxNodeRoot struct {
	// The path to the root of the underlying file system.
	Path string

	// The device on which the Path resides. This must be set if
	// the underlying filesystem crosses file systems.
	Dev uint64

	// NewNode returns a new InodeEmbedder to be used to respond
	// to a LOOKUP/CREATE/MKDIR/MKNOD opcode. If not set, use a
	// FfboxNode.
	NewNode func(rootData *FfboxNodeRoot, parent *Inode, name string, st *syscall.Stat_t) InodeEmbedder

	// RootNode is the root of the Loopback. This must be set if
	// the Loopback file system is not the root of the FUSE
	// mount. It is set automatically by NewFfboxNodeRoot.
	RootNode InodeEmbedder
}

func (r *FfboxNodeRoot) newNode(parent *Inode, name string, st *syscall.Stat_t) InodeEmbedder {
	if r.NewNode != nil {
		return r.NewNode(r, parent, name, st)
	}
	return &FfboxNode{
		RootData: r,
	}
}

func (r *FfboxNodeRoot) idFromStat(st *syscall.Stat_t) StableAttr {
	// We compose an inode number by the underlying inode, and
	// mixing in the device number. In traditional filesystems,
	// the inode numbers are small. The device numbers are also
	// small (typically 16 bit). Finally, we mask out the root
	// device number of the root, so a loopback FS that does not
	// encompass multiple mounts will reflect the inode numbers of
	// the underlying filesystem
	swapped := (uint64(st.Dev) << 32) | (uint64(st.Dev) >> 32)
	swappedRootDev := (r.Dev << 32) | (r.Dev >> 32)
	return StableAttr{
		Mode: uint32(st.Mode),
		Gen:  1,
		// This should work well for traditional backing FSes,
		// not so much for other go-fuse FS-es
		Ino: (swapped ^ swappedRootDev) ^ st.Ino,
	}
}

// FfboxNode is a filesystem node in a loopback file system. It is
// public so it can be used as a basis for other loopback based
// filesystems. See NewLoopbackFile or FfboxNodeRoot for more
// information.
type FfboxNode struct {
	Inode

	// RootData points back to the root of the loopback filesystem.
	RootData *FfboxNodeRoot
	isComplete bool
}

// loopbackNodeEmbedder can only be implemented by the FfboxNode
// concrete type.
type loopbackNodeEmbedder interface {
	loopbackNode() *FfboxNode
}

func (n *FfboxNode) loopbackNode() *FfboxNode {
	return n
}

var _ = (NodeStatfser)((*FfboxNode)(nil))

func (n *FfboxNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(n.path(), &s)
	if err != nil {
		return ToErrno(err)
	}
	out.FromStatfsT(&s)
	return OK
}

// path returns the full path to the file in the underlying file
// system.
func (n *FfboxNode) root() *Inode {
	var rootNode *Inode
	if n.RootData.RootNode != nil {
		rootNode = n.RootData.RootNode.EmbeddedInode()
	} else {
		rootNode = n.Root()
	}

	return rootNode
}

func (n *FfboxNode) path() string {
	path := n.Path(n.root())
	return filepath.Join(n.RootData.Path, path)
}

var _ = (NodeLookuper)((*FfboxNode)(nil))

func cloudFolderKey(path string) string {
	path = strings.TrimLeft(path, "/")
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	return path
}

// isFolderCached checks whether the folder is already marked as cached.
func (n *FfboxNode) isFolderCached(path string) bool {
	if n.isComplete {
		return true
	}
	// Allocate a small buffer. "1" is only one byte so a 2 byte buffer is plenty.
	buf := make([]byte, 2)
	nread, err := unix.Lgetxattr(path, "user.is_complete", buf)
	if err != nil {
		// The attribute is probably not set.
		return false
	}

	// Compare the value. You can compare as a string...
	return string(buf[:nread]) == "1"
}

// markFolderCached marks a folder as cached.
func (n *FfboxNode) markFolderCached(path string) {
	n.isComplete = true
	err := unix.Lsetxattr(path, "user.is_complete", []byte("1"), 0)
	if err != nil {
		fmt.Printf("🔴Error setting xattr: %v\n", err)
	}
}

// Updated cloudLookup implements cloud_getattr‑like behavior using AWS SDK v2.
// It is meant to be called (for example from a Lookup handler) when a file or folder
// under a “cloud” path is requested.
func cloudLookup(ctx context.Context, n *FfboxNode, name string, out *fuse.EntryOut) bool {
	parentPath := filepath.Dir(name)
	fmt.Printf("Checking parent: %s\n", parentPath)
	if n.isFolderCached(parentPath) {
		return false
	}
	fmt.Printf("🟠 Cloud getting attributes of %s, parent: %s\n", name, parentPath)

	// Build the S3 prefix using our helper.
	prefix := cloudFolderKey(parentPath)
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"), // This limits the listing to the folder level.
	}

	// Call S3 to list objects.
	resp, err := s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		fmt.Printf("Error listing S3 objects: %v\n", err)
		return false
	}

	// if resp.IsTruncated {
	// 	fmt.Printf("🔴Warning: Directory listing for %s is truncated!\n", name)
	// }

	// Compute the local path for the parent folder.
	localParent := strings.TrimLeft(parentPath, "/")

	// Process common prefixes to create missing local subdirectories.
	for _, cp := range resp.CommonPrefixes {
		if cp.Prefix == nil {
			continue
		}
		// Remove the trailing slash and get the last part as directory name.
		cpStr := strings.TrimRight(*cp.Prefix, "/")
		parts := strings.Split(cpStr, "/")
		dirName := parts[len(parts)-1]
		localDirPath := filepath.Join(rootPath, localParent, dirName)
		if err := os.MkdirAll(localDirPath, 0755); err != nil {
			fmt.Printf("Error creating directory %s: %v\n", localDirPath, err)
		}
	}

	// Process files (S3 objects) to create sparse placeholder files if they do not exist.
	for _, obj := range resp.Contents {
		if obj.Key == nil {
			continue
		}
		key := *obj.Key
		parts := strings.Split(key, "/")
		fileName := parts[len(parts)-1]
		localFilePath := filepath.Join(rootPath, localParent, fileName)
		if _, err := os.Stat(localFilePath); os.IsNotExist(err) {
			// Create the file (it will be sparse if we simply truncate to the given size).
			f, err := os.OpenFile(localFilePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				fmt.Printf("Error creating file %s: %v\n", localFilePath, err)
				continue
			}
			// Truncate the file to the size of the S3 object.
			if err := f.Truncate(*obj.Size); err != nil {
				fmt.Printf("Error truncating file %s: %v\n", localFilePath, err)
			}
			f.Close()

			// If available, use LastModified to update the file times.
			if obj.LastModified != nil {
				modTime := *obj.LastModified
				if err := os.Chtimes(localFilePath, modTime, modTime); err != nil {
					fmt.Printf("Error setting times on file %s: %v\n", localFilePath, err)
				}
			}
		}
	}

	// Mark this folder as cached so we don’t repeat S3 lookups.
	n.markFolderCached(parentPath)

	return true
}

func (n *FfboxNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	fmt.Println("Lookup22222", p)
	st := syscall.Stat_t{}
	err := syscall.Lstat(p, &st)
	if err != nil {
		if cloudLookup(ctx, n, name, out) {
			err = syscall.Lstat(p, &st)
			out.Attr.FromStat(&st)
			node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
			ch := n.NewInode(ctx, node, n.RootData.idFromStat(&st))
			return ch, 0
		}
		return nil, ToErrno(err)
	}

	out.Attr.FromStat(&st)
	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, n.RootData.idFromStat(&st))
	return ch, 0
}

// preserveOwner sets uid and gid of `path` according to the caller information
// in `ctx`.
func (n *FfboxNode) preserveOwner(ctx context.Context, path string) error {
	if os.Getuid() != 0 {
		return nil
	}
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return nil
	}
	return syscall.Lchown(path, int(caller.Uid), int(caller.Gid))
}

var _ = (NodeMknoder)((*FfboxNode)(nil))

func intDev(dev uint32) int {
	return int(dev)
}

func (n *FfboxNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := syscall.Mknod(p, mode, intDev(rdev))
	if err != nil {
		return nil, ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Rmdir(p)
		return nil, ToErrno(err)
	}

	out.Attr.FromStat(&st)

	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, n.RootData.idFromStat(&st))

	return ch, 0
}

var _ = (NodeMkdirer)((*FfboxNode)(nil))

func (n *FfboxNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := os.Mkdir(p, os.FileMode(mode))
	if err != nil {
		return nil, ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Rmdir(p)
		return nil, ToErrno(err)
	}

	out.Attr.FromStat(&st)

	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, n.RootData.idFromStat(&st))

	return ch, 0
}

var _ = (NodeRmdirer)((*FfboxNode)(nil))

func (n *FfboxNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	p := filepath.Join(n.path(), name)
	err := syscall.Rmdir(p)
	return ToErrno(err)
}

var _ = (NodeUnlinker)((*FfboxNode)(nil))

func (n *FfboxNode) Unlink(ctx context.Context, name string) syscall.Errno {
	p := filepath.Join(n.path(), name)
	err := syscall.Unlink(p)
	return ToErrno(err)
}

var _ = (NodeRenamer)((*FfboxNode)(nil))

func (n *FfboxNode) Rename(ctx context.Context, name string, newParent InodeEmbedder, newName string, flags uint32) syscall.Errno {
	e2, ok := newParent.(loopbackNodeEmbedder)
	if !ok {
		return syscall.EXDEV
	}

	if e2.loopbackNode().RootData != n.RootData {
		return syscall.EXDEV
	}

	if flags&RENAME_EXCHANGE != 0 {
		return n.renameExchange(name, e2.loopbackNode(), newName)
	}

	p1 := filepath.Join(n.path(), name)
	p2 := filepath.Join(e2.loopbackNode().path(), newName)

	err := syscall.Rename(p1, p2)
	return ToErrno(err)
}

var _ = (NodeCreater)((*FfboxNode)(nil))

func (n *FfboxNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *Inode, fh FileHandle, fuseFlags uint32, errno syscall.Errno) {
	p := filepath.Join(n.path(), name)
	flags = flags &^ syscall.O_APPEND
	fd, err := syscall.Open(p, int(flags)|os.O_CREATE, mode)
	if err != nil {
		return nil, nil, 0, ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		return nil, nil, 0, ToErrno(err)
	}

	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, n.RootData.idFromStat(&st))
	lf := NewLoopbackFile(fd)

	out.FromStat(&st)
	return ch, lf, 0, 0
}

func renameat(olddirfd int, oldpath string, newdirfd int, newpath string, flags uint) (err error) {
	return unix.Renameat2(olddirfd, oldpath, newdirfd, newpath, flags)
}


func (n *FfboxNode) renameExchange(name string, newParent *FfboxNode, newName string) syscall.Errno {
	fd1, err := syscall.Open(n.path(), syscall.O_DIRECTORY, 0)
	if err != nil {
		return ToErrno(err)
	}
	defer syscall.Close(fd1)
	p2 := newParent.path()
	fd2, err := syscall.Open(p2, syscall.O_DIRECTORY, 0)
	defer syscall.Close(fd2)
	if err != nil {
		return ToErrno(err)
	}

	var st syscall.Stat_t
	if err := syscall.Fstat(fd1, &st); err != nil {
		return ToErrno(err)
	}

	// Double check that nodes didn't change from under us.
	if n.root() != n.EmbeddedInode() && n.Inode.StableAttr().Ino != n.RootData.idFromStat(&st).Ino {
		return syscall.EBUSY
	}
	if err := syscall.Fstat(fd2, &st); err != nil {
		return ToErrno(err)
	}

	if (newParent.root() != newParent.EmbeddedInode()) && newParent.Inode.StableAttr().Ino != n.RootData.idFromStat(&st).Ino {
		return syscall.EBUSY
	}

	return ToErrno(renameat(fd1, name, fd2, newName, unix.RENAME_EXCHANGE))
}

var _ = (NodeSymlinker)((*FfboxNode)(nil))

func (n *FfboxNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	p := filepath.Join(n.path(), name)
	err := syscall.Symlink(target, p)
	if err != nil {
		return nil, ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Unlink(p)
		return nil, ToErrno(err)
	}
	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, n.RootData.idFromStat(&st))

	out.Attr.FromStat(&st)
	return ch, 0
}

var _ = (NodeLinker)((*FfboxNode)(nil))

func (n *FfboxNode) Link(ctx context.Context, target InodeEmbedder, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {

	p := filepath.Join(n.path(), name)
	err := syscall.Link(filepath.Join(n.RootData.Path, target.EmbeddedInode().Path(nil)), p)
	if err != nil {
		return nil, ToErrno(err)
	}
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Unlink(p)
		return nil, ToErrno(err)
	}
	node := n.RootData.newNode(n.EmbeddedInode(), name, &st)
	ch := n.NewInode(ctx, node, n.RootData.idFromStat(&st))

	out.Attr.FromStat(&st)
	return ch, 0
}

var _ = (NodeReadlinker)((*FfboxNode)(nil))

func (n *FfboxNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	p := n.path()

	for l := 256; ; l *= 2 {
		buf := make([]byte, l)
		sz, err := syscall.Readlink(p, buf)
		if err != nil {
			return nil, ToErrno(err)
		}

		if sz < len(buf) {
			return buf[:sz], 0
		}
	}
}

var _ = (NodeOpener)((*FfboxNode)(nil))

func (n *FfboxNode) Open(ctx context.Context, flags uint32) (fh FileHandle, fuseFlags uint32, errno syscall.Errno) {
	flags = flags &^ syscall.O_APPEND
	p := n.path()
	fmt.Println("Open999999", p)
	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, ToErrno(err)
	}
	lf := NewLoopbackFile(f)
	return lf, 0, 0
}

var _ = (NodeOpendirHandler)((*FfboxNode)(nil))

func (n *FfboxNode) OpendirHandle(ctx context.Context, flags uint32) (FileHandle, uint32, syscall.Errno) {
	ds, errno := NewLoopbackDirStream(n.path())
	if errno != 0 {
		return nil, 0, errno
	}
	return ds, 0, errno
}

var _ = (NodeReaddirer)((*FfboxNode)(nil))

func (n *FfboxNode) Readdir(ctx context.Context) (DirStream, syscall.Errno) {
	fmt.Println("Readdir44444", n.path())
	return NewLoopbackDirStream(n.path())
}

var _ = (NodeGetattrer)((*FfboxNode)(nil))

func (n *FfboxNode) Getattr(ctx context.Context, f FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f != nil {
		if fga, ok := f.(FileGetattrer); ok {
			return fga.Getattr(ctx, out)
		}
	}

	p := n.path()
	fmt.Println("Getattr33333", p)

	var err error
	st := syscall.Stat_t{}
	if &n.Inode == n.Root() {
		err = syscall.Stat(p, &st)
	} else {
		err = syscall.Lstat(p, &st)
	}

	if err != nil {
		return ToErrno(err)
	}
	out.FromStat(&st)
	return OK
}

var _ = (NodeSetattrer)((*FfboxNode)(nil))

func (n *FfboxNode) Setattr(ctx context.Context, f FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	p := n.path()
	fsa, ok := f.(FileSetattrer)
	if ok && fsa != nil {
		fsa.Setattr(ctx, in, out)
	} else {
		if m, ok := in.GetMode(); ok {
			if err := syscall.Chmod(p, m); err != nil {
				return ToErrno(err)
			}
		}

		uid, uok := in.GetUID()
		gid, gok := in.GetGID()
		if uok || gok {
			suid := -1
			sgid := -1
			if uok {
				suid = int(uid)
			}
			if gok {
				sgid = int(gid)
			}
			if err := syscall.Chown(p, suid, sgid); err != nil {
				return ToErrno(err)
			}
		}

		mtime, mok := in.GetMTime()
		atime, aok := in.GetATime()

		if mok || aok {
			ta := unix.Timespec{Nsec: unix.UTIME_OMIT}
			tm := unix.Timespec{Nsec: unix.UTIME_OMIT}
			var err error
			if aok {
				ta, err = unix.TimeToTimespec(atime)
				if err != nil {
					return ToErrno(err)
				}
			}
			if mok {
				tm, err = unix.TimeToTimespec(mtime)
				if err != nil {
					return ToErrno(err)
				}
			}
			ts := []unix.Timespec{ta, tm}
			if err := unix.UtimesNanoAt(unix.AT_FDCWD, p, ts, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				return ToErrno(err)
			}
		}

		if sz, ok := in.GetSize(); ok {
			if err := syscall.Truncate(p, int64(sz)); err != nil {
				return ToErrno(err)
			}
		}
	}

	fga, ok := f.(FileGetattrer)
	if ok && fga != nil {
		fga.Getattr(ctx, out)
	} else {
		st := syscall.Stat_t{}
		err := syscall.Lstat(p, &st)
		if err != nil {
			return ToErrno(err)
		}
		out.FromStat(&st)
	}
	return OK
}

var _ = (NodeGetxattrer)((*FfboxNode)(nil))

func (n *FfboxNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	sz, err := unix.Lgetxattr(n.path(), attr, dest)
	return uint32(sz), ToErrno(err)
}

var _ = (NodeSetxattrer)((*FfboxNode)(nil))

func (n *FfboxNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	err := unix.Lsetxattr(n.path(), attr, data, int(flags))
	return ToErrno(err)
}

var _ = (NodeRemovexattrer)((*FfboxNode)(nil))

func (n *FfboxNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	err := unix.Lremovexattr(n.path(), attr)
	return ToErrno(err)
}

// var _ = (NodeCopyFileRanger)((*FfboxNode)(nil))

// func (n *FfboxNode) CopyFileRange(ctx context.Context, fhIn FileHandle,
// 	offIn uint64, out *Inode, fhOut FileHandle, offOut uint64,
// 	len uint64, flags uint64) (uint32, syscall.Errno) {
// 	lfIn, ok := fhIn.(*loopbackFile)
// 	if !ok {
// 		return 0, unix.ENOTSUP
// 	}
// 	lfOut, ok := fhOut.(*loopbackFile)
// 	if !ok {
// 		return 0, unix.ENOTSUP
// 	}
// 	signedOffIn := int64(offIn)
// 	signedOffOut := int64(offOut)
// 	doCopyFileRange(lfIn.fd, signedOffIn, lfOut.fd, signedOffOut, int(len), int(flags))
// 	return 0, syscall.ENOSYS
// }

// NewFfboxNodeRoot returns a root node for a loopback file system whose
// root is at the given root. This node implements all NodeXxxxer
// operations available.
func NewFfboxNodeRoot(cachePath string, s3Bucket string) (InodeEmbedder, error) {
	var st syscall.Stat_t
	rootPath = cachePath
	err := syscall.Stat(cachePath, &st)
	if err != nil {
		return nil, err
	}

	root := &FfboxNodeRoot{
		Path: cachePath,
		Dev:  uint64(st.Dev),
	}

	// Create a new AWS session.
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1")) // Change to your region
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	// Create an S3 client
	s3Client = s3.NewFromConfig(cfg)
	bucketName = s3Bucket

	rootNode := root.newNode(nil, "", &st)
	root.RootNode = rootNode
	return rootNode, nil
}
