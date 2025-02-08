package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// PassthroughNode is our "loopback" node that forwards operations to the real filesystem.
type PassthroughNode struct {
	fs.Inode
	rootPath string
}

// This will be called when the filesystem looks up an entry.
// We return a child node that references the underlying path on the real filesystem.
func (n *PassthroughNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	realPath := filepath.Join(n.rootPath, name)
	st, err := os.Lstat(realPath)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	// Create a new child node.
	child := &PassthroughNode{
		rootPath: realPath,
	}

	// Embed child node within the parent.
	return n.NewInode(
		ctx,
		child,
		fs.StableAttr{
			Mode: uint32(st.Mode()),
			Ino:  uint64(st.Sys().(*syscall.Stat_t).Ino),
		},
	), 0
}

// Readdir is used to list directory contents.
func (n *PassthroughNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dir, err := os.Open(n.rootPath)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	ds := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		st := e.Sys().(*syscall.Stat_t)
		ds = append(ds, fuse.DirEntry{
			Mode: uint32(e.Mode()),
			Ino:  st.Ino,
			Name: e.Name(),
		})
	}
	return fs.NewListDirStream(ds), 0
}

// Getattr is used to retrieve file/directory attributes.
func (n *PassthroughNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fi, err := os.Lstat(n.rootPath)
	if err != nil {
		return fs.ToErrno(err)
	}
	st := fi.Sys().(*syscall.Stat_t)
	out.FromStat(st)
	return 0
}

// Open opens the file for read/write. We'll use the built-in FileHandle.
func (n *PassthroughNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	f, err := os.OpenFile(n.rootPath, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	return f, fuse.FOPEN_KEEP_CACHE, 0
}

// Mkdir creates a new directory.
func (n *PassthroughNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	newPath := filepath.Join(n.rootPath, name)
	err := os.Mkdir(newPath, os.FileMode(mode))
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	st, err := os.Lstat(newPath)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	child := &PassthroughNode{
		rootPath: newPath,
	}
	return n.NewInode(
		ctx,
		child,
		fs.StableAttr{
			Mode: uint32(st.Mode()),
			Ino:  uint64(st.Sys().(*syscall.Stat_t).Ino),
		},
	), 0
}

// Unlink removes a file.
func (n *PassthroughNode) Unlink(ctx context.Context, name string) syscall.Errno {
	fullPath := filepath.Join(n.rootPath, name)
	if err := os.Remove(fullPath); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

// Rmdir removes a directory.
func (n *PassthroughNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	fullPath := filepath.Join(n.rootPath, name)
	if err := os.Remove(fullPath); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

// Rename renames (moves) a file or directory.
func (n *PassthroughNode) Rename(ctx context.Context, oldName string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := filepath.Join(n.rootPath, oldName)
	newParentNode := newParent.(*PassthroughNode)
	newPath := filepath.Join(newParentNode.rootPath, newName)
	if err := os.Rename(oldPath, newPath); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <targetDir> <mountPoint>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}

	targetDir := flag.Arg(0)
	mountPoint := flag.Arg(1)

	// Construct a new file system root node.
	root := &PassthroughNode{
		rootPath: targetDir,
	}

	// Create a go-fuse filesystem by mounting the root node.
	opts := &fs.Options{}
	opts.Debug = false // Set to true to enable debugging logs.

	server, err := fs.Mount(mountPoint, root, opts)
	if err != nil {
		log.Fatalf("Mount failed: %v\n", err)
		return
	}

	fmt.Printf("Passthrough FS mounted on %s, forwarding to %s\n", mountPoint, targetDir)

	// Run the FUSE server in the foreground until unmounted.
	server.Wait()
}