// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)


type FfboxRoot struct {
	fs.LoopbackRoot
}

func (r *FfboxRoot) newNode(parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
	if r.NewNode != nil {
		return r.NewNode(&r.LoopbackRoot, parent, name, st)
	}
	return &FfboxNode{
		LoopbackNode: fs.LoopbackNode{
			RootData: &r.LoopbackRoot,
		},
	}
}

type FfboxNode struct {
	fs.LoopbackNode

	// extra fields
}

// path returns the full path to the file in the underlying file
// system.
func (n *FfboxNode) root() *fs.Inode {
	var rootNode *fs.Inode
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

func (n *FfboxNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fmt.Println("Lookup", name, "path", n.path())
	return n.LoopbackNode.Lookup(ctx, name, out)
}

func (n *FfboxNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fmt.Println("Getattr", n.path())
	return n.LoopbackNode.Getattr(ctx, f, out)
}

func (n *FfboxNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	fmt.Println("Readdir", n.path())
	return n.LoopbackNode.Readdir(ctx)
}

func (n *FfboxNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fmt.Println("Open", n.path())
	return n.LoopbackNode.Open(ctx, flags)
}

func NewFfboxRoot(rootPath string) (fs.InodeEmbedder, error) {
	var st syscall.Stat_t
	err := syscall.Stat(rootPath, &st)
	if err != nil {
		return nil, err
	}

	root := &FfboxRoot{
		LoopbackRoot: fs.LoopbackRoot{
			Path: rootPath,
			Dev:  uint64(st.Dev),
		},
	}

	rootNode := root.newNode(nil, "", &st)
	root.RootNode = rootNode
	return rootNode, nil
}
