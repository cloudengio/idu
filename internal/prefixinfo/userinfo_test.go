// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/file"
	"cloudeng.io/file/filewalk/localfs"
)

func TestXAttr(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	lfs := localfs.New()
	info, err := lfs.Stat(ctx, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	xattr, err := lfs.XAttr(ctx, tmpDir, info)
	if err != nil {
		t.Fatal(err)
	}
	info.SetSys(xattr)
	pi := New(tmpDir, info)

	tmpFileName := filepath.Join(tmpDir, "a")

	if err := os.WriteFile(tmpFileName, []byte{0x00}, 0600); err != nil {
		t.Fatal(err)
	}

	info, err = lfs.Stat(ctx, tmpFileName)
	if err != nil {
		t.Fatal(err)
	}

	fi := file.NewInfoFromFileInfo(info)
	xattr, err = lfs.XAttr(ctx, tmpFileName, fi)
	if err != nil {
		t.Fatal(err)
	}
	fi.SetSys(xattr)
	pi.AppendInfo(fi)

	uid, gid := pi.UserGroup()
	ouid, ogid := os.Getuid(), os.Getgid()
	if ouid == -1 {
		// Windows returns uid and gid as -1, so this is really
		// a pointless test on windows.
		ouid, ogid = int(uid), int(gid)
	}

	if got, want := int(uid), ouid; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := int(gid), ogid; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	xattr = pi.XAttrInfo(fi)
	if got, want := int(xattr.UID), ouid; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := int(xattr.GID), ogid; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	fi.SetSys(NewSysInfo(600, 6, 33, 44, 1))

	xattr = pi.XAttrInfo(fi)
	if got, want := xattr.UID, int64(600); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := xattr.GID, int64(6); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

}
