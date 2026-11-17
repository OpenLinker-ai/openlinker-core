//go:build unix

package attachment

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

func checkOwner(info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("attachment directory ownership cannot be verified")
	}
	if int(stat.Uid) != os.Geteuid() {
		return errors.New("attachment directory must be owned by the Core process user")
	}
	return nil
}

func filesystemSpace(dir string) (uint64, uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, 0, err
	}
	block := uint64(stat.Bsize)
	return uint64(stat.Bavail) * block, uint64(stat.Blocks) * block, nil
}
