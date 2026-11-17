//go:build !unix

package attachment

import (
	"errors"
	"io/fs"
)

// Ownership and free-space checks are POSIX only; other platforms rely on
// the 0700 permission check and the instance byte limit.
func checkOwner(fs.FileInfo) error { return nil }

func filesystemSpace(string) (uint64, uint64, error) {
	return 0, 0, errors.New("free space is not available on this platform")
}
