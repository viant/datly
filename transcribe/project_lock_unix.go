//go:build !windows

package transcribe

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func tryLockProjectMetadataFile(file *os.File) (unlock func() error, blocked bool, err error) {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, true, nil
		}
		return nil, false, err
	}
	return func() error { return unix.Flock(int(file.Fd()), unix.LOCK_UN) }, false, nil
}
