//go:build windows

package transcribe

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func tryLockProjectMetadataFile(file *os.File) (unlock func() error, blocked bool, err error) {
	handle := windows.Handle(file.Fd())
	overlapped := &windows.Overlapped{}
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
	if err := windows.LockFileEx(handle, flags, 0, 1, 0, overlapped); err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, true, nil
		}
		return nil, false, err
	}
	return func() error { return windows.UnlockFileEx(handle, 0, 1, 0, overlapped) }, false, nil
}
