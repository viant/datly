package gateway

import (
	"fmt"
	"strings"
)

func validateAsyncJobPaths(jobURL, failedJobURL string) error {
	if jobURL == "" {
		return nil
	}
	if unsafe, reason := isUnsafeAsyncURL(jobURL); unsafe {
		return fmt.Errorf("JobURL %q is unsafe: %s", jobURL, reason)
	}
	if failedJobURL != "" {
		if unsafe, reason := isUnsafeAsyncURL(failedJobURL); unsafe {
			return fmt.Errorf("FailedJobURL %q is unsafe: %s", failedJobURL, reason)
		}
	}
	return nil
}

func isUnsafeAsyncURL(raw string) (bool, string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return true, "empty path"
	}
	lower := strings.ToLower(trimmed)
	switch lower {
	case "/", "file://", "file://localhost", "file://localhost/", "mem://", "mem:///":
		return true, "root-like path"
	}
	if isRootLikePath(lower) {
		return true, "root-like path"
	}
	if isBroadTempPath(lower) {
		return true, "broad temp path"
	}
	return false, ""
}

func isRootLikePath(raw string) bool {
	switch raw {
	case "file://localhost", "file://localhost/", "mem://", "mem:///", "mem://localhost", "mem://localhost/":
		return true
	}
	return false
}

func isBroadTempPath(raw string) bool {
	candidates := []string{
		"/tmp",
		"/var/tmp",
		"file://localhost/tmp",
		"file://localhost/var/tmp",
	}
	for _, candidate := range candidates {
		if raw == candidate || raw == candidate+"/" {
			return true
		}
	}
	return false
}
