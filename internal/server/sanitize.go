package server

import (
	"github.com/beamshare/beam/internal/fileutil"
)

// SanitizeFilename cleans incoming filenames by removing path traversal components,
// relative directory references, null characters, and leading path separators.
// If the cleaned filename evaluates to empty, it returns fallback (or "upload.bin"
// if fallback is omitted or empty).
func SanitizeFilename(name string, fallback ...string) string {
	defaultFallback := "upload.bin"
	if len(fallback) > 0 && fallback[0] != "" {
		defaultFallback = fallback[0]
	}

	return fileutil.SanitizeReceivedFilename(name, defaultFallback)
}
