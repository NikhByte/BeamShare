package server

import (
	"path/filepath"
	"strings"
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

	// Remove null bytes
	clean := strings.ReplaceAll(name, "\x00", "")

	// Replace backslashes with forward slashes so path operations work consistently across OSes
	clean = strings.ReplaceAll(clean, "\\", "/")

	// Extract base name after cleaning path
	clean = filepath.Base(filepath.Clean(clean))

	// Trim remaining null bytes, dots, slashes, backslashes, and surrounding spaces
	clean = strings.Trim(clean, "\x00./\\ \t\r\n")

	if clean == "" {
		return defaultFallback
	}

	return clean
}
