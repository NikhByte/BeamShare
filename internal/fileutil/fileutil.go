package fileutil

import (
	"path/filepath"
	"strings"
)

// SanitizeReceivedFilename cleans an untrusted input filename, removes path traversal components
// and harmful boundary characters, and returns a safe filename within the working directory.
// If the sanitized filename is empty, it returns fallback (or "upload.bin" if fallback is empty).
func SanitizeReceivedFilename(rawName string, fallback string) string {
	if fallback == "" {
		fallback = "upload.bin"
	}

	// Remove null bytes
	cleaned := strings.ReplaceAll(rawName, "\x00", "")

	// Normalize backslashes to forward slashes for cross-platform base path extraction
	cleaned = strings.ReplaceAll(cleaned, "\\", "/")

	// Strip directory paths and normalize relative path tokens
	cleaned = filepath.Base(filepath.Clean(cleaned))

	// Trim remaining null bytes, dots, slashes, backslashes, and surrounding spaces
	cleaned = strings.Trim(cleaned, "\x00./\\ \t\r\n")

	// Trim whitespace
	cleaned = strings.TrimSpace(cleaned)

	if cleaned == "" {
		return fallback
	}

	return cleaned
}
