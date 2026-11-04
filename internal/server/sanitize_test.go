package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		name             string
		inputFilename    string
		fallback         string
		expectedFilename string
	}{
		{
			name:             "Standard filename",
			inputFilename:    "document.pdf",
			fallback:         "upload.bin",
			expectedFilename: "document.pdf",
		},
		{
			name:             "Filename with spaces and special chars",
			inputFilename:    "Project Report (2026).pdf",
			fallback:         "upload.bin",
			expectedFilename: "Project Report (2026).pdf",
		},
		{
			name:             "Relative path traversal with unix separators",
			inputFilename:    "../../etc/passwd",
			fallback:         "upload.bin",
			expectedFilename: "passwd",
		},
		{
			name:             "Relative path traversal with windows separators",
			inputFilename:    "..\\..\\evil_win.bat",
			fallback:         "upload.bin",
			expectedFilename: "evil_win.bat",
		},
		{
			name:             "Absolute unix path",
			inputFilename:    "/absolute/path/test.txt",
			fallback:         "upload.bin",
			expectedFilename: "test.txt",
		},
		{
			name:             "Absolute windows path",
			inputFilename:    "C:\\Users\\Admin\\Desktop\\secret.key",
			fallback:         "upload.bin",
			expectedFilename: "secret.key",
		},
		{
			name:             "Nested directory path",
			inputFilename:    "nested/dir/sub/data.dat",
			fallback:         "upload.bin",
			expectedFilename: "data.dat",
		},
		{
			name:             "Null byte prefix with path traversal",
			inputFilename:    "\x00../file",
			fallback:         "upload.bin",
			expectedFilename: "file",
		},
		{
			name:             "Null byte windows path traversal",
			inputFilename:    "\x00..\\..\\malicious.exe",
			fallback:         "upload.bin",
			expectedFilename: "malicious.exe",
		},
		{
			name:             "Multiple null bytes",
			inputFilename:    "a\x00b\x00c.txt",
			fallback:         "upload.bin",
			expectedFilename: "abc.txt",
		},
		{
			name:             "Dots only input",
			inputFilename:    "....",
			fallback:         "upload.bin",
			expectedFilename: "upload.bin",
		},
		{
			name:             "Single dot input",
			inputFilename:    ".",
			fallback:         "upload.bin",
			expectedFilename: "upload.bin",
		},
		{
			name:             "Double dot input",
			inputFilename:    "..",
			fallback:         "upload.bin",
			expectedFilename: "upload.bin",
		},
		{
			name:             "Empty string input",
			inputFilename:    "",
			fallback:         "upload.bin",
			expectedFilename: "upload.bin",
		},
		{
			name:             "Empty string with custom fallback",
			inputFilename:    "",
			fallback:         "download.bin",
			expectedFilename: "download.bin",
		},
		{
			name:             "Null byte only input",
			inputFilename:    "\x00\x00\x00",
			fallback:         "download.bin",
			expectedFilename: "download.bin",
		},
		{
			name:             "Leading slashes and dots",
			inputFilename:    "./.././something.tar.gz",
			fallback:         "upload.bin",
			expectedFilename: "something.tar.gz",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := SanitizeFilename(tc.inputFilename, tc.fallback)
			assert.Equal(t, tc.expectedFilename, actual)
		})
	}
}
