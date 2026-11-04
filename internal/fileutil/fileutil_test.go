package fileutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeReceivedFilename(t *testing.T) {
	tests := []struct {
		name             string
		rawName          string
		fallback         string
		expectedFilename string
	}{
		{
			name:             "simple filename",
			rawName:          "document.pdf",
			fallback:         "upload.bin",
			expectedFilename: "document.pdf",
		},
		{
			name:             "relative path traversal unix",
			rawName:          "../../evil.sh",
			fallback:         "upload.bin",
			expectedFilename: "evil.sh",
		},
		{
			name:             "relative path traversal windows",
			rawName:          `..\..\evil_win.bat`,
			fallback:         "upload.bin",
			expectedFilename: "evil_win.bat",
		},
		{
			name:             "absolute unix path",
			rawName:          "/absolute/path/test.txt",
			fallback:         "upload.bin",
			expectedFilename: "test.txt",
		},
		{
			name:             "windows absolute path with backslashes",
			rawName:          `C:\Windows\System32\cmd.exe`,
			fallback:         "upload.bin",
			expectedFilename: "cmd.exe",
		},
		{
			name:             "nested directory subpath",
			rawName:          "nested/dir/sub/data.dat",
			fallback:         "upload.bin",
			expectedFilename: "data.dat",
		},
		{
			name:             "etc passwd path traversal",
			rawName:          "../../../etc/passwd",
			fallback:         "upload.bin",
			expectedFilename: "passwd",
		},
		{
			name:             "dots only",
			rawName:          "....",
			fallback:         "upload.bin",
			expectedFilename: "upload.bin",
		},
		{
			name:             "empty string",
			rawName:          "",
			fallback:         "upload.bin",
			expectedFilename: "upload.bin",
		},
		{
			name:             "empty string with custom fallback",
			rawName:          "",
			fallback:         "download.bin",
			expectedFilename: "download.bin",
		},
		{
			name:             "empty string with empty fallback",
			rawName:          "",
			fallback:         "",
			expectedFilename: "upload.bin",
		},
		{
			name:             "leading null byte",
			rawName:          "\x00filename.txt",
			fallback:         "upload.bin",
			expectedFilename: "filename.txt",
		},
		{
			name:             "embedded null byte",
			rawName:          "file\x00name.txt",
			fallback:         "upload.bin",
			expectedFilename: "filename.txt",
		},
		{
			name:             "trailing null byte and slashes",
			rawName:          "../../../\x00",
			fallback:         "upload.bin",
			expectedFilename: "upload.bin",
		},
		{
			name:             "whitespace only",
			rawName:          "   ",
			fallback:         "upload.bin",
			expectedFilename: "upload.bin",
		},
		{
			name:             "padded spaces around filename",
			rawName:          "  hello.txt  ",
			fallback:         "upload.bin",
			expectedFilename: "hello.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeReceivedFilename(tt.rawName, tt.fallback)
			assert.Equal(t, tt.expectedFilename, result)
		})
	}
}
