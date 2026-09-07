package server

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyReader struct {
	size int64
	read int64
}

func (r *dummyReader) Read(p []byte) (int, error) {
	if r.read >= r.size {
		return 0, io.EOF
	}
	rem := r.size - r.read
	n := len(p)
	if int64(n) > rem {
		n = int(rem)
	}
	// just fill with zeros
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	r.read += int64(n)
	return n, nil
}

func TestUploadDownloadLargeFile(t *testing.T) {
	// Use 1MB size for unit testing streaming upload/download
	const fileSize = 1 * 1024 * 1024

	// Start a test server
	srv, err := New("", 10*1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	t.Run("Upload large file", func(t *testing.T) {
		bodyReader, bodyWriter := io.Pipe()
		writer := multipart.NewWriter(bodyWriter)

		go func() {
			defer bodyWriter.Close()
			defer writer.Close()

			part, err := writer.CreateFormFile("file", "large_test.bin")
			require.NoError(t, err)

			_, err = io.Copy(part, &dummyReader{size: fileSize})
			require.NoError(t, err)
		}()

		req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload", bodyReader)
		require.NoError(t, err)
		req.Header.Set("Content-Type", writer.FormDataContentType())

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Check if file was created locally and has correct size
		outName := "received_large_test.bin"
		defer os.Remove(outName)

		info, err := os.Stat(outName)
		require.NoError(t, err)
		assert.Equal(t, int64(fileSize), info.Size())
		if runtime.GOOS != "windows" {
			assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
		}

		// Setup server state to point to our newly uploaded file for download test
		srv.UpdateSharedFile(outName, "large_test.bin", int64(fileSize))

		t.Run("Download large file", func(t *testing.T) {
			resp, err := http.Get(ts.URL + "/api/download")
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			// Read and discard to simulate download
			n, err := io.Copy(io.Discard, resp.Body)
			require.NoError(t, err)

			assert.Equal(t, int64(fileSize), n)
		})
	})
}

func TestWriteLive_Truncation(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	// Max live backlog is 1 * 1024 * 1024 (1MB)
	// Let's create data slightly over 1MB
	part1 := make([]byte, 1024*1024)
	for i := range part1 {
		part1[i] = 'A'
	}
	part1[len(part1)-1] = '\n'

	part2 := []byte("hello world\n")

	// Total written: 1MB + len("hello world\n")
	srv.WriteLive(part1)
	srv.WriteLive(part2)

	backlog := srv.GetLiveBacklog()
	
	// We expect the first part (which is 'A's) to be truncated because it exceeds 1MB.
	// Actually, wait, let's look at the logic. 
	// len(liveData) = 1048576 + 12 = 1048588
	// maxLiveBacklog = 1048576
	// truncateIdx = 1048588 - 1048576 = 12
	// It searches for \n in liveData[12:].
	// Since part1 ends at 1048575, \n is at 1048575.
	// So it finds \n and slices after it.
	// This means the kept backlog will just be part2!
	
	// Let's verify.
	assert.Equal(t, part2, backlog)
}

func TestUploadPathTraversalAndPermissions(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	traversalFilenames := []struct {
		inputFilename    string
		expectedFilename string
	}{
		{"../../evil.sh", "received_evil.sh"},
		{"..\\..\\evil_win.bat", "received_evil_win.bat"},
		{"/absolute/path/test.txt", "received_test.txt"},
		{"nested/dir/sub/data.dat", "received_data.dat"},
		{"../../../etc/passwd", "received_passwd"},
		{"....", "received_upload.bin"},
		{"", "received_upload.bin"},
	}

	for _, tc := range traversalFilenames {
		t.Run(tc.inputFilename, func(t *testing.T) {
			bodyReader, bodyWriter := io.Pipe()
			writer := multipart.NewWriter(bodyWriter)

			go func() {
				defer bodyWriter.Close()
				defer writer.Close()

				part, err := writer.CreateFormFile("file", tc.inputFilename)
				require.NoError(t, err)
				_, err = part.Write([]byte("test content"))
				require.NoError(t, err)
			}()

			req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload", bodyReader)
			require.NoError(t, err)
			req.Header.Set("Content-Type", writer.FormDataContentType())

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			info, err := os.Stat(tc.expectedFilename)
			require.NoError(t, err, "File should be created at sanitized path: %s", tc.expectedFilename)
			defer os.Remove(tc.expectedFilename)

			if runtime.GOOS != "windows" {
				assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
			}
		})
	}
}

func TestLiveStream_ClientCleanupOnUpdateSharedFile(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/live/stream", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Ensure client is registered
	require.Eventually(t, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return len(srv.liveClients) == 1
	}, 5*time.Second, 10*time.Millisecond)

	// Calling UpdateSharedFile must close all active clients and clear liveClients slice
	srv.UpdateSharedFile("new_file.txt", "new_file.txt", 100)

	srv.mu.Lock()
	clientsLen := len(srv.liveClients)
	srv.mu.Unlock()
	assert.Equal(t, 0, clientsLen)
}

func TestNewRingBuffer_Clamping(t *testing.T) {
	rbSmall := NewRingBuffer(10)
	assert.Equal(t, MinRingBufferSize, len(rbSmall.buf))

	rbLarge := NewRingBuffer(500 * 1024 * 1024)
	assert.Equal(t, MaxRingBufferSize, len(rbLarge.buf))

	rbNormal := NewRingBuffer(1024 * 1024)
	assert.Equal(t, 1024*1024, len(rbNormal.buf))
}

func TestDownload_HTTPRangeRequests(t *testing.T) {
	// Create a temporary test file with known content
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "rangetest.bin")
	content := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz")
	totalSize := int64(len(content))
	err := os.WriteFile(filePath, content, 0644)
	require.NoError(t, err)

	srv, err := New(filePath, 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// 1. Full Download without Range header
	t.Run("Full Download", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/download")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "bytes", resp.Header.Get("Accept-Ranges"))
		assert.Equal(t, fmt.Sprintf("%d", totalSize), resp.Header.Get("Content-Length"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, content, body)
	})

	// 2. Initial Range: bytes=0-9
	t.Run("Initial Range bytes=0-9", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=0-9")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		assert.Equal(t, "10", resp.Header.Get("Content-Length"))
		assert.Equal(t, fmt.Sprintf("bytes 0-9/%d", totalSize), resp.Header.Get("Content-Range"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, content[0:10], body)
	})

	// 3. Open-ended Resumption Range: bytes=20-
	t.Run("Resume Range bytes=20-", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=20-")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		expectedLength := fmt.Sprintf("%d", totalSize-20)
		assert.Equal(t, expectedLength, resp.Header.Get("Content-Length"))
		assert.Equal(t, fmt.Sprintf("bytes 20-%d/%d", totalSize-1, totalSize), resp.Header.Get("Content-Range"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, content[20:], body)
	})

	// 4. Suffix Range: bytes=-10 (last 10 bytes)
	t.Run("Suffix Range bytes=-10", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=-10")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		assert.Equal(t, "10", resp.Header.Get("Content-Length"))
		assert.Equal(t, fmt.Sprintf("bytes %d-%d/%d", totalSize-10, totalSize-1, totalSize), resp.Header.Get("Content-Range"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, content[totalSize-10:], body)
	})

	// 5. Unsatisfiable Range: bytes=5000-6000
	t.Run("Unsatisfiable Range", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=5000-6000")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)
		assert.Equal(t, fmt.Sprintf("bytes */%d", totalSize), resp.Header.Get("Content-Range"))
	})

	// 6. Preflight CORS OPTIONS
	t.Run("CORS Preflight", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodOptions, ts.URL+"/api/download", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNoContent, resp.StatusCode)
		assert.Contains(t, resp.Header.Get("Access-Control-Allow-Headers"), "Range")
	})
}

func TestServer_CacheControlHeaders(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// Root HTML page must have Cache-Control: no-cache
	respIndex, err := http.Get(ts.URL + "/")
	require.NoError(t, err)
	defer respIndex.Body.Close()
	assert.Equal(t, http.StatusOK, respIndex.StatusCode)
	assert.Equal(t, "no-cache", respIndex.Header.Get("Cache-Control"))

	// Service Worker script must prevent caching
	respSW, err := http.Get(ts.URL + "/sw.js")
	require.NoError(t, err)
	defer respSW.Body.Close()
	assert.Equal(t, http.StatusOK, respSW.StatusCode)
	assert.Equal(t, "no-cache, no-store, must-revalidate", respSW.Header.Get("Cache-Control"))
	assert.Equal(t, "/", respSW.Header.Get("Service-Worker-Allowed"))
}
