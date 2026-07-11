package server

import (
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

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
	// Use a 2GB size for testing
	const fileSize = 2 * 1024 * 1024 * 1024

	// Start a test server
	srv, err := New("")
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	t.Run("Upload 2GB file", func(t *testing.T) {
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

		// Setup server state to point to our newly uploaded file for download test
		srv.UpdateSharedFile(outName, "large_test.bin", int64(fileSize))

		t.Run("Download 2GB file", func(t *testing.T) {
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
	srv, err := New("")
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
