package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
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
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	r.read += int64(n)
	return n, nil
}

func TestUploadDownloadLargeFile(t *testing.T) {
	const fileSize = 1 * 1024 * 1024

	srv, err := New("", 10*1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
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
		req.Header.Set("Authorization", "Bearer "+srv.Token())

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		outName := "received_large_test.bin"
		defer os.Remove(outName)

		info, err := os.Stat(outName)
		require.NoError(t, err)
		assert.Equal(t, int64(fileSize), info.Size())
		if runtime.GOOS != "windows" {
			assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
		}

		srv.UpdateSharedFile(outName, "large_test.bin", int64(fileSize))

		t.Run("Download large file", func(t *testing.T) {
			resp, err := http.Get(ts.URL + "/api/download?token=" + srv.Token())
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			n, err := io.Copy(io.Discard, resp.Body)
			require.NoError(t, err)

			assert.Equal(t, int64(fileSize), n)
		})
	})
}

func TestUploadInterruptedFile(t *testing.T) {
	srv, err := New("", 10*1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	bodyReader, bodyWriter := io.Pipe()
	writer := multipart.NewWriter(bodyWriter)

	go func() {
		part, err := writer.CreateFormFile("file", "interrupted_test.bin")
		if err != nil {
			bodyWriter.CloseWithError(err)
			return
		}
		_, _ = part.Write([]byte("some initial chunk"))
		_ = bodyWriter.CloseWithError(fmt.Errorf("connection reset by peer"))
	}()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload", bodyReader)
	require.NoError(t, err)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+srv.Token())

	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
		assert.NotEqual(t, http.StatusOK, resp.StatusCode)
	}

	outName := "received_interrupted_test.bin"
	assert.Eventually(t, func() bool {
		_, statErr := os.Stat(outName)
		return os.IsNotExist(statErr)
	}, 1*time.Second, 10*time.Millisecond, "partial file should be removed upon interrupted upload")
}

func TestWriteLive_Truncation(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	part1 := make([]byte, 1024*1024)
	for i := range part1 {
		part1[i] = 'A'
	}
	part1[len(part1)-1] = '\n'

	part2 := []byte("hello world\n")

	srv.WriteLive(part1)
	srv.WriteLive(part2)

	backlog := srv.GetLiveBacklog()
	assert.Equal(t, part2, backlog)
}

func TestUploadPathTraversalAndPermissions(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
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
			req.Header.Set("Authorization", "Bearer "+srv.Token())

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

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/live/stream?token="+srv.Token(), nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return len(srv.liveClients) == 1
	}, 5*time.Second, 10*time.Millisecond)

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
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "rangetest.bin")
	content := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz")
	totalSize := int64(len(content))
	err := os.WriteFile(filePath, content, 0644)
	require.NoError(t, err)

	srv, err := New(filePath, 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	t.Run("Full Download", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+srv.Token())

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "bytes", resp.Header.Get("Accept-Ranges"))
		assert.Equal(t, fmt.Sprintf("%d", totalSize), resp.Header.Get("Content-Length"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, content, body)
	})

	t.Run("Initial Range bytes=0-9", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=0-9")
		req.Header.Set("Authorization", "Bearer "+srv.Token())

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

	t.Run("Resume Range bytes=20-", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=20-")
		req.Header.Set("Authorization", "Bearer "+srv.Token())

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

	t.Run("Suffix Range bytes=-10", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=-10")
		req.Header.Set("Authorization", "Bearer "+srv.Token())

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

	t.Run("Unsatisfiable Range", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=5000-6000")
		req.Header.Set("Authorization", "Bearer "+srv.Token())

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)
		assert.Equal(t, fmt.Sprintf("bytes */%d", totalSize), resp.Header.Get("Content-Range"))
	})

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

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	respIndex, err := http.Get(ts.URL + "/")
	require.NoError(t, err)
	defer respIndex.Body.Close()
	assert.Equal(t, http.StatusOK, respIndex.StatusCode)
	assert.Equal(t, "no-cache", respIndex.Header.Get("Cache-Control"))

	respSW, err := http.Get(ts.URL + "/sw.js")
	require.NoError(t, err)
	defer respSW.Body.Close()
	assert.Equal(t, http.StatusOK, respSW.StatusCode)
	assert.Equal(t, "no-cache, no-store, must-revalidate", respSW.Header.Get("Cache-Control"))
	assert.Equal(t, "/", respSW.Header.Get("Service-Worker-Allowed"))
}

func TestPNAHeaders(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	endpoints := []struct {
		path   string
		method string
	}{
		{"/api/meta", http.MethodGet},
		{"/api/download", http.MethodGet},
		{"/api/upload", http.MethodPost},
		{"/api/live/stream", http.MethodGet},
		{"/api/qr?url=test", http.MethodGet},
	}

	for _, ep := range endpoints {
		t.Run(fmt.Sprintf("OPTIONS %s", ep.path), func(t *testing.T) {
			req, err := http.NewRequest(http.MethodOptions, ts.URL+ep.path, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, "true", resp.Header.Get("Access-Control-Allow-Private-Network"))
		})

		t.Run(fmt.Sprintf("%s %s", ep.method, ep.path), func(t *testing.T) {
			var req *http.Request
			if ep.method == http.MethodPost {
				var b bytes.Buffer
				writer := multipart.NewWriter(&b)
				writer.Close()
				req, err = http.NewRequest(http.MethodPost, ts.URL+ep.path, &b)
				require.NoError(t, err)
				req.Header.Set("Content-Type", writer.FormDataContentType())
			} else {
				req, err = http.NewRequest(ep.method, ts.URL+ep.path, nil)
				require.NoError(t, err)
			}
			req.Header.Set("Authorization", "Bearer "+srv.Token())

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, "true", resp.Header.Get("Access-Control-Allow-Private-Network"))
		})
	}
}

func TestLiveStream_ConcurrentSubscribersStress(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	const numSubscribers = 30
	const numMessages = 20

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	receivedCounts := make([]int64, numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)
		go func(subIdx int) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/live/stream?token="+srv.Token(), nil)
			if err != nil {
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			buf := make([]byte, 1024)
			for {
				n, err := resp.Body.Read(buf)
				if n > 0 {
					atomic.AddInt64(&receivedCounts[subIdx], int64(n))
				}
				if err != nil {
					return
				}
			}
		}(i)
	}

	require.Eventually(t, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return len(srv.liveClients) == numSubscribers
	}, 5*time.Second, 20*time.Millisecond)

	for m := 0; m < numMessages; m++ {
		msg := fmt.Sprintf("event-message-%04d\n", m)
		srv.WriteLive([]byte(msg))
		time.Sleep(5 * time.Millisecond)
	}

	require.Eventually(t, func() bool {
		for i := 0; i < numSubscribers; i++ {
			if atomic.LoadInt64(&receivedCounts[i]) == 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	wg.Wait()

	require.Eventually(t, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return len(srv.liveClients) == 0
	}, 5*time.Second, 20*time.Millisecond)
}

func TestLiveStreamReader_ContinuousStreaming(t *testing.T) {
	srv, err := New("", 10*1024*1024)
	require.NoError(t, err)

	srv.WriteLive([]byte("backlog 1\n"))

	reader := srv.NewLiveStreamReader(context.Background(), 0)
	defer reader.Close()

	buf := make([]byte, 1024)

	// Read initial backlog
	n, err := reader.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "backlog 1\n", string(buf[:n]))

	// Asynchronously write live chunks
	go func() {
		time.Sleep(20 * time.Millisecond)
		srv.WriteLive([]byte("live chunk 2\n"))
		time.Sleep(20 * time.Millisecond)
		srv.CloseLive()
	}()

	// Read live chunk
	n, err = reader.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "live chunk 2\n", string(buf[:n]))

	// Read EOF after CloseLive
	_, err = reader.Read(buf)
	assert.Equal(t, io.EOF, err)
}

func TestLiveStreamReader_WithOffset(t *testing.T) {
	srv, err := New("", 10*1024*1024)
	require.NoError(t, err)

	srv.WriteLive([]byte("0123456789"))

	reader := srv.NewLiveStreamReader(context.Background(), 5)
	defer reader.Close()

	buf := make([]byte, 1024)

	n, err := reader.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "56789", string(buf[:n]))

	go func() {
		time.Sleep(10 * time.Millisecond)
		srv.WriteLive([]byte("next"))
		srv.CloseLive()
	}()

	n, err = reader.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "next", string(buf[:n]))

	_, err = reader.Read(buf)
	assert.Equal(t, io.EOF, err)
}

func TestLiveStreamReader_CloseAndCleanup(t *testing.T) {
	srv, err := New("", 10*1024*1024)
	require.NoError(t, err)

	reader := srv.NewLiveStreamReader(context.Background(), 0)

	srv.mu.Lock()
	clientCount := len(srv.liveClients)
	srv.mu.Unlock()
	assert.Equal(t, 1, clientCount)

	err = reader.Close()
	require.NoError(t, err)

	srv.mu.Lock()
	clientCount = len(srv.liveClients)
	srv.mu.Unlock()
	assert.Equal(t, 0, clientCount)

	buf := make([]byte, 100)
	_, err = reader.Read(buf)
	assert.Equal(t, io.EOF, err)
}

func TestConcurrentMetaDownloadUpdateSharedFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath1 := filepath.Join(tmpDir, "file1.txt")
	filePath2 := filepath.Join(tmpDir, "file2.txt")
	content1 := []byte("content of file 1")
	content2 := []byte("content of file 2 - larger size")

	require.NoError(t, os.WriteFile(filePath1, content1, 0644))
	require.NoError(t, os.WriteFile(filePath2, content2, 0644))

	srv, err := New(filePath1, 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	const iterations = 100
	var wg sync.WaitGroup

	// Goroutine 1: Continuously update shared file state
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				srv.UpdateSharedFile(filePath1, "file1.txt", int64(len(content1)))
			} else {
				srv.UpdateSharedFile(filePath2, "file2.txt", int64(len(content2)))
			}
			time.Sleep(500 * time.Microsecond)
		}
	}()

	// Goroutine 2: Continuously request /api/meta
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 2 * time.Second}
		for i := 0; i < iterations; i++ {
			resp, err := client.Get(ts.URL + "/api/meta?token=" + srv.Token())
			if err == nil {
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				var meta FileMeta
				if err := json.NewDecoder(resp.Body).Decode(&meta); err == nil {
					assert.Contains(t, []string{"file1.txt", "file2.txt"}, meta.Name)
				}
				resp.Body.Close()
			}
			time.Sleep(500 * time.Microsecond)
		}
	}()

	// Goroutine 3: Continuously request /api/download
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 2 * time.Second}
		for i := 0; i < iterations; i++ {
			resp, err := client.Get(ts.URL + "/api/download?token=" + srv.Token())
			if err == nil {
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			time.Sleep(500 * time.Microsecond)
		}
	}()

	wg.Wait()
}

func TestConcurrentMetaAndDownloadDuringUpload(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.bin")
	err := os.WriteFile(filePath, []byte("initial file content"), 0644)
	require.NoError(t, err)

	srv, err := New(filePath, 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Goroutine 1: Rapidly update shared file metadata
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				i++
				newFile := filepath.Join(tmpDir, fmt.Sprintf("file_%d.bin", i))
				content := fmt.Sprintf("content_%d", i)
				_ = os.WriteFile(newFile, []byte(content), 0644)
				srv.UpdateSharedFile(newFile, fmt.Sprintf("file_%d.bin", i), int64(len(content)))
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Goroutine 2: Query /api/meta concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 1 * time.Second}
		for {
			select {
			case <-stop:
				return
			default:
				resp, err := client.Get(ts.URL + "/api/meta?token=" + srv.Token())
				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}
	}()

	// Goroutine 3: Query /api/download concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 1 * time.Second}
		for {
			select {
			case <-stop:
				return
			default:
				resp, err := client.Get(ts.URL + "/api/download?token=" + srv.Token())
				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestCORSOriginValidation(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	validOrigins := []string{
		"http://localhost:3000",
		"http://127.0.0.1:8080",
		"http://192.168.1.50:8080",
		"http://10.0.0.5:3000",
		"http://beam.local:8080",
	}

	for _, origin := range validOrigins {
		t.Run("ValidOrigin_"+origin, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/meta?token="+srv.Token(), nil)
			require.NoError(t, err)
			req.Header.Set("Origin", origin)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, origin, resp.Header.Get("Access-Control-Allow-Origin"))
		})
	}

	invalidOrigins := []string{
		"http://evil.com",
		"https://attacker.org",
		"http://8.8.8.8:8080",
	}

	for _, origin := range invalidOrigins {
		t.Run("InvalidOrigin_"+origin, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/meta?token="+srv.Token(), nil)
			require.NoError(t, err)
			req.Header.Set("Origin", origin)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.NotEqual(t, origin, resp.Header.Get("Access-Control-Allow-Origin"))
			assert.NotEqual(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
		})
	}
}

func TestSessionTokenAuthentication(t *testing.T) {
	srv, err := New("", 1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	t.Run("No Token", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/download")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("Invalid Token", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer invalid-token")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("Valid Bearer Token", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+srv.Token())

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("Valid Query Token", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/download?token=" + srv.Token())
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}
