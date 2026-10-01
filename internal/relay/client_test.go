package relay

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestRelayClient(t *testing.T) {
	// Create local test server acting as relay
	relayServer := NewServer()
	ts := httptest.NewServer(relayServer)
	defer ts.Close()

	client := NewClient(ts.URL)

	// 1. Test Register
	sessID, err := client.Register(context.Background())
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if sessID == "" {
		t.Fatalf("expected non-empty session ID")
	}

	// 2. Test PushState
	err = client.PushState(context.Background(), "offer-sdp-test", []map[string]interface{}{{"candidate": "cand1"}}, map[string]interface{}{"name": "test.txt", "size": 100}, nil)
	if err != nil {
		t.Fatalf("PushState failed: %v", err)
	}

	// 3. Test Poll (trigger download)
	go func() {
		sess := relayServer.getSession(sessID)
		if sess != nil {
			sess.EnqueueDownload(DownloadRequest{})
		}
	}()

	cmd, err := client.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}
	if cmd.Action != "download" {
		t.Fatalf("expected action 'download', got '%s'", cmd.Action)
	}

	// 4. Test UploadData
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "upload.txt")
	testData := []byte("Hello Relay Test Data!")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	// Setup Pipe on relay session so handleData receives stream
	sess := relayServer.getSession(sessID)
	pr, pw := io.Pipe()
	sess.SetPipes(pr, pw)

	uploadDone := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(pr)
		uploadDone <- data
	}()

	err = client.UploadData(context.Background(), filePath)
	if err != nil {
		t.Fatalf("UploadData unencrypted failed: %v", err)
	}

	received := <-uploadDone
	if string(received) != string(testData) {
		t.Fatalf("expected uploaded data '%s', got '%s'", string(testData), string(received))
	}

	// 5. Test Encrypted UploadData
	client.Key = make([]byte, 32)
	rand.Read(client.Key)

	sessID2, err := client.Register(context.Background())
	if err != nil {
		t.Fatalf("Register 2 failed: %v", err)
	}
	sess2 := relayServer.getSession(sessID2)

	pr2, pw2 := io.Pipe()
	sess2.SetPipes(pr2, pw2)

	encryptedDone := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(pr2)
		encryptedDone <- data
	}()

	err = client.UploadData(context.Background(), filePath)
	if err != nil {
		t.Fatalf("UploadData encrypted failed: %v", err)
	}

	encryptedReceived := <-encryptedDone
	if len(encryptedReceived) <= len(testData) {
		t.Fatalf("expected encrypted data payload to be larger than plaintext")
	}
}

func TestRelayClient_HTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	client := NewClient(ts.URL)
	_, err := client.Register(context.Background())
	if err == nil {
		t.Fatalf("expected error when server responds with 500")
	}
}

func TestUploadReaderAtOffset(t *testing.T) {
	relayServer := NewServer()
	ts := httptest.NewServer(relayServer)
	defer ts.Close()

	createIsolatedSession := func(t *testing.T) (*Client, *Session) {
		client := NewClient(ts.URL)
		sessID, err := client.Register(context.Background())
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}
		sess := relayServer.getSession(sessID)
		return client, sess
	}

	testData := []byte("Stream reader in-memory backlog test data!")

	t.Run("Unencrypted", func(t *testing.T) {
		client, sess := createIsolatedSession(t)
		pr1, pw1 := io.Pipe()
		sess.SetPipes(pr1, pw1)

		uploadDone := make(chan []byte, 1)
		go func() {
			data, _ := io.ReadAll(pr1)
			uploadDone <- data
		}()

		err := client.UploadReaderAtOffset(context.Background(), bytes.NewReader(testData), 0)
		if err != nil {
			t.Fatalf("UploadReaderAtOffset unencrypted failed: %v", err)
		}

		received := <-uploadDone
		if string(received) != string(testData) {
			t.Fatalf("expected uploaded data '%s', got '%s'", string(testData), string(received))
		}
	})

	t.Run("Encrypted", func(t *testing.T) {
		client, sess := createIsolatedSession(t)
		client.Key = make([]byte, 32)
		rand.Read(client.Key)

		pr2, pw2 := io.Pipe()
		sess.SetPipes(pr2, pw2)

		encryptedDone := make(chan []byte, 1)
		go func() {
			data, _ := io.ReadAll(pr2)
			encryptedDone <- data
		}()

		err := client.UploadReaderAtOffset(context.Background(), bytes.NewReader(testData), 0)
		if err != nil {
			t.Fatalf("UploadReaderAtOffset encrypted failed: %v", err)
		}

		encryptedReceived := <-encryptedDone
		if len(encryptedReceived) <= len(testData) {
			t.Fatalf("expected encrypted data payload to be larger than plaintext")
		}

		// Verify decrypting the encrypted stream
		decReader, err := NewDecryptingReader(bytes.NewReader(encryptedReceived), client.Key)
		if err != nil {
			t.Fatalf("NewDecryptingReader failed: %v", err)
		}
		decryptedData, err := io.ReadAll(decReader)
		if err != nil {
			t.Fatalf("Decrypting stream failed: %v", err)
		}
		if string(decryptedData) != string(testData) {
			t.Fatalf("expected decrypted data '%s', got '%s'", string(testData), string(decryptedData))
		}
	})

	t.Run("WithOffset", func(t *testing.T) {
		client, sess := createIsolatedSession(t)
		sess.mu.Lock()
		sess.RequestedOffset = 10
		sess.mu.Unlock()
		pr3, pw3 := io.Pipe()
		sess.SetPipes(pr3, pw3)

		offsetData := testData[10:]
		uploadDoneOffset := make(chan []byte, 1)
		go func() {
			data, _ := io.ReadAll(pr3)
			uploadDoneOffset <- data
		}()

		err := client.UploadReaderAtOffset(context.Background(), bytes.NewReader(offsetData), 10)
		if err != nil {
			t.Fatalf("UploadReaderAtOffset with offset failed: %v", err)
		}

		receivedOffset := <-uploadDoneOffset
		if string(receivedOffset) != string(offsetData) {
			t.Fatalf("expected offset uploaded data '%s', got '%s'", string(offsetData), string(receivedOffset))
		}
	})

	t.Run("InvalidKeyLength", func(t *testing.T) {
		client, _ := createIsolatedSession(t)
		client.Key = []byte("short-key-16bytes") // 17 bytes, not 32

		err := client.UploadReaderAtOffset(context.Background(), bytes.NewReader(testData), 0)
		if err == nil {
			t.Fatalf("expected error for invalid key length, got nil")
		}
	})
}

type mockHTTPClient struct {
	called bool
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.called = true
	return nil, nil
}

func (m *mockHTTPClient) Get(url string) (*http.Response, error) {
	m.called = true
	return nil, nil
}

func (m *mockHTTPClient) Post(url, contentType string, body io.Reader) (*http.Response, error) {
	m.called = true
	return nil, nil
}

func TestClient_InvalidKeyLength_NoHTTPRequestDispatched(t *testing.T) {
	invalidKeyLengths := []int{1, 10, 16, 31, 33, 64}

	for _, length := range invalidKeyLengths {
		mockHTTP := &mockHTTPClient{}
		client := NewClient("http://localhost:8080")
		client.HTTP = mockHTTP
		client.Key = make([]byte, length)

		err := client.UploadReaderAtOffset(context.Background(), bytes.NewReader([]byte("test data")), 0)
		if err == nil {
			t.Fatalf("expected error for key length %d, got nil", length)
		}
		if mockHTTP.called {
			t.Fatalf("expected no HTTP request dispatched for invalid key length %d", length)
		}

		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "nonexistent_or_dummy.txt")
		// Note: filePath doesn't even need to exist if key validation happens before os.Open
		_ = os.WriteFile(filePath, []byte("test data"), 0644)

		mockHTTP.called = false
		err = client.UploadDataAtOffset(context.Background(), filePath, 0)
		if err == nil {
			t.Fatalf("expected error for key length %d in UploadDataAtOffset, got nil", length)
		}
		if mockHTTP.called {
			t.Fatalf("expected no HTTP request dispatched for invalid key length %d in UploadDataAtOffset", length)
		}
	}
}

