package relay

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
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

	for _, keyLen := range []int{32} {
		t.Run(fmt.Sprintf("Encrypted_%dByteKey", keyLen), func(t *testing.T) {
			client, sess := createIsolatedSession(t)
			client.Key = make([]byte, keyLen)
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
	}

	t.Run("InvalidKeyLength", func(t *testing.T) {
		client, _ := createIsolatedSession(t)
		invalidKeys := [][]byte{
			make([]byte, 10),
			make([]byte, 16),
			make([]byte, 24),
			make([]byte, 31),
			make([]byte, 33),
			make([]byte, 40),
		}
		for _, key := range invalidKeys {
			client.Key = key
			err := client.UploadReaderAtOffset(context.Background(), bytes.NewReader(testData), 0)
			if !errors.Is(err, ErrInvalidKeySize) {
				t.Fatalf("expected ErrInvalidKeySize for key length %d, got %v", len(key), err)
			}
		}
	})

	t.Run("InvalidKeyLengths", func(t *testing.T) {
		invalidKeys := [][]byte{
			make([]byte, 10),
			make([]byte, 16),
			make([]byte, 64),
		}

		for _, badKey := range invalidKeys {
			client, _ := createIsolatedSession(t)
			client.Key = badKey

			err := client.UploadReaderAtOffset(context.Background(), bytes.NewReader(testData), 0)
			if err == nil {
				t.Fatalf("expected error for key length %d in UploadReaderAtOffset, got nil", len(badKey))
			}

			tmpFile := filepath.Join(t.TempDir(), "test.txt")
			os.WriteFile(tmpFile, testData, 0644)
			err = client.UploadData(context.Background(), tmpFile)
			if err == nil {
				t.Fatalf("expected error for key length %d in UploadData, got nil", len(badKey))
			}
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

}

func TestRelayClient_InvalidKeyLength(t *testing.T) {
	relayServer := NewServer()
	ts := httptest.NewServer(relayServer)
	defer ts.Close()

	client := NewClient(ts.URL)
	sessID, err := client.Register(context.Background())
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	sess := relayServer.getSession(sessID)
	pr, pw := io.Pipe()
	sess.SetPipes(pr, pw)
	defer pr.Close()
	defer pw.Close()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "dummy.txt")
	if err := os.WriteFile(filePath, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	invalidKeys := [][]byte{
		[]byte("too-short"),
		make([]byte, 16),
		make([]byte, 31),
		make([]byte, 33),
		make([]byte, 64),
	}

	for _, key := range invalidKeys {
		client.Key = key

		err := client.UploadData(context.Background(), filePath)
		if !errors.Is(err, ErrInvalidKeySize) {
			t.Fatalf("expected ErrInvalidKeySize for UploadData with key length %d, got %v", len(key), err)
		}

		err = client.UploadReaderAtOffset(context.Background(), bytes.NewReader([]byte("data")), 0)
		if !errors.Is(err, ErrInvalidKeySize) {
			t.Fatalf("expected ErrInvalidKeySize for UploadReaderAtOffset with key length %d, got %v", len(key), err)
		}
	}
}

type mockHTTPClient struct {
	called bool
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.called = true
	return nil, fmt.Errorf("HTTP request should not have been emitted")
}

func (m *mockHTTPClient) Get(url string) (*http.Response, error) {
	m.called = true
	return nil, fmt.Errorf("HTTP request should not have been emitted")
}

func (m *mockHTTPClient) Post(url, contentType string, body io.Reader) (*http.Response, error) {
	m.called = true
	return nil, fmt.Errorf("HTTP request should not have been emitted")
}

func TestUploadReaderAtOffset_InvalidKeyLength(t *testing.T) {
	invalidKeyLengths := []int{1, 10, 16, 31, 33, 64}

	for _, keyLen := range invalidKeyLengths {
		t.Run(fmt.Sprintf("KeyLength_%d", keyLen), func(t *testing.T) {
			mockHTTP := &mockHTTPClient{}
			client := &Client{
				BaseURL:   "http://localhost:9999",
				SessionID: "test-session",
				HTTP:      mockHTTP,
				Key:       make([]byte, keyLen),
			}

			err := client.UploadReaderAtOffset(context.Background(), bytes.NewReader([]byte("test data")), 0)
			if !errors.Is(err, ErrInvalidKeySize) {
				t.Fatalf("expected ErrInvalidKeySize for key length %d, got %v", keyLen, err)
			}
			if mockHTTP.called {
				t.Fatalf("expected no HTTP request dispatched for invalid key length %d", keyLen)
			}

			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, "dummy.txt")
			_ = os.WriteFile(filePath, []byte("test data"), 0644)

			mockHTTP.called = false
			err = client.UploadDataAtOffset(context.Background(), filePath, 0)
			if !errors.Is(err, ErrInvalidKeySize) {
				t.Fatalf("expected ErrInvalidKeySize for key length %d in UploadDataAtOffset, got %v", keyLen, err)
			}
			if mockHTTP.called {
				t.Fatalf("expected no HTTP request dispatched for invalid key length %d in UploadDataAtOffset", keyLen)
			}
		})
	}
}
