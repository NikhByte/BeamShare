package relay

import (
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
	err = client.PushState(context.Background(), "offer-sdp-test", []map[string]interface{}{{"candidate": "cand1"}}, map[string]interface{}{"name": "test.txt", "size": 100})
	if err != nil {
		t.Fatalf("PushState failed: %v", err)
	}

	// 3. Test Poll (trigger download)
	go func() {
		sess := relayServer.getSession(sessID)
		if sess != nil {
			sess.DownloadReq <- DownloadRequest{}
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
	sess.DataPipeR = pr
	sess.DataPipeW = pw

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

	pr2, pw2 := io.Pipe()
	sess.DataPipeR = pr2
	sess.DataPipeW = pw2

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
