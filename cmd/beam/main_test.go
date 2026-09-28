package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/beamshare/beam/internal/relay"
	"github.com/beamshare/beam/internal/server"
)

func TestParseFlags(t *testing.T) {
	argsIn := []string{"--stun-server=stun:1", "--stun-server", "stun:2", "--turn-server=turn:1", "--turn-username", "user", "--turn-credential=pass", "--discovery-timeout=15", "send", "file.txt"}
	cleanArgs, iceServers, discoveryTimeout := parseFlags(argsIn)

	expectedArgs := []string{"send", "file.txt"}
	if !reflect.DeepEqual(cleanArgs, expectedArgs) {
		t.Fatalf("expected args %v, got %v", expectedArgs, cleanArgs)
	}

	if len(iceServers) != 2 {
		t.Fatalf("expected 2 ICE servers, got %d", len(iceServers))
	}

	if !reflect.DeepEqual(iceServers[0].URLs, []string{"stun:1", "stun:2"}) {
		t.Fatalf("expected stun URLs, got %v", iceServers[0].URLs)
	}

	if !reflect.DeepEqual(iceServers[1].URLs, []string{"turn:1"}) {
		t.Fatalf("expected turn URLs, got %v", iceServers[1].URLs)
	}

	if iceServers[1].Username != "user" || iceServers[1].Credential != "pass" {
		t.Fatalf("expected turn auth, got user=%v pass=%v", iceServers[1].Username, iceServers[1].Credential)
	}

	if discoveryTimeout != 15*1000*1000*1000 { // 15 seconds
		t.Fatalf("expected discovery timeout 15s, got %v", discoveryTimeout)
	}
}

func TestBufferSizeClamping(t *testing.T) {
	// Test excessively large buffer size gets clamped to 100MB
	argsExcessive := []string{"--buffer-size=1000000000", "send", "file.txt"}
	parseFlags(argsExcessive)
	if liveBufferSize != 100*1024*1024 {
		t.Fatalf("expected liveBufferSize clamped to 100MB, got %d", liveBufferSize)
	}

	// Test negative/sub-minimum buffer size gets clamped to 64KB
	argsSubMin := []string{"--buffer-size=100", "send", "file.txt"}
	parseFlags(argsSubMin)
	if liveBufferSize != 64*1024 {
		t.Fatalf("expected liveBufferSize clamped to 64KB, got %d", liveBufferSize)
	}
}

func TestDownloadFile_PlainHTTP(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		meta := server.FileMeta{
			Name: "test_download.txt",
			Size: 12,
		}
		json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello World!"))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	defer os.Remove("received_test_download.txt")

	err := downloadFile(ts.URL)
	if err != nil {
		t.Fatalf("downloadFile failed: %v", err)
	}
}

func TestDownloadFile_Relay(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("s") != "session123" {
			t.Fatalf("expected session ID 'session123', got '%s'", r.URL.Query().Get("s"))
		}
		meta := server.FileMeta{
			Name: "test_download_relay.txt",
			Size: 15,
		}
		json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("s") != "session123" {
			t.Fatalf("expected session ID 'session123', got '%s'", r.URL.Query().Get("s"))
		}
		w.Write([]byte("Relay Data 1234"))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	defer os.Remove("received_test_download_relay.txt")

	urlWithSession := "http://example.com/?backend=" + ts.URL + "&s=session123"

	err := downloadFile(urlWithSession)
	if err != nil {
		t.Fatalf("downloadFile failed: %v", err)
	}
}

func TestDownloadFile_InvalidKey(t *testing.T) {
	// Test bad base64 key
	err := downloadFile("http://example.com/#k=!!!invalid_base64!!!")
	if err == nil {
		t.Fatal("expected error for invalid base64 key fragment, got nil")
	}

	// Test non-32-byte key (16 bytes = 128 bit)
	shortKey := make([]byte, 16)
	shortKeyB64 := base64.URLEncoding.EncodeToString(shortKey)
	err = downloadFile("http://example.com/#k=" + shortKeyB64)
	if err == nil {
		t.Fatal("expected error for 16-byte key fragment, got nil")
	}

	// Test non-32-byte key (64 bytes)
	longKey := make([]byte, 64)
	longKeyB64 := base64.URLEncoding.EncodeToString(longKey)
	err = downloadFile("http://example.com/#k=" + longKeyB64)
	if err == nil {
		t.Fatal("expected error for 64-byte key fragment, got nil")
	}
}

func TestDownloadFile_Encrypted(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	keyStr := base64.URLEncoding.EncodeToString(key)

	plainData := []byte("Encrypted Payload Data 1234567890")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		meta := server.FileMeta{
			Name: "test_encrypted.txt",
			Size: int64(len(plainData)),
		}
		json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		encReader, err := relay.NewEncryptingReader(bytes.NewReader(plainData), key)
		if err != nil {
			t.Fatalf("NewEncryptingReader failed: %v", err)
		}
		encData, err := io.ReadAll(encReader)
		if err != nil {
			t.Fatalf("ReadAll encReader failed: %v", err)
		}
		w.Write(encData)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	defer os.Remove("received_test_encrypted.txt")

	downloadURL := "http://example.com/?backend=" + ts.URL + "#k=" + keyStr
	err := downloadFile(downloadURL)
	if err != nil {
		t.Fatalf("downloadFile encrypted failed: %v", err)
	}

	downloaded, err := os.ReadFile("received_test_encrypted.txt")
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(downloaded, plainData) {
		t.Fatalf("expected decrypted data '%s', got '%s'", string(plainData), string(downloaded))
	}
}

