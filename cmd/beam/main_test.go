package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/server"
	"github.com/beamshare/beam/internal/signaling"
	"github.com/pion/webrtc/v3"
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

func TestWebRTCDuplicateOffsetAndBackpressure(t *testing.T) {
	// Create temporary test file
	fileSize := int64(150 * 1024) // 150KB
	testData := make([]byte, fileSize)
	for i := range testData {
		testData[i] = byte(i % 251)
	}
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_transfer.bin")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	fileName := "test_transfer.bin"

	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to create sender session: %v", err)
	}
	defer senderSession.Close()

	senderTxReady := make(chan struct{})

	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		var (
			transferMu     sync.Mutex
			cancelTransfer context.CancelFunc
		)

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				dataStr := string(msg.Data)
				if strings.HasPrefix(dataStr, "OFFSET:") {
					parts := strings.SplitN(dataStr, ":", 2)
					var offset int64
					if len(parts) == 2 {
						offset, _ = strconv.ParseInt(parts[1], 10, 64)
					}

					transferMu.Lock()
					if cancelTransfer != nil {
						cancelTransfer()
					}
					ctx, cancel := context.WithCancel(context.Background())
					cancelTransfer = cancel
					transferMu.Unlock()

					go func(ctx context.Context) {
						file, err := os.Open(filePath)
						if err != nil {
							return
						}
						defer file.Close()

						if offset > 0 {
							if _, err := file.Seek(offset, 0); err != nil {
								return
							}
						}

						metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
						if errSend := dc.SendText(metaHeader); errSend != nil {
							return
						}

						buffer := make([]byte, 16*1024)
						totalSent := offset

						for {
							select {
							case <-ctx.Done():
								return
							default:
							}

							for dc.BufferedAmount() > 1024*1024 {
								select {
								case <-ctx.Done():
									return
								case <-time.After(10 * time.Millisecond):
								}
							}

							n, err := file.Read(buffer)
							if n > 0 {
								chunk := make([]byte, n)
								copy(chunk, buffer[:n])
								if errSend := dc.Send(chunk); errSend != nil {
									return
								}
								totalSent += int64(n)
							}
							if err != nil {
								break
							}
						}

						for dc.BufferedAmount() > 0 {
							select {
							case <-ctx.Done():
								return
							case <-time.After(10 * time.Millisecond):
							}
						}
						_ = dc.SendText("EOF")
					}(ctx)
				}
			}
		})
		close(senderTxReady)
	}

	rxPC, err := signaling.NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create receiver PeerConnection: %v", err)
	}
	defer rxPC.Close()

	var mu sync.Mutex
	var rxCandidates []webrtc.ICECandidateInit

	rxPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			cand := c.ToJSON()
			mu.Lock()
			rxCandidates = append(rxCandidates, cand)
			mu.Unlock()
			_ = senderSession.AddICECandidate(cand)
		}
	})

	var receivedBuf bytes.Buffer
	eofReceived := make(chan struct{})

	rxOpenCh := make(chan struct{})
	rxDataChannelCh := make(chan *webrtc.DataChannel, 1)

	rxPC.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			select {
			case <-rxOpenCh:
			default:
				close(rxOpenCh)
			}
		})
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			mu.Lock()
			defer mu.Unlock()
			if msg.IsString {
				str := string(msg.Data)
				if str == "EOF" {
					close(eofReceived)
				}
			} else {
				receivedBuf.Write(msg.Data)
			}
		})
		rxDataChannelCh <- dc
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	if err != nil {
		t.Fatalf("failed to create offer: %v", err)
	}

	if err := rxPC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  senderSession.RawOffer(),
	}); err != nil {
		t.Fatalf("SetRemoteDescription failed: %v", err)
	}

	for _, cand := range senderSession.GetCandidates() {
		_ = rxPC.AddICECandidate(cand)
	}

	senderSession.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = rxPC.AddICECandidate(c.ToJSON())
		}
	})

	answerObj, err := rxPC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("CreateAnswer failed: %v", err)
	}
	if err := rxPC.SetLocalDescription(answerObj); err != nil {
		t.Fatalf("SetLocalDescription failed: %v", err)
	}

	answerBytes, err := json.Marshal(answerObj)
	if err != nil {
		t.Fatalf("marshal answer failed: %v", err)
	}

	if err := senderSession.ProvideAnswer(string(answerBytes)); err != nil {
		t.Fatalf("ProvideAnswer failed: %v", err)
	}

	mu.Lock()
	for _, cand := range rxCandidates {
		_ = senderSession.AddICECandidate(cand)
	}
	mu.Unlock()

	var rxDC *webrtc.DataChannel
	select {
	case rxDC = <-rxDataChannelCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel")
	}

	select {
	case <-rxOpenCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for rxOpenCh")
	}

	select {
	case <-senderTxReady:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for senderTxReady")
	}

	// Send initial OFFSET:0 to start transfer
	_ = rxDC.SendText("OFFSET:0")

	// Sleep briefly so first transfer starts
	time.Sleep(10 * time.Millisecond)

	// Send duplicate OFFSET:50000 while first transfer is streaming
	mu.Lock()
	receivedBuf.Reset() // Clear buffer for new transfer start
	mu.Unlock()

	targetOffset := int64(50000)
	_ = rxDC.SendText(fmt.Sprintf("OFFSET:%d", targetOffset))

	// Wait for EOF
	select {
	case <-eofReceived:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for EOF after duplicate OFFSET")
	}

	mu.Lock()
	receivedBytes := receivedBuf.Bytes()
	mu.Unlock()

	expectedData := testData[targetOffset:]
	if !bytes.Equal(receivedBytes, expectedData) {
		t.Fatalf("received data length %d mismatch expected length %d (data corrupt)", len(receivedBytes), len(expectedData))
	}
}

