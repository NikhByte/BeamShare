package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func TestWebRTCSenderGoroutineDeduplicationAndBackpressure(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "sender_test.bin")
	// Set test payload size to 2MB to ensure active stream in-flight when offset is changed
	fileSize := int64(2 * 1024 * 1024)
	testPayload := make([]byte, fileSize)
	for i := range testPayload {
		testPayload[i] = byte(i % 251)
	}
	if err := os.WriteFile(filePath, testPayload, 0600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	txDCReady := make(chan struct{})
	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to create sender session: %v", err)
	}
	defer senderSession.Close()

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		close(txDCReady)

		var (
			senderCancel context.CancelFunc
			senderMu     sync.Mutex
		)

		dc.OnClose(func() {
			senderMu.Lock()
			if senderCancel != nil {
				senderCancel()
				senderCancel = nil
			}
			senderMu.Unlock()
		})

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if !msg.IsString {
				return
			}
			dataStr := string(msg.Data)
			if strings.HasPrefix(dataStr, "OFFSET:") {
				parts := strings.SplitN(dataStr, ":", 2)
				var offset int64
				if len(parts) == 2 {
					offset, _ = strconv.ParseInt(parts[1], 10, 64)
				}

				senderMu.Lock()
				if senderCancel != nil {
					senderCancel()
				}
				var ctx context.Context
				ctx, senderCancel = context.WithCancel(mainCtx)
				senderMu.Unlock()

				go func(ctx context.Context) {
					file, err := os.Open(filePath)
					if err != nil {
						return
					}
					defer file.Close()

					if offset > 0 {
						_, err = file.Seek(offset, io.SeekStart)
						if err != nil {
							return
						}
					}

					metaHeader := fmt.Sprintf("META:%s:%d", filepath.Base(filePath), fileSize)
					if errSend := dc.SendText(metaHeader); errSend != nil {
						return
					}

					bufferedAmountLowChan := make(chan struct{}, 1)
					dc.SetBufferedAmountLowThreshold(512 * 1024)
					dc.OnBufferedAmountLow(func() {
						select {
						case bufferedAmountLowChan <- struct{}{}:
						default:
						}
					})

					buffer := make([]byte, 32*1024)
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
							case <-bufferedAmountLowChan:
							case <-time.After(50 * time.Millisecond):
							}
						}

						n, err := file.Read(buffer)
						if n > 0 {
							chunk := make([]byte, n)
							copy(chunk, buffer[:n])
							errSend := dc.Send(chunk)
							if errSend != nil {
								return
							}
							totalSent += int64(n)
						}
						if err != nil {
							break
						}
					}

					dc.SetBufferedAmountLowThreshold(0)
					for dc.BufferedAmount() > 0 {
						select {
						case <-ctx.Done():
							return
						case <-bufferedAmountLowChan:
						case <-time.After(50 * time.Millisecond):
						}
					}

					if ctx.Err() != nil {
						return
					}

					dc.SendText("EOF")
				}(ctx)
			}
		})
	}

	rxPC, err := signaling.NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create rxPC: %v", err)
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

	var rxDC *webrtc.DataChannel
	rxOpenCh := make(chan struct{})
	rxDCChan := make(chan *webrtc.DataChannel, 1)

	rxPC.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			select {
			case <-rxOpenCh:
			default:
				close(rxOpenCh)
			}
		})
		rxDCChan <- dc
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	if err != nil {
		t.Fatalf("CreateOffer failed: %v", err)
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

	answer, err := rxPC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("CreateAnswer failed: %v", err)
	}
	if err := rxPC.SetLocalDescription(answer); err != nil {
		t.Fatalf("SetLocalDescription failed: %v", err)
	}

	answerBytes, _ := json.Marshal(answer)
	if err := senderSession.ProvideAnswer(string(answerBytes)); err != nil {
		t.Fatalf("ProvideAnswer failed: %v", err)
	}

	mu.Lock()
	for _, cand := range rxCandidates {
		_ = senderSession.AddICECandidate(cand)
	}
	mu.Unlock()

	select {
	case rxDC = <-rxDCChan:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for rxDC")
	}

	select {
	case <-rxOpenCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for rxOpenCh")
	}

	select {
	case <-txDCReady:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for txDCReady")
	}

	var receivedBuf bytes.Buffer
	eofChan := make(chan struct{})
	firstChunkReceived := make(chan struct{}, 1)

	rxDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		mu.Lock()
		defer mu.Unlock()
		if msg.IsString {
			str := string(msg.Data)
			if strings.HasPrefix(str, "META:") {
				receivedBuf.Reset()
			} else if str == "EOF" {
				select {
				case <-eofChan:
				default:
					close(eofChan)
				}
			}
		} else {
			receivedBuf.Write(msg.Data)
			select {
			case firstChunkReceived <- struct{}{}:
			default:
			}
		}
	})

	// Send initial OFFSET:0 to start streaming
	if err := rxDC.SendText("OFFSET:0"); err != nil {
		t.Fatalf("SendText OFFSET:0 failed: %v", err)
	}

	// Wait until at least 1 chunk is received from the first stream
	select {
	case <-firstChunkReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first chunk")
	}

	// Reset buffer and send second OFFSET:32768 while the first stream is actively running
	offsetSecond := int64(32768)
	mu.Lock()
	receivedBuf.Reset()
	mu.Unlock()

	if err := rxDC.SendText(fmt.Sprintf("OFFSET:%d", offsetSecond)); err != nil {
		t.Fatalf("SendText OFFSET second failed: %v", err)
	}

	select {
	case <-eofChan:
		mu.Lock()
		receivedBytes := receivedBuf.Bytes()
		mu.Unlock()

		expectedBytes := testPayload[offsetSecond:]
		if !bytes.Equal(receivedBytes, expectedBytes) {
			t.Fatalf("payload mismatch! expected length %d, got %d", len(expectedBytes), len(receivedBytes))
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for EOF after deduplication stream")
	}
}
