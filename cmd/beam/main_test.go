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
	"unsafe"

	"github.com/beamshare/beam/internal/server"
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

func setupConnectedPeerConnections(t *testing.T) (*webrtc.PeerConnection, *webrtc.PeerConnection, *webrtc.DataChannel, *webrtc.DataChannel) {
	sPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create sender PC: %v", err)
	}
	rPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		sPC.Close()
		t.Fatalf("failed to create receiver PC: %v", err)
	}

	sDC, err := sPC.CreateDataChannel("data", nil)
	if err != nil {
		sPC.Close()
		rPC.Close()
		t.Fatalf("failed to create data channel: %v", err)
	}

	rDCC := make(chan *webrtc.DataChannel, 1)
	rPC.OnDataChannel(func(dc *webrtc.DataChannel) {
		rDCC <- dc
	})

	var mu sync.Mutex
	var sCands []webrtc.ICECandidateInit
	var rCands []webrtc.ICECandidateInit

	sPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		cand := c.ToJSON()
		mu.Lock()
		defer mu.Unlock()
		if rPC.RemoteDescription() != nil {
			_ = rPC.AddICECandidate(cand)
		} else {
			sCands = append(sCands, cand)
		}
	})

	rPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		cand := c.ToJSON()
		mu.Lock()
		defer mu.Unlock()
		if sPC.RemoteDescription() != nil {
			_ = sPC.AddICECandidate(cand)
		} else {
			rCands = append(rCands, cand)
		}
	})

	offer, err := sPC.CreateOffer(nil)
	if err != nil {
		sPC.Close()
		rPC.Close()
		t.Fatalf("failed to create offer: %v", err)
	}
	_ = sPC.SetLocalDescription(offer)
	_ = rPC.SetRemoteDescription(offer)

	mu.Lock()
	for _, c := range sCands {
		_ = rPC.AddICECandidate(c)
	}
	sCands = nil
	mu.Unlock()

	answer, err := rPC.CreateAnswer(nil)
	if err != nil {
		sPC.Close()
		rPC.Close()
		t.Fatalf("failed to create answer: %v", err)
	}
	_ = rPC.SetLocalDescription(answer)
	_ = sPC.SetRemoteDescription(answer)

	mu.Lock()
	for _, c := range rCands {
		_ = sPC.AddICECandidate(c)
	}
	rCands = nil
	mu.Unlock()

	var rDC *webrtc.DataChannel
	select {
	case rDC = <-rDCC:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for receiver datachannel")
	}

	sOpen := make(chan struct{})
	sDC.OnOpen(func() { close(sOpen) })

	select {
	case <-sOpen:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for sender datachannel open")
	}

	return sPC, rPC, sDC, rDC
}

func TestP2PSender_DuplicateOffsetCancellation(t *testing.T) {
	sPC, rPC, sDC, rDC := setupConnectedPeerConnections(t)
	defer sPC.Close()
	defer rPC.Close()

	payloadSize := 512 * 1024
	testPayload := make([]byte, payloadSize)
	for i := range testPayload {
		testPayload[i] = byte((i * 31) % 256)
	}

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "largefile.dat")
	if err := os.WriteFile(filePath, testPayload, 0600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	fileName := "largefile.dat"
	fileSize := int64(payloadSize)

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	var (
		senderMu     sync.Mutex
		senderCancel context.CancelFunc
	)

	sDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
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
				workerCtx, cancel := context.WithCancel(mainCtx)
				senderCancel = cancel
				senderMu.Unlock()

				go func(ctx context.Context, startOffset int64) {
					defer cancel()

					file, err := os.Open(filePath)
					if err != nil {
						return
					}
					defer file.Close()

					if startOffset > 0 {
						_, err = file.Seek(startOffset, io.SeekStart)
						if err != nil {
							return
						}
					}

					select {
					case <-ctx.Done():
						return
					default:
					}

					metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
					if errSend := sDC.SendText(metaHeader); errSend != nil {
						return
					}

					buffer := make([]byte, 16*1024)
					totalSent := startOffset

					for {
						select {
						case <-ctx.Done():
							return
						default:
						}

						for sDC.BufferedAmount() > 128*1024 {
							select {
							case <-ctx.Done():
								return
							case <-time.After(10 * time.Millisecond):
							}
						}

						n, errRead := file.Read(buffer)
						if n > 0 {
							chunk := make([]byte, n)
							copy(chunk, buffer[:n])
							errSend := sDC.Send(chunk)
							if errSend != nil {
								return
							}
							totalSent += int64(n)
						}
						if errRead != nil {
							break
						}
						time.Sleep(2 * time.Millisecond) // smooth out send rate
					}

					for sDC.BufferedAmount() > 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(10 * time.Millisecond):
						}
					}

					select {
					case <-ctx.Done():
						return
					default:
					}

					_ = sDC.SendText("EOF")
				}(workerCtx, offset)
			}
		}
	})

	var mu sync.Mutex
	var receivedBuf bytes.Buffer
	var receivedMetas []string
	eofChan := make(chan struct{}, 1)

	rDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		mu.Lock()
		defer mu.Unlock()

		if msg.IsString {
			if strings.HasPrefix(string(msg.Data), "META:") {
				receivedMetas = append(receivedMetas, string(msg.Data))
			} else if string(msg.Data) == "EOF" {
				select {
				case eofChan <- struct{}{}:
				default:
				}
			}
		} else {
			if len(receivedMetas) == 2 {
				receivedBuf.Write(msg.Data)
			}
		}
	})

	rDC.SendText("OFFSET:0")

	// Allow first stream to start sending
	time.Sleep(10 * time.Millisecond)

	targetOffset := int64(262144)
	mu.Lock()
	receivedBuf.Reset()
	mu.Unlock()

	// Send duplicate OFFSET message
	rDC.SendText(fmt.Sprintf("OFFSET:%d", targetOffset))

	select {
	case <-eofChan:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for EOF after duplicate OFFSET")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedMetas) != 2 {
		t.Fatalf("expected 2 META messages (one per OFFSET), got %d: %v", len(receivedMetas), receivedMetas)
	}

	expectedBytes := testPayload[targetOffset:]
	if !bytes.Equal(receivedBuf.Bytes(), expectedBytes) {
		t.Fatalf("received bytes mismatch for second offset stream: expected %d bytes, got %d bytes", len(expectedBytes), receivedBuf.Len())
	}
}

func TestP2PSender_FlowControlAndUnsharedSlices(t *testing.T) {
	sPC, rPC, sDC, rDC := setupConnectedPeerConnections(t)
	defer sPC.Close()
	defer rPC.Close()

	payloadSize := 256 * 1024
	testPayload := make([]byte, payloadSize)
	for i := range testPayload {
		testPayload[i] = byte((i * 13) % 256)
	}

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test.dat")
	if err := os.WriteFile(filePath, testPayload, 0600); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	var mu sync.Mutex
	var sentChunks [][]byte
	var receivedBuf bytes.Buffer
	eofReceived := make(chan struct{})

	rDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		mu.Lock()
		defer mu.Unlock()

		if msg.IsString {
			if string(msg.Data) == "EOF" {
				close(eofReceived)
			}
		} else {
			receivedBuf.Write(msg.Data)
		}
	})

	sDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString && strings.HasPrefix(string(msg.Data), "OFFSET:") {
			go func() {
				file, err := os.Open(filePath)
				if err != nil {
					return
				}
				defer file.Close()

				metaHeader := fmt.Sprintf("META:test.dat:%d", payloadSize)
				_ = sDC.SendText(metaHeader)

				buffer := make([]byte, 16*1024)

				for {
					select {
					case <-mainCtx.Done():
						return
					default:
					}

					for sDC.BufferedAmount() > 64*1024 {
						select {
						case <-mainCtx.Done():
							return
						case <-time.After(10 * time.Millisecond):
						}
					}

					n, errRead := file.Read(buffer)
					if n > 0 {
						chunk := make([]byte, n)
						copy(chunk, buffer[:n])

						mu.Lock()
						sentChunks = append(sentChunks, chunk)
						mu.Unlock()

						if errSend := sDC.Send(chunk); errSend != nil {
							return
						}
					}
					if errRead != nil {
						break
					}
				}

				for sDC.BufferedAmount() > 0 {
					select {
					case <-mainCtx.Done():
						return
					case <-time.After(10 * time.Millisecond):
					}
				}

				_ = sDC.SendText("EOF")
			}()
		}
	})

	rDC.SendText("OFFSET:0")

	select {
	case <-eofReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for EOF")
	}

	mu.Lock()
	defer mu.Unlock()

	if !bytes.Equal(receivedBuf.Bytes(), testPayload) {
		t.Fatalf("received data mismatch: got %d bytes, expected %d bytes", receivedBuf.Len(), len(testPayload))
	}

	ptrMap := make(map[uintptr]bool)
	for _, chk := range sentChunks {
		ptr := uintptr(unsafe.Pointer(&chk[0]))
		if ptrMap[ptr] {
			t.Fatalf("duplicate chunk pointer detected: 0x%x was reused across chunks", ptr)
		}
		ptrMap[ptr] = true
	}
}
