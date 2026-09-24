package main

import (
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

	if !reflect.DeepEqual(parsedTurnServers, []string{"turn:1"}) {
		t.Fatalf("expected parsedTurnServers ['turn:1'], got %v", parsedTurnServers)
	}
	if parsedTurnUsername != "user" || parsedTurnCredential != "pass" {
		t.Fatalf("expected parsed turn auth user=user pass=pass, got user=%s pass=%s", parsedTurnUsername, parsedTurnCredential)
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

func TestDownloadFile_PathTraversalSanitization(t *testing.T) {
	traversalCases := []struct {
		rawMetaName      string
		expectedFileName string
	}{
		{"../../etc/passwd", "received_passwd"},
		{"..\\..\\evil.bat", "received_evil.bat"},
		{"\x00../malicious.sh", "received_malicious.sh"},
		{"....", "received_download.bin"},
	}

	for _, tc := range traversalCases {
		t.Run(tc.rawMetaName, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
				meta := server.FileMeta{
					Name: tc.rawMetaName,
					Size: 10,
				}
				json.NewEncoder(w).Encode(meta)
			})
			mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("0123456789"))
			})

			ts := httptest.NewServer(mux)
			defer ts.Close()

			defer os.Remove(tc.expectedFileName)

			err := downloadFile(ts.URL)
			if err != nil {
				t.Fatalf("downloadFile failed for %s: %v", tc.rawMetaName, err)
			}

			if _, err := os.Stat(tc.expectedFileName); os.IsNotExist(err) {
				t.Fatalf("expected file %s to exist, but was not found", tc.expectedFileName)
			}
		})
	}
}

func TestWebRTCDataChannel_DuplicateOffsetCancellationAndBackpressure(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "beam_test_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	data := make([]byte, 512*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	tmpFile.Write(data)
	tmpFile.Close()

	api := signaling.NewWebRTCAPI()
	pcSender, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create pcSender: %v", err)
	}
	defer pcSender.Close()

	pcReceiver, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed to create pcReceiver: %v", err)
	}
	defer pcReceiver.Close()

	pcSender.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			pcReceiver.AddICECandidate(c.ToJSON())
		}
	})
	pcReceiver.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			pcSender.AddICECandidate(c.ToJSON())
		}
	})

	var receiverDC *webrtc.DataChannel
	dcReady := make(chan struct{})

	var receivedChunks [][]byte
	var receivedChunksMu sync.Mutex
	eofReceived := make(chan struct{})

	var receivedBytesAfterMeta int
	pcReceiver.OnDataChannel(func(dc *webrtc.DataChannel) {
		receiverDC = dc
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				dataStr := string(msg.Data)
				if strings.HasPrefix(dataStr, "META:") {
					receivedChunksMu.Lock()
					receivedBytesAfterMeta = 0
					receivedChunksMu.Unlock()
				} else if dataStr == "EOF" {
					close(eofReceived)
				}
			} else {
				receivedChunksMu.Lock()
				c := make([]byte, len(msg.Data))
				copy(c, msg.Data)
				receivedChunks = append(receivedChunks, c)
				receivedBytesAfterMeta += len(msg.Data)
				receivedChunksMu.Unlock()
			}
		})
		dc.OnOpen(func() {
			close(dcReady)
		})
	})

	ordered := true
	dcSender, err := pcSender.CreateDataChannel("beam-file", &webrtc.DataChannelInit{
		Ordered: &ordered,
	})
	if err != nil {
		t.Fatalf("failed to create data channel: %v", err)
	}

	offer, err := pcSender.CreateOffer(nil)
	if err != nil {
		t.Fatalf("failed to create offer: %v", err)
	}
	if err := pcSender.SetLocalDescription(offer); err != nil {
		t.Fatalf("failed to set local sdp: %v", err)
	}

	<-webrtc.GatheringCompletePromise(pcSender)

	if err := pcReceiver.SetRemoteDescription(*pcSender.LocalDescription()); err != nil {
		t.Fatalf("failed to set remote sdp: %v", err)
	}

	answer, err := pcReceiver.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("failed to create answer: %v", err)
	}
	if err := pcReceiver.SetLocalDescription(answer); err != nil {
		t.Fatalf("failed to set local answer: %v", err)
	}

	<-webrtc.GatheringCompletePromise(pcReceiver)

	if err := pcSender.SetRemoteDescription(*pcReceiver.LocalDescription()); err != nil {
		t.Fatalf("failed to set remote answer: %v", err)
	}

	select {
	case <-dcReady:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for data channel to open")
	}

	time.Sleep(50 * time.Millisecond)

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	filePath := tmpFile.Name()
	fileName := filepath.Base(filePath)
	fileSize := int64(len(data))

	var streamMu sync.Mutex
	var streamCancel context.CancelFunc

	dcSender.OnMessage(func(msg webrtc.DataChannelMessage) {
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

			streamMu.Lock()
			if streamCancel != nil {
				streamCancel()
			}
			var streamCtx context.Context
			streamCtx, streamCancel = context.WithCancel(mainCtx)
			streamMu.Unlock()

			go func(ctx context.Context, reqOffset int64) {
				file, err := os.Open(filePath)
				if err != nil {
					return
				}
				defer file.Close()

				if reqOffset > 0 {
					_, err = file.Seek(reqOffset, io.SeekStart)
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
				dcSender.SendText(metaHeader)

				buffer := make([]byte, 16*1024)
				totalSent := reqOffset

				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					if dcSender.BufferedAmount() > 1024*1024 {
						waitForBufferedAmountLow(dcSender, 512*1024, 250*time.Millisecond)
					}

					n, err := file.Read(buffer)
					if n > 0 {
						select {
						case <-ctx.Done():
							return
						default:
						}

						chunk := make([]byte, n)
						copy(chunk, buffer[:n])

						errSend := dcSender.Send(chunk)
						if errSend != nil {
							return
						}
						totalSent += int64(n)
					}
					if err != nil {
						break
					}
				}

				if dcSender.BufferedAmount() > 0 {
					waitForBufferedAmountLow(dcSender, 0, 250*time.Millisecond)
				}

				select {
				case <-ctx.Done():
					return
				default:
				}

				dcSender.SendText("EOF")
			}(streamCtx, offset)
		}
	})

	receiverDC.SendText("OFFSET:0")
	time.Sleep(5 * time.Millisecond)
	receiverDC.SendText("OFFSET:1024")

	select {
	case <-eofReceived:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for EOF")
	}

	receivedChunksMu.Lock()
	defer receivedChunksMu.Unlock()

	expectedLen := len(data) - 1024
	if receivedBytesAfterMeta != expectedLen {
		t.Fatalf("expected received bytes after meta %d, got %d", expectedLen, receivedBytesAfterMeta)
	}
}

func TestWaitForBufferedAmountLow(t *testing.T) {
	// Nil DataChannel should return immediately without panic
	waitForBufferedAmountLow(nil, 512*1024, 10*time.Millisecond)

	pc1, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed pc1: %v", err)
	}
	defer pc1.Close()

	pc2, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed pc2: %v", err)
	}
	defer pc2.Close()

	dc1, err := pc1.CreateDataChannel("test-channel", nil)
	if err != nil {
		t.Fatalf("failed dc1: %v", err)
	}

	dcOpen := make(chan struct{})
	dc1.OnOpen(func() {
		close(dcOpen)
	})

	pc1.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = pc2.AddICECandidate(c.ToJSON())
		}
	})
	pc2.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = pc1.AddICECandidate(c.ToJSON())
		}
	})

	offer, err := pc1.CreateOffer(nil)
	if err != nil {
		t.Fatalf("failed offer: %v", err)
	}
	_ = pc1.SetLocalDescription(offer)
	_ = pc2.SetRemoteDescription(offer)

	answer, err := pc2.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("failed answer: %v", err)
	}
	_ = pc2.SetLocalDescription(answer)
	_ = pc1.SetRemoteDescription(answer)

	select {
	case <-dcOpen:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for data channel to open")
	}

	// BufferedAmount() is initially 0, so <= threshold 512KB resolves immediately
	start := time.Now()
	waitForBufferedAmountLow(dc1, 512*1024, 500*time.Millisecond)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("expected immediate resolution when bufferedAmount <= threshold")
	}

	// Timeout fallback check when waiting with 50ms timeout and bufferedAmount > threshold
	_ = dc1.Send(make([]byte, 2*1024*1024))
	start = time.Now()
	waitForBufferedAmountLow(dc1, 0, 50*time.Millisecond)
	elapsed := time.Since(start)
	if elapsed < 35*time.Millisecond {
		t.Fatalf("expected timeout or drain wait of at least ~35ms, got %v", elapsed)
	}
}
