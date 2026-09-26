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

func TestWebRTCFileSender_DuplicateOffsetCancellationAndIntegrity(t *testing.T) {
	// Create a temporary test file with known contents (500 KB)
	testContent := make([]byte, 500*1024)
	for i := range testContent {
		testContent[i] = byte(i % 251)
	}

	tmpFile, err := os.CreateTemp("", "webrtc_test_*.bin")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.Write(testContent); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	filePath := tmpFile.Name()
	fileName := "webrtc_test.bin"
	fileSize := int64(len(testContent))

	// Setup WebRTC peer connections
	api := signaling.NewWebRTCAPI()
	pcSender, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection sender failed: %v", err)
	}
	defer pcSender.Close()

	pcReceiver, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection receiver failed: %v", err)
	}
	defer pcReceiver.Close()

	var senderCandidates []webrtc.ICECandidateInit
	var senderCandMu sync.Mutex
	pcSender.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			senderCandMu.Lock()
			senderCandidates = append(senderCandidates, c.ToJSON())
			senderCandMu.Unlock()
		}
	})

	var receiverCandidates []webrtc.ICECandidateInit
	var receiverCandMu sync.Mutex
	pcReceiver.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			receiverCandMu.Lock()
			receiverCandidates = append(receiverCandidates, c.ToJSON())
			receiverCandMu.Unlock()
		}
	})

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	// DataChannel setup
	dcSender, err := pcSender.CreateDataChannel("transfer", nil)
	if err != nil {
		t.Fatalf("CreateDataChannel failed: %v", err)
	}

	dcSenderReady := make(chan struct{})
	dcSender.OnOpen(func() {
		close(dcSenderReady)
	})

	var (
		senderMu     sync.Mutex
		senderCancel context.CancelFunc
	)

	// Attach sender handler matching main.go
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

			senderMu.Lock()
			if senderCancel != nil {
				senderCancel()
			}
			var senderCtx context.Context
			senderCtx, senderCancel = context.WithCancel(mainCtx)
			senderMu.Unlock()

			go func() {
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

				select {
				case <-senderCtx.Done():
					return
				default:
				}

				metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
				if errSend := dcSender.SendText(metaHeader); errSend != nil {
					return
				}

				bufferedAmountLowChan := make(chan struct{}, 1)
				dcSender.SetBufferedAmountLowThreshold(512 * 1024)
				dcSender.OnBufferedAmountLow(func() {
					select {
					case bufferedAmountLowChan <- struct{}{}:
					default:
					}
				})

				buffer := make([]byte, 16*1024)
				totalSent := offset

				for {
					select {
					case <-senderCtx.Done():
						return
					default:
					}

					if dcSender.BufferedAmount() > 1024*1024 {
						select {
						case <-bufferedAmountLowChan:
						default:
						}

						ticker := time.NewTicker(20 * time.Millisecond)
						for dcSender.BufferedAmount() > 1024*1024 {
							select {
							case <-senderCtx.Done():
								ticker.Stop()
								return
							case <-bufferedAmountLowChan:
							case <-ticker.C:
							}
						}
						ticker.Stop()
					}

					select {
					case <-senderCtx.Done():
						return
					default:
					}

					n, err := file.Read(buffer)
					if n > 0 {
						chunk := make([]byte, n)
						copy(chunk, buffer[:n])
						errSend := dcSender.Send(chunk)
						if errSend != nil {
							return
						}
						totalSent += int64(n)
						time.Sleep(1 * time.Millisecond)
					}
					if err != nil {
						break
					}
				}

				select {
				case <-senderCtx.Done():
					return
				default:
				}

				dcSender.SetBufferedAmountLowThreshold(0)
				if dcSender.BufferedAmount() > 0 {
					select {
					case <-bufferedAmountLowChan:
					default:
					}

					ticker := time.NewTicker(20 * time.Millisecond)
					for dcSender.BufferedAmount() > 0 {
						select {
						case <-senderCtx.Done():
							ticker.Stop()
							return
						case <-bufferedAmountLowChan:
						case <-ticker.C:
						}
					}
					ticker.Stop()
				}

				select {
				case <-senderCtx.Done():
					return
				default:
				}

				dcSender.SendText("EOF")
			}()
		}
	})

	var (
		rxMu         sync.Mutex
		rxBuf        bytes.Buffer
		metaReceived string
		eofChan      = make(chan struct{}, 1)
	)

	rxDataChannelCh := make(chan *webrtc.DataChannel, 1)
	pcReceiver.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			rxMu.Lock()
			defer rxMu.Unlock()
			if msg.IsString {
				str := string(msg.Data)
				if strings.HasPrefix(str, "META:") {
					metaReceived = str
					rxBuf.Reset()
				} else if str == "EOF" {
					select {
					case eofChan <- struct{}{}:
					default:
					}
				}
			} else {
				rxBuf.Write(msg.Data)
			}
		})
		rxDataChannelCh <- dc
	})

	// SDP Offer / Answer Exchange
	offer, err := pcSender.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer failed: %v", err)
	}
	if err := pcSender.SetLocalDescription(offer); err != nil {
		t.Fatalf("SetLocalDescription sender failed: %v", err)
	}
	if err := pcReceiver.SetRemoteDescription(offer); err != nil {
		t.Fatalf("SetRemoteDescription receiver failed: %v", err)
	}

	senderCandMu.Lock()
	for _, cand := range senderCandidates {
		_ = pcReceiver.AddICECandidate(cand)
	}
	pcSender.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = pcReceiver.AddICECandidate(c.ToJSON())
		}
	})
	senderCandMu.Unlock()

	answer, err := pcReceiver.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("CreateAnswer failed: %v", err)
	}
	if err := pcReceiver.SetLocalDescription(answer); err != nil {
		t.Fatalf("SetLocalDescription receiver failed: %v", err)
	}
	if err := pcSender.SetRemoteDescription(answer); err != nil {
		t.Fatalf("SetRemoteDescription sender failed: %v", err)
	}

	receiverCandMu.Lock()
	for _, cand := range receiverCandidates {
		_ = pcSender.AddICECandidate(cand)
	}
	pcReceiver.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = pcSender.AddICECandidate(c.ToJSON())
		}
	})
	receiverCandMu.Unlock()

	select {
	case <-dcSenderReady:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for sender DataChannel open")
	}

	var dcReceiver *webrtc.DataChannel
	select {
	case dcReceiver = <-rxDataChannelCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for receiver DataChannel")
	}

	for dcReceiver.ReadyState() != webrtc.DataChannelStateOpen {
		time.Sleep(10 * time.Millisecond)
	}

	// Test 1: Full transfer from offset 0
	if err := dcReceiver.SendText("OFFSET:0"); err != nil {
		t.Fatalf("SendText OFFSET:0 failed: %v", err)
	}

	select {
	case <-eofChan:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for EOF")
	}

	rxMu.Lock()
	if metaReceived != fmt.Sprintf("META:%s:%d", fileName, fileSize) {
		t.Fatalf("unexpected meta header: %s", metaReceived)
	}
	if !bytes.Equal(rxBuf.Bytes(), testContent) {
		t.Fatalf("received content mismatch (length %d vs expected %d)", rxBuf.Len(), len(testContent))
	}
	rxMu.Unlock()

	// Test 2: Duplicate OFFSET message interrupts active transfer and resumes cleanly from offset 50,000
	rxMu.Lock()
	rxBuf.Reset()
	metaReceived = ""
	rxMu.Unlock()

	if err := dcReceiver.SendText("OFFSET:0"); err != nil {
		t.Fatalf("SendText OFFSET:0 failed: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	rxMu.Lock()
	rxBuf.Reset()
	metaReceived = ""
	rxMu.Unlock()

	const resumeOffset int64 = 50000
	if err := dcReceiver.SendText(fmt.Sprintf("OFFSET:%d", resumeOffset)); err != nil {
		t.Fatalf("SendText OFFSET:%d failed: %v", resumeOffset, err)
	}

	select {
	case <-eofChan:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for EOF after duplicate OFFSET")
	}

	rxMu.Lock()
	expectedResumedContent := testContent[resumeOffset:]
	if !bytes.Equal(rxBuf.Bytes(), expectedResumedContent) {
		t.Fatalf("resumed content mismatch: got %d bytes, expected %d bytes", rxBuf.Len(), len(expectedResumedContent))
	}
	rxMu.Unlock()
}
