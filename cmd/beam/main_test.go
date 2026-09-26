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
	"github.com/pion/ice/v2"
	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/require"
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

func TestP2PSender_ContextCancellationAndBackpressure(t *testing.T) {
	// Create dummy test file
	tmpFile, err := os.CreateTemp("", "beam_test_*.bin")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	testData := make([]byte, 10*1024*1024) // 10MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	_, err = tmpFile.Write(testData)
	require.NoError(t, err)
	tmpFile.Close()

	filePath := tmpFile.Name()
	fileName := filepath.Base(filePath)
	fileSize := int64(len(testData))

	m := &webrtc.MediaEngine{}
	require.NoError(t, m.RegisterDefaultCodecs())
	settingEngine := webrtc.SettingEngine{}
	settingEngine.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	settingEngine.SetIncludeLoopbackCandidate(true)
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithSettingEngine(settingEngine))

	pc1, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc1.Close()

	pc2, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc2.Close()

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

	dc1, err := pc1.CreateDataChannel("beam-test", nil)
	require.NoError(t, err)

	dc2Chan := make(chan *webrtc.DataChannel, 1)
	pc2.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc2Chan <- dc
	})

	offer, err := pc1.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, pc1.SetLocalDescription(offer))
	require.NoError(t, pc2.SetRemoteDescription(offer))

	answer, err := pc2.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, pc2.SetLocalDescription(answer))
	require.NoError(t, pc1.SetRemoteDescription(answer))

	var receiverDC *webrtc.DataChannel
	select {
	case receiverDC = <-dc2Chan:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for data channel on pc2")
	}

	receiverDCOpen := make(chan struct{})
	receiverDC.OnOpen(func() {
		select {
		case <-receiverDCOpen:
		default:
			close(receiverDCOpen)
		}
	})
	if receiverDC.ReadyState() == webrtc.DataChannelStateOpen {
		select {
		case <-receiverDCOpen:
		default:
			close(receiverDCOpen)
		}
	}

	dc1Open := make(chan struct{})
	dc1.OnOpen(func() {
		close(dc1Open)
	})

	select {
	case <-dc1Open:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for dc1 to open")
	}

	select {
	case <-receiverDCOpen:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for receiverDC to open")
	}

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	var (
		senderCancel context.CancelFunc
		senderMu     sync.Mutex
	)

	dc1.OnClose(func() {
		senderMu.Lock()
		if senderCancel != nil {
			senderCancel()
			senderCancel = nil
		}
		senderMu.Unlock()
	})

	dc1.OnMessage(func(msg webrtc.DataChannelMessage) {
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
				var senderCtx context.Context
				senderCtx, senderCancel = context.WithCancel(mainCtx)
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

					select {
					case <-ctx.Done():
						return
					default:
					}

					metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
					if errSend := dc1.SendText(metaHeader); errSend != nil {
						return
					}

					bufferedAmountLowChan := make(chan struct{}, 1)
					dc1.SetBufferedAmountLowThreshold(512 * 1024)
					dc1.OnBufferedAmountLow(func() {
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

						if dc1.BufferedAmount() > 1024*1024 {
							ticker := time.NewTicker(10 * time.Millisecond)
							for dc1.BufferedAmount() > 512*1024 {
								select {
								case <-ctx.Done():
									ticker.Stop()
									return
								case <-bufferedAmountLowChan:
								case <-ticker.C:
								}
							}
							ticker.Stop()
						}

						select {
						case <-ctx.Done():
							return
						default:
						}

						n, err := file.Read(buffer)
						if n > 0 {
							chunk := make([]byte, n)
							copy(chunk, buffer[:n])
							errSend := dc1.Send(chunk)
							if errSend != nil {
								return
							}
							totalSent += int64(n)
						}
						if err != nil {
							break
						}
					}

					dc1.SetBufferedAmountLowThreshold(0)
					if dc1.BufferedAmount() > 0 {
						ticker := time.NewTicker(10 * time.Millisecond)
						for dc1.BufferedAmount() > 0 {
							select {
							case <-ctx.Done():
								ticker.Stop()
								return
							case <-bufferedAmountLowChan:
							case <-ticker.C:
							}
						}
						ticker.Stop()
					}

					select {
					case <-ctx.Done():
						return
					default:
					}

					dc1.SendText("EOF")
				}(senderCtx)
			}
		}
	})

	var mu sync.Mutex
	var receivedBytes bytes.Buffer
	eofCh := make(chan struct{})

	receiverDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			str := string(msg.Data)
			if strings.HasPrefix(str, "META:") {
				mu.Lock()
				receivedBytes.Reset()
				mu.Unlock()
			} else if str == "EOF" {
				select {
				case <-eofCh:
				default:
					close(eofCh)
				}
			}
		} else {
			mu.Lock()
			receivedBytes.Write(msg.Data)
			mu.Unlock()
		}
	})

	// Send duplicate rapid OFFSET requests to test cancellation & restart
	require.NoError(t, receiverDC.SendText("OFFSET:0"))
	time.Sleep(5 * time.Millisecond)
	require.NoError(t, receiverDC.SendText("OFFSET:0"))
	time.Sleep(5 * time.Millisecond)

	mu.Lock()
	receivedBytes.Reset()
	mu.Unlock()

	require.NoError(t, receiverDC.SendText("OFFSET:0"))

	select {
	case <-eofCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for transfer completion EOF")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, len(testData), receivedBytes.Len())
	require.Equal(t, testData, receivedBytes.Bytes())
}

