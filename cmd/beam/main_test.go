package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
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

	"github.com/beamshare/beam/internal/relay"
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
			senderDone   chan struct{}
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
				prevDone := senderDone
				senderDone = make(chan struct{})
				var ctx context.Context
				ctx, senderCancel = context.WithCancel(mainCtx)
				currDone := senderDone
				senderMu.Unlock()

				go func(ctx context.Context, off int64, prevDone <-chan struct{}, currDone chan struct{}) {
					defer close(currDone)
					if prevDone != nil {
						<-prevDone
					}
					select {
					case <-ctx.Done():
						return
					default:
					}

					file, err := os.Open(filePath)
					if err != nil {
						return
					}
					defer file.Close()

					if off > 0 {
						_, err = file.Seek(off, io.SeekStart)
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
					totalSent := off

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
				}(ctx, offset, prevDone, currDone)
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

func TestDownloadFile_InvalidKey(t *testing.T) {
	requestsCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsCount++
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer ts.Close()

	// 1. Invalid base64 string
	invalidB64URL := "http://example.com/?backend=" + ts.URL + "#k=invalid!base64!key!"
	err := downloadFile(invalidB64URL)
	if err == nil {
		t.Fatal("expected error for malformed base64 key, got nil")
	}
	if !strings.Contains(err.Error(), "invalid base64 key encoding") {
		t.Fatalf("expected base64 encoding error, got: %v", err)
	}

	// 2. Non-32-byte decoded key (16 bytes)
	shortKey := make([]byte, 16)
	shortKeyB64 := base64.URLEncoding.EncodeToString(shortKey)
	shortKeyURL := "http://example.com/?backend=" + ts.URL + "#k=" + shortKeyB64
	err = downloadFile(shortKeyURL)
	if err == nil {
		t.Fatal("expected error for 16-byte key, got nil")
	}
	if !strings.Contains(err.Error(), "invalid encryption key length: expected 32 bytes, got 16") {
		t.Fatalf("expected 32-byte length error, got: %v", err)
	}

	if requestsCount != 0 {
		t.Fatalf("expected 0 HTTP requests when key is invalid, got %d", requestsCount)
	}
}

func TestDownloadFile_ValidEncrypted(t *testing.T) {
	validKey := make([]byte, 32)
	for i := range validKey {
		validKey[i] = byte(i + 1)
	}
	validKeyB64 := base64.URLEncoding.EncodeToString(validKey)

	plainContent := []byte("Encrypted Relay Stream Payload Test!")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		meta := server.FileMeta{
			Name: "test_encrypted.txt",
			Size: int64(len(plainContent)),
		}
		json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		encReader, err := relay.NewEncryptingReader(bytes.NewReader(plainContent), validKey)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		io.Copy(w, encReader)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	defer os.Remove("received_test_encrypted.txt")

	encryptedURL := "http://example.com/?backend=" + ts.URL + "#k=" + validKeyB64
	err := downloadFile(encryptedURL)
	if err != nil {
		t.Fatalf("downloadFile failed for valid key: %v", err)
	}

	data, err := os.ReadFile("received_test_encrypted.txt")
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}
	if !bytes.Equal(data, plainContent) {
		t.Fatalf("expected decrypted content '%s', got '%s'", string(plainContent), string(data))
	}
}

func TestWebRTC_SenderDeduplicationAndChecksum(t *testing.T) {
	// 1. Create a test payload (300KB)
	fileSize := int64(300 * 1024)
	payload := make([]byte, fileSize)
	for i := range payload {
		payload[i] = byte((i * 31 + 17) % 256)
	}
	expectedHash := sha256.Sum256(payload)

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_webrtc_dedup.bin")
	err := os.WriteFile(filePath, payload, 0600)
	if err != nil {
		t.Fatalf("failed writing test file: %v", err)
	}
	fileName := filepath.Base(filePath)

	// 2. Setup sender signaling session
	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 10*time.Second)
	if err != nil {
		t.Fatalf("failed creating sender session: %v", err)
	}
	defer senderSession.Close()

	// Attach data channel handler (mirroring cmd/beam/main.go logic)
	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		var (
			senderMu     sync.Mutex
			cancelSender context.CancelFunc
			senderDone   chan struct{}
		)

		dc.OnClose(func() {
			senderMu.Lock()
			if cancelSender != nil {
				cancelSender()
				cancelSender = nil
			}
			senderMu.Unlock()
		})

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				dataStr := string(msg.Data)
				if strings.HasPrefix(dataStr, "OFFSET:") {
					parts := strings.SplitN(dataStr, ":", 2)
					var offset int64
					if len(parts) == 2 {
						offset, _ = strconv.ParseInt(parts[1], 10, 64)
					}

					senderMu.Lock()
					if cancelSender != nil {
						cancelSender()
					}
					prevDone := senderDone
					senderDone = make(chan struct{})
					ctx, cancel := context.WithCancel(context.Background())
					cancelSender = cancel
					currDone := senderDone
					senderMu.Unlock()

					go func(ctx context.Context, offset int64, prevDone <-chan struct{}, currDone chan struct{}) {
						defer close(currDone)
						if prevDone != nil {
							<-prevDone
						}
						select {
						case <-ctx.Done():
							return
						default:
						}

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

						readBuf := make([]byte, 32*1024)
						totalSent := offset

						for {
							select {
							case <-ctx.Done():
								return
							default:
							}

							select {
							case <-bufferedAmountLowChan:
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

							n, readErr := file.Read(readBuf)
							if n > 0 {
								select {
								case <-ctx.Done():
									return
								default:
								}
								chunk := make([]byte, n)
								copy(chunk, readBuf[:n])
								errSend := dc.Send(chunk)
								if errSend != nil {
									return
								}
								totalSent += int64(n)
							}
							if readErr != nil {
								break
							}
						}

						dc.SetBufferedAmountLowThreshold(0)
						eofTimeout := time.After(5 * time.Second)
						for dc.BufferedAmount() > 0 {
							select {
							case <-ctx.Done():
								return
							case <-eofTimeout:
								break
							case <-bufferedAmountLowChan:
							case <-time.After(50 * time.Millisecond):
							}
							if dc.BufferedAmount() == 0 {
								break
							}
						}

						select {
						case <-ctx.Done():
							return
						default:
						}

						_ = dc.SendText("EOF")
					}(ctx, offset, prevDone, currDone)
				}
			}
		})
	}

	// 3. Setup receiver peer connection
	rxPC, err := signaling.NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("failed creating rxPC: %v", err)
	}
	defer rxPC.Close()

	var rxMu sync.Mutex
	var rxCandidates []webrtc.ICECandidateInit
	rxPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			cand := c.ToJSON()
			rxMu.Lock()
			rxCandidates = append(rxCandidates, cand)
			rxMu.Unlock()
			_ = senderSession.AddICECandidate(cand)
		}
	})

	var (
		receivedBuf  bytes.Buffer
		receivedMeta string
		eofChan      = make(chan struct{})
		rxDataChan   = make(chan *webrtc.DataChannel, 1)
	)

	rxPC.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			rxMu.Lock()
			defer rxMu.Unlock()
			if msg.IsString {
				str := string(msg.Data)
				if strings.HasPrefix(str, "META:") {
					receivedMeta = str
					// Clear buffer on new META signal
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
			}
		})
		rxDataChan <- dc
	})

	// Handshake
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	if err != nil {
		t.Fatalf("CreateOffer failed: %v", err)
	}

	err = rxPC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  senderSession.RawOffer(),
	})
	if err != nil {
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
	err = rxPC.SetLocalDescription(answer)
	if err != nil {
		t.Fatalf("SetLocalDescription failed: %v", err)
	}

	answerBytes, err := json.Marshal(answer)
	if err != nil {
		t.Fatalf("Marshal answer failed: %v", err)
	}

	err = senderSession.ProvideAnswer(string(answerBytes))
	if err != nil {
		t.Fatalf("ProvideAnswer failed: %v", err)
	}

	rxMu.Lock()
	for _, cand := range rxCandidates {
		_ = senderSession.AddICECandidate(cand)
	}
	rxMu.Unlock()

	var rxDC *webrtc.DataChannel
	select {
	case rxDC = <-rxDataChan:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for rxDC")
	}

	// Wait until channel open
	for rxDC.ReadyState() != webrtc.DataChannelStateOpen {
		time.Sleep(10 * time.Millisecond)
	}

	// 4. Send duplicate OFFSET messages to test goroutine cancellation/deduplication
	_ = rxDC.SendText("OFFSET:0")
	time.Sleep(5 * time.Millisecond)
	// Duplicate OFFSET signal triggers cancellation of previous goroutine and starts fresh
	_ = rxDC.SendText("OFFSET:0")

	// 5. Wait for EOF and verify SHA-256 payload checksum match
	select {
	case <-eofChan:
		rxMu.Lock()
		defer rxMu.Unlock()
		actualBytes := receivedBuf.Bytes()
		actualHash := sha256.Sum256(actualBytes)

		if actualHash != expectedHash {
			t.Fatalf("SHA-256 checksum mismatch!\nExpected len %d, got len %d\nExpected SHA256: %x\nActual SHA256:   %x",
				len(payload), len(actualBytes), expectedHash, actualHash)
		}
		expectedMeta := fmt.Sprintf("META:%s:%d", fileName, fileSize)
		if receivedMeta != expectedMeta {
			t.Fatalf("Expected meta %s, got %s", expectedMeta, receivedMeta)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for P2P WebRTC transfer EOF")
	}
}
