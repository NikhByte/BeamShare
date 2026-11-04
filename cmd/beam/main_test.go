package main

import (
	"bytes"
	"context"
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
	metaCount := 0
	pcReceiver.OnDataChannel(func(dc *webrtc.DataChannel) {
		receiverDC = dc
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				dataStr := string(msg.Data)
				if strings.HasPrefix(dataStr, "META:") {
					receivedChunksMu.Lock()
					metaCount++
					receivedBytesAfterMeta = 0
					receivedChunksMu.Unlock()
				} else if dataStr == "EOF" {
					close(eofReceived)
				}
			} else {
				receivedChunksMu.Lock()
				if metaCount >= 2 {
					c := make([]byte, len(msg.Data))
					copy(c, msg.Data)
					receivedChunks = append(receivedChunks, c)
					receivedBytesAfterMeta += len(msg.Data)
				}
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

				bufferedAmountLowChan := make(chan struct{}, 1)
				dcSender.SetBufferedAmountLowThreshold(512 * 1024)
				dcSender.OnBufferedAmountLow(func() {
					select {
					case bufferedAmountLowChan <- struct{}{}:
					default:
					}
				})

				buffer := make([]byte, 16*1024)
				totalSent := reqOffset

				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					if dcSender.BufferedAmount() > 1024*1024 {
						for dcSender.BufferedAmount() > 512*1024 {
							select {
							case <-ctx.Done():
								return
							case <-bufferedAmountLowChan:
							case <-time.After(10 * time.Millisecond):
							}
						}
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

				dcSender.SetBufferedAmountLowThreshold(0)
				for dcSender.BufferedAmount() > 0 {
					select {
					case <-ctx.Done():
						return
					case <-bufferedAmountLowChan:
					case <-time.After(10 * time.Millisecond):
					}
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

func TestActiveChannelsPruningOnClose(t *testing.T) {
	channelsMu.Lock()
	activeChannels = nil
	channelsMu.Unlock()
	defer func() {
		channelsMu.Lock()
		activeChannels = nil
		channelsMu.Unlock()
	}()

	api := webrtc.NewAPI()
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

	dc1Open := make(chan struct{})
	dc2Open := make(chan struct{})

	dc1, err := pc1.CreateDataChannel("live1", nil)
	require.NoError(t, err)
	dc1.OnOpen(func() {
		channelsMu.Lock()
		dc1.OnClose(func() {
			channelsMu.Lock()
			defer channelsMu.Unlock()
			var updated []*webrtc.DataChannel
			for _, ch := range activeChannels {
				if ch != dc1 {
					updated = append(updated, ch)
				}
			}
			activeChannels = updated
		})
		activeChannels = append(activeChannels, dc1)
		channelsMu.Unlock()
		close(dc1Open)
	})

	dc2, err := pc1.CreateDataChannel("live2", nil)
	require.NoError(t, err)
	dc2.OnOpen(func() {
		channelsMu.Lock()
		dc2.OnClose(func() {
			channelsMu.Lock()
			defer channelsMu.Unlock()
			var updated []*webrtc.DataChannel
			for _, ch := range activeChannels {
				if ch != dc2 {
					updated = append(updated, ch)
				}
			}
			activeChannels = updated
		})
		activeChannels = append(activeChannels, dc2)
		channelsMu.Unlock()
		close(dc2Open)
	})

	offer, err := pc1.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, pc1.SetLocalDescription(offer))
	require.NoError(t, pc2.SetRemoteDescription(offer))

	answer, err := pc2.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, pc2.SetLocalDescription(answer))
	require.NoError(t, pc1.SetRemoteDescription(answer))

	select {
	case <-dc1Open:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for dc1 open")
	}
	select {
	case <-dc2Open:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for dc2 open")
	}

	channelsMu.Lock()
	count := len(activeChannels)
	channelsMu.Unlock()
	require.Equal(t, 2, count)

	// Close dc1
	require.NoError(t, dc1.Close())

	require.Eventually(t, func() bool {
		channelsMu.Lock()
		defer channelsMu.Unlock()
		return len(activeChannels) == 1 && activeChannels[0] == dc2
	}, 5*time.Second, 50*time.Millisecond)

	// Close dc2
	require.NoError(t, dc2.Close())

	require.Eventually(t, func() bool {
		channelsMu.Lock()
		defer channelsMu.Unlock()
		return len(activeChannels) == 0
	}, 5*time.Second, 50*time.Millisecond)
}
