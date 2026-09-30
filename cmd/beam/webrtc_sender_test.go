package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/server"
	"github.com/beamshare/beam/internal/signaling"
	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupWebRTCPair(t *testing.T, srv *server.Server, testFilePath string, testFileName string, testFileSize int64, onRxMsg func(dc *webrtc.DataChannel, msg webrtc.DataChannelMessage)) (*signaling.Session, *webrtc.PeerConnection, *webrtc.DataChannel, chan struct{}) {
	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 10*time.Second)
	require.NoError(t, err)

	senderTxReady := make(chan struct{})

	// Setup data channel handler (mirroring runSend session.OnOpen logic)
	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		close(senderTxReady)

		var (
			uploadFile *os.File
			uploadName string
			uploadSize int64
			uploaded   int64
			uploadStat time.Time

			senderMu     sync.Mutex
			senderCancel context.CancelFunc
			senderDone   chan struct{}
		)
		_ = uploadSize

		dc.OnClose(func() {
			senderMu.Lock()
			if senderCancel != nil {
				senderCancel()
			}
			senderMu.Unlock()
		})

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				dataStr := string(msg.Data)
				if strings.HasPrefix(dataStr, "UPLOAD_META:") {
					parts := strings.SplitN(dataStr, ":", 3)
					if len(parts) == 3 {
						name := parts[1]
						size, _ := int64(0), int64(0)
						uploadName = "received_" + filepath.Base(name)
						uploadSize = size
						uploaded = 0
						uploadStat = time.Now()
						uploadFile, _ = os.OpenFile(uploadName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
					}
				} else if dataStr == "UPLOAD_EOF" {
					if uploadFile != nil {
						uploadFile.Close()
						_ = uploadStat
						uploadFile = nil
					}
				} else if strings.HasPrefix(dataStr, "OFFSET:") {
					parts := strings.SplitN(dataStr, ":", 2)
					var offset int64
					if len(parts) == 2 {
						var errParse error
						_, errParse = fmt.Sscanf(parts[1], "%d", &offset)
						_ = errParse
					}

					senderMu.Lock()
					if senderCancel != nil {
						senderCancel()
						prevDone := senderDone
						senderMu.Unlock()
						if prevDone != nil {
							<-prevDone
						}
						senderMu.Lock()
					}

					ctx, cancel := context.WithCancel(context.Background())
					done := make(chan struct{})
					senderCancel = cancel
					senderDone = done
					senderMu.Unlock()

					go func(ctx context.Context, done chan struct{}) {
						defer func() {
							t.Logf("[Sender] Goroutine exiting for offset %d, ctx err: %v", offset, ctx.Err())
							close(done)
						}()
						t.Logf("[Sender] Started streaming for offset %d", offset)
						file, err := os.Open(testFilePath)
						if err != nil {
							t.Logf("[Sender] Error opening file: %v", err)
							return
						}
						defer file.Close()

						if offset > 0 {
							_, err = file.Seek(offset, 0)
							if err != nil {
								t.Logf("[Sender] Error seeking: %v", err)
								return
							}
						}

						if ctx.Err() != nil {
							return
						}

						metaHeader := fmt.Sprintf("META:%s:%d", testFileName, testFileSize)
						if errSend := dc.SendText(metaHeader); errSend != nil {
							t.Logf("[Sender] Error sending meta: %v", errSend)
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
							if ctx.Err() != nil {
								t.Logf("[Sender] ctx canceled during read loop")
								return
							}

							if dc.BufferedAmount() > 1024*1024 {
								t.Logf("[Sender] Backpressure pause: BufferedAmount=%d", dc.BufferedAmount())
								ticker := time.NewTicker(10 * time.Millisecond)
								for dc.BufferedAmount() > 512*1024 {
									select {
									case <-ctx.Done():
										ticker.Stop()
										return
									case <-bufferedAmountLowChan:
									case <-ticker.C:
									}
								}
								ticker.Stop()
								t.Logf("[Sender] Backpressure resumed: BufferedAmount=%d", dc.BufferedAmount())
							}

							if ctx.Err() != nil {
								return
							}

							n, errRead := file.Read(buffer)
							if n > 0 {
								chunk := make([]byte, n)
								copy(chunk, buffer[:n])

								errSend := dc.Send(chunk)
								if errSend != nil {
									t.Logf("[Sender] Error sending chunk: %v", errSend)
									return
								}
								time.Sleep(2 * time.Millisecond)
								totalSent += int64(n)
							}
							if errRead != nil {
								t.Logf("[Sender] EOF reading file, totalSent=%d", totalSent)
								break
							}
						}

						if ctx.Err() != nil {
							return
						}

						dc.SetBufferedAmountLowThreshold(0)
						if dc.BufferedAmount() > 0 {
							t.Logf("[Sender] Waiting for zero buffer, current=%d", dc.BufferedAmount())
							eofCtx, eofCancel := context.WithTimeout(ctx, 5*time.Second)
							defer eofCancel()
							ticker := time.NewTicker(10 * time.Millisecond)
							defer ticker.Stop()

							for dc.BufferedAmount() > 0 {
								select {
								case <-eofCtx.Done():
									t.Logf("[Sender] EOF flush timeout/cancel")
									goto sendEOF
								case <-bufferedAmountLowChan:
								case <-ticker.C:
								}
							}
						}

					sendEOF:
						if ctx.Err() == nil {
							t.Logf("[Sender] Sending EOF")
							_ = dc.SendText("EOF")
						}
					}(ctx, done)
				}
			} else {
				if uploadFile != nil {
					n, _ := uploadFile.Write(msg.Data)
					uploaded += int64(n)
				}
			}
		})
	}

	rxPC, err := signaling.NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)

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
		if dc.ReadyState() == webrtc.DataChannelStateOpen {
			select {
			case <-rxOpenCh:
			default:
				close(rxOpenCh)
			}
		}
		if onRxMsg != nil {
			dc.OnMessage(func(msg webrtc.DataChannelMessage) {
				onRxMsg(dc, msg)
			})
		}
		rxDataChannelCh <- dc
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	require.NoError(t, err)

	err = rxPC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  senderSession.RawOffer(),
	})
	require.NoError(t, err)

	for _, cand := range senderSession.GetCandidates() {
		_ = rxPC.AddICECandidate(cand)
	}

	senderSession.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = rxPC.AddICECandidate(c.ToJSON())
		}
	})

	answer, err := rxPC.CreateAnswer(nil)
	require.NoError(t, err)
	err = rxPC.SetLocalDescription(answer)
	require.NoError(t, err)

	answerBytes, err := json.Marshal(answer)
	require.NoError(t, err)

	err = senderSession.ProvideAnswer(string(answerBytes))
	require.NoError(t, err)

	mu.Lock()
	for _, cand := range rxCandidates {
		_ = senderSession.AddICECandidate(cand)
	}
	mu.Unlock()

	var rxDC *webrtc.DataChannel
	select {
	case rxDC = <-rxDataChannelCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel")
	}

	select {
	case <-rxOpenCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel open")
	}

	select {
	case <-senderTxReady:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for sender DataChannel open")
	}

	return senderSession, rxPC, rxDC, senderTxReady
}

func TestWebRTC_DuplicateOffsetCancellation(t *testing.T) {
	srv, err := server.New("", 0)
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	fileSize := 1024 * 1024 // 1MB
	testPayload := make([]byte, fileSize)
	for i := range testPayload {
		testPayload[i] = byte((i * 31) % 256)
	}

	hasher := sha256.New()
	hasher.Write(testPayload)
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "dup_offset_test.dat")
	err = os.WriteFile(filePath, testPayload, 0644)
	require.NoError(t, err)

	var mu sync.Mutex
	var receivedBuf bytes.Buffer
	var receivedMeta []string
	eofReceived := make(chan struct{})
	chunkCount := 0
	duplicateSent := false

	onRxMsg := func(dc *webrtc.DataChannel, msg webrtc.DataChannelMessage) {
		mu.Lock()
		defer mu.Unlock()

		if msg.IsString {
			str := string(msg.Data)
			t.Logf("[Rx] Text message received: %s", str)
			if strings.HasPrefix(str, "META:") {
				receivedMeta = append(receivedMeta, str)
				receivedBuf.Reset()
			} else if str == "EOF" {
				t.Logf("[Rx] EOF received!")
				select {
				case <-eofReceived:
				default:
					close(eofReceived)
				}
			}
		} else {
			chunkCount++
			receivedBuf.Write(msg.Data)
			t.Logf("[Rx] Chunk %d received (%d bytes)", chunkCount, len(msg.Data))
			if chunkCount == 2 && !duplicateSent {
				duplicateSent = true
				t.Logf("[Rx] Sending duplicate OFFSET:0 signal")
				go func() {
					_ = dc.SendText("OFFSET:0")
				}()
			}
		}
	}

	senderSession, rxPC, rxDC, _ := setupWebRTCPair(t, srv, filePath, "dup_offset_test.dat", int64(fileSize), onRxMsg)
	defer senderSession.Close()
	defer rxPC.Close()

	// Send initial OFFSET:0
	err = rxDC.SendText("OFFSET:0")
	require.NoError(t, err)

	select {
	case <-eofReceived:
		mu.Lock()
		defer mu.Unlock()

		dlHasher := sha256.New()
		dlHasher.Write(receivedBuf.Bytes())
		actualHash := hex.EncodeToString(dlHasher.Sum(nil))

		assert.Equal(t, fileSize, receivedBuf.Len(), "Received file size should match total payload size")
		assert.Equal(t, expectedHash, actualHash, "Hash of file received after duplicate OFFSET must match expected hash")
		assert.Equal(t, 2, len(receivedMeta), "Should receive two META headers from initial and duplicate stream")
		assert.True(t, duplicateSent, "Duplicate OFFSET signal should have been sent during stream")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for transfer completion after duplicate OFFSET")
	}
}

func TestWebRTC_IndependentChunkSlicesAndBackpressure(t *testing.T) {
	srv, err := server.New("", 0)
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	fileSize := 512 * 1024 // 512KB
	testPayload := make([]byte, fileSize)
	for i := range testPayload {
		testPayload[i] = byte((i * 13) % 256)
	}

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "chunk_test.dat")
	err = os.WriteFile(filePath, testPayload, 0644)
	require.NoError(t, err)

	var mu sync.Mutex
	var receivedBuf bytes.Buffer
	eofReceived := make(chan struct{})
	var chunkPointers [][]byte

	onRxMsg := func(dc *webrtc.DataChannel, msg webrtc.DataChannelMessage) {
		mu.Lock()
		defer mu.Unlock()

		if msg.IsString {
			if string(msg.Data) == "EOF" {
				select {
				case <-eofReceived:
				default:
					close(eofReceived)
				}
			}
		} else {
			chunkPointers = append(chunkPointers, msg.Data)
			receivedBuf.Write(msg.Data)
		}
	}

	senderSession, rxPC, rxDC, _ := setupWebRTCPair(t, srv, filePath, "chunk_test.dat", int64(fileSize), onRxMsg)
	defer senderSession.Close()
	defer rxPC.Close()

	err = rxDC.SendText("OFFSET:0")
	require.NoError(t, err)

	select {
	case <-eofReceived:
		mu.Lock()
		defer mu.Unlock()

		assert.Equal(t, testPayload, receivedBuf.Bytes())
		assert.True(t, len(chunkPointers) > 1, "Should receive multiple chunks")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for chunked transfer completion")
	}
}

func TestWebRTC_EOFFlushZeroBuffer(t *testing.T) {
	srv, err := server.New("", 0)
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	fileSize := 1024 // Small file 1KB
	testPayload := make([]byte, fileSize)

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "eof_test.dat")
	err = os.WriteFile(filePath, testPayload, 0644)
	require.NoError(t, err)

	eofReceived := make(chan struct{})
	onRxMsg := func(dc *webrtc.DataChannel, msg webrtc.DataChannelMessage) {
		if msg.IsString && string(msg.Data) == "EOF" {
			select {
			case <-eofReceived:
			default:
				close(eofReceived)
			}
		}
	}

	senderSession, rxPC, rxDC, _ := setupWebRTCPair(t, srv, filePath, "eof_test.dat", int64(fileSize), onRxMsg)
	defer senderSession.Close()
	defer rxPC.Close()

	err = rxDC.SendText("OFFSET:0")
	require.NoError(t, err)

	select {
	case <-eofReceived:
		// EOF received successfully without hanging on zero buffer
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for EOF token on zero-buffer flush")
	}
}

func TestWebRTC_BackpressureThresholdPauseResume(t *testing.T) {
	srv, err := server.New("", 0)
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// 2MB payload to ensure BufferedAmount exceeds 1MB threshold and triggers backpressure pause
	fileSize := 2 * 1024 * 1024
	testPayload := make([]byte, fileSize)
	for i := range testPayload {
		testPayload[i] = byte((i * 17) % 256)
	}

	hasher := sha256.New()
	hasher.Write(testPayload)
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "backpressure_test.dat")
	err = os.WriteFile(filePath, testPayload, 0644)
	require.NoError(t, err)

	var mu sync.Mutex
	var receivedBuf bytes.Buffer
	eofReceived := make(chan struct{})

	onRxMsg := func(dc *webrtc.DataChannel, msg webrtc.DataChannelMessage) {
		mu.Lock()
		defer mu.Unlock()

		if msg.IsString {
			if string(msg.Data) == "EOF" {
				select {
				case <-eofReceived:
				default:
					close(eofReceived)
				}
			}
		} else {
			receivedBuf.Write(msg.Data)
			// Introduce small delay to simulate slow receiver consuming buffer
			time.Sleep(500 * time.Microsecond)
		}
	}

	senderSession, rxPC, rxDC, _ := setupWebRTCPair(t, srv, filePath, "backpressure_test.dat", int64(fileSize), onRxMsg)
	defer senderSession.Close()
	defer rxPC.Close()

	err = rxDC.SendText("OFFSET:0")
	require.NoError(t, err)

	select {
	case <-eofReceived:
		mu.Lock()
		defer mu.Unlock()

		dlHasher := sha256.New()
		dlHasher.Write(receivedBuf.Bytes())
		actualHash := hex.EncodeToString(dlHasher.Sum(nil))

		assert.Equal(t, fileSize, receivedBuf.Len())
		assert.Equal(t, expectedHash, actualHash)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for backpressure transfer completion")
	}
}
