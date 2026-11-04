package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/signaling"
	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupConnectedDataChannels creates a pair of connected WebRTC PeerConnections
// with an open DataChannel between sender and receiver for testing.
func setupConnectedDataChannels(t *testing.T) (*webrtc.PeerConnection, *webrtc.PeerConnection, *webrtc.DataChannel, *webrtc.DataChannel) {
	t.Helper()

	api := signaling.NewWebRTCAPI()
	senderPC, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)

	receiverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)

	var mu sync.Mutex
	senderPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			mu.Lock()
			_ = receiverPC.AddICECandidate(c.ToJSON())
			mu.Unlock()
		}
	})

	receiverPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			mu.Lock()
			_ = senderPC.AddICECandidate(c.ToJSON())
			mu.Unlock()
		}
	})

	rxDCChan := make(chan *webrtc.DataChannel, 1)
	receiverPC.OnDataChannel(func(dc *webrtc.DataChannel) {
		rxDCChan <- dc
	})

	ordered := true
	senderDC, err := senderPC.CreateDataChannel("beam-test", &webrtc.DataChannelInit{
		Ordered: &ordered,
	})
	require.NoError(t, err)

	offer, err := senderPC.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, senderPC.SetLocalDescription(offer))
	require.NoError(t, receiverPC.SetRemoteDescription(offer))

	answer, err := receiverPC.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, receiverPC.SetLocalDescription(answer))
	require.NoError(t, senderPC.SetRemoteDescription(answer))

	var receiverDC *webrtc.DataChannel
	select {
	case receiverDC = <-rxDCChan:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel")
	}

	senderOpen := make(chan struct{})
	senderDC.OnOpen(func() {
		close(senderOpen)
	})

	receiverOpen := make(chan struct{})
	receiverDC.OnOpen(func() {
		close(receiverOpen)
	})

	select {
	case <-senderOpen:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for sender DataChannel open")
	}

	select {
	case <-receiverOpen:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel open")
	}

	return senderPC, receiverPC, senderDC, receiverDC
}

// setupSenderDataChannelHandler simulates the session.OnOpen logic from main.go
func setupSenderDataChannelHandler(dc *webrtc.DataChannel, filePath string, fileName string, fileSize int64, activeSendersCount *int32) {
	var senderMu sync.Mutex
	var activeCancel context.CancelFunc

	dc.OnClose(func() {
		senderMu.Lock()
		if activeCancel != nil {
			activeCancel()
			activeCancel = nil
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
			if activeCancel != nil {
				activeCancel()
			}
			transferCtx, cancel := context.WithCancel(context.Background())
			activeCancel = cancel
			senderMu.Unlock()

			go func(ctx context.Context) {
				if activeSendersCount != nil {
					atomic.AddInt32(activeSendersCount, 1)
					defer atomic.AddInt32(activeSendersCount, -1)
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

				metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
				senderMu.Lock()
				if ctx.Err() != nil {
					senderMu.Unlock()
					return
				}
				errSend := dc.SendText(metaHeader)
				senderMu.Unlock()
				if errSend != nil {
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

				buffer := make([]byte, 64*1000) // 64KB chunk size
				totalSent := offset

				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					if dc.BufferedAmount() > 512*1024 {
						for dc.BufferedAmount() > 512*1024 {
							select {
							case <-ctx.Done():
								return
							case <-bufferedAmountLowChan:
							case <-time.After(10 * time.Millisecond):
							}
						}
					}

					select {
					case <-ctx.Done():
						return
					default:
					}

					n, errRead := file.Read(buffer)
					if n > 0 {
						senderMu.Lock()
						if ctx.Err() != nil {
							senderMu.Unlock()
							return
						}
						chunkCopy := make([]byte, n)
						copy(chunkCopy, buffer[:n])
						errSend := dc.Send(chunkCopy)
						senderMu.Unlock()
						if errSend != nil {
							return
						}
						totalSent += int64(n)
					}
					if errRead != nil {
						break
					}
				}

				dc.SetBufferedAmountLowThreshold(0)
				for dc.BufferedAmount() > 0 {
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

				dc.SendText("EOF")
			}(transferCtx)
		}
	})
}

func TestWebRTCTransfer_DuplicateOffsetCancellation(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "large_test.bin")
	fileData := make([]byte, 500*1024) // 500KB test data
	for i := range fileData {
		fileData[i] = byte(i % 256)
	}
	require.NoError(t, os.WriteFile(testFile, fileData, 0600))

	senderPC, receiverPC, senderDC, receiverDC := setupConnectedDataChannels(t)
	defer senderPC.Close()
	defer receiverPC.Close()

	var activeSenders int32
	setupSenderDataChannelHandler(senderDC, testFile, "large_test.bin", int64(len(fileData)), &activeSenders)

	var rxMu sync.Mutex
	var receivedBytes bytes.Buffer
	eofCh := make(chan struct{}, 1)
	metaCount := 0

	receiverDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		rxMu.Lock()
		defer rxMu.Unlock()
		if msg.IsString {
			dataStr := string(msg.Data)
			if strings.HasPrefix(dataStr, "META:") {
				metaCount++
				if metaCount == 2 {
					receivedBytes.Reset()
				}
			} else if dataStr == "EOF" {
				select {
				case eofCh <- struct{}{}:
				default:
				}
			}
		} else {
			if metaCount >= 2 {
				receivedBytes.Write(msg.Data)
			}
		}
	})

	// Send initial OFFSET:0
	require.NoError(t, receiverDC.SendText("OFFSET:0"))

	// Wait briefly so transfer begins
	time.Sleep(5 * time.Millisecond)

	const offset int64 = 102400
	require.NoError(t, receiverDC.SendText(fmt.Sprintf("OFFSET:%d", offset)))

	select {
	case <-eofCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for EOF after duplicate OFFSET")
	}

	// Verify replacement sender completed and active senders count returns to 0
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&activeSenders) == 0
	}, 2*time.Second, 20*time.Millisecond)

	rxMu.Lock()
	gotBytes := receivedBytes.Bytes()
	rxMu.Unlock()

	expectedData := fileData[offset:]
	assert.Equal(t, len(expectedData), len(gotBytes), "received byte count mismatch")
	assert.Equal(t, expectedData, gotBytes, "received bytes corrupted or duplicated")
}

func TestWebRTCTransfer_BufferCloningAndIntegrity(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "integrity_test.bin")

	// 1MB file size with non-repeating sequence
	fileSize := 1024 * 1024
	fileData := make([]byte, fileSize)
	for i := range fileData {
		fileData[i] = byte((i*17 + 31) % 256)
	}
	require.NoError(t, os.WriteFile(testFile, fileData, 0600))

	senderPC, receiverPC, senderDC, receiverDC := setupConnectedDataChannels(t)
	defer senderPC.Close()
	defer receiverPC.Close()

	setupSenderDataChannelHandler(senderDC, testFile, "integrity_test.bin", int64(fileSize), nil)

	var rxMu sync.Mutex
	var receivedBytes bytes.Buffer
	eofCh := make(chan struct{})

	receiverDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		rxMu.Lock()
		defer rxMu.Unlock()
		if msg.IsString {
			if string(msg.Data) == "EOF" {
				close(eofCh)
			}
		} else {
			receivedBytes.Write(msg.Data)
		}
	})

	require.NoError(t, receiverDC.SendText("OFFSET:0"))

	select {
	case <-eofCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for EOF")
	}

	rxMu.Lock()
	gotBytes := receivedBytes.Bytes()
	rxMu.Unlock()

	require.Equal(t, len(fileData), len(gotBytes), "received data size should match original file")
	assert.Equal(t, fileData, gotBytes, "received data payload must match original file exactly without chunk corruption")
}

func TestWebRTCTransfer_BackpressureAndEOFDrain(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "backpressure_test.bin")
	fileData := make([]byte, 200*1024) // 200KB
	for i := range fileData {
		fileData[i] = byte(i % 256)
	}
	require.NoError(t, os.WriteFile(testFile, fileData, 0600))

	senderPC, receiverPC, senderDC, receiverDC := setupConnectedDataChannels(t)
	defer senderPC.Close()
	defer receiverPC.Close()

	setupSenderDataChannelHandler(senderDC, testFile, "backpressure_test.bin", int64(len(fileData)), nil)

	eofCh := make(chan struct{})
	var rxBytes int64

	receiverDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			if string(msg.Data) == "EOF" {
				close(eofCh)
			}
		} else {
			atomic.AddInt64(&rxBytes, int64(len(msg.Data)))
		}
	})

	require.NoError(t, receiverDC.SendText("OFFSET:0"))

	select {
	case <-eofCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for EOF signal during backpressure drain test")
	}

	assert.Equal(t, int64(len(fileData)), atomic.LoadInt64(&rxBytes))
}
