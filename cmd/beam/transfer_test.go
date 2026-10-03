package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/signaling"
	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupPeerConnectionPair(t *testing.T) (*webrtc.PeerConnection, *webrtc.PeerConnection, *webrtc.DataChannel, *webrtc.DataChannel) {
	api := signaling.NewWebRTCAPI()

	pcSender, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)

	pcReceiver, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)

	ordered := true
	dcSender, err := pcSender.CreateDataChannel("file-transfer", &webrtc.DataChannelInit{
		Ordered: &ordered,
	})
	require.NoError(t, err)

	rxDCChan := make(chan *webrtc.DataChannel, 1)
	pcReceiver.OnDataChannel(func(dc *webrtc.DataChannel) {
		rxDCChan <- dc
	})

	// Register ICE candidate exchange
	pcSender.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = pcReceiver.AddICECandidate(c.ToJSON())
		}
	})
	pcReceiver.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = pcSender.AddICECandidate(c.ToJSON())
		}
	})

	offer, err := pcSender.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, pcSender.SetLocalDescription(offer))
	require.NoError(t, pcReceiver.SetRemoteDescription(offer))

	answer, err := pcReceiver.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, pcReceiver.SetLocalDescription(answer))
	require.NoError(t, pcSender.SetRemoteDescription(answer))

	var rxDC *webrtc.DataChannel
	select {
	case rxDC = <-rxDCChan:
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for receiver DataChannel")
	}

	return pcSender, pcReceiver, dcSender, rxDC
}

func TestSingleFlightDuplicateOffset(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "testdata.bin")

	// Create test file with 256KB of data
	fileData := make([]byte, 256*1024)
	for i := range fileData {
		fileData[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(filePath, fileData, 0644))

	fileName := filepath.Base(filePath)
	fileSize := int64(len(fileData))

	pcSender, pcReceiver, dcSender, rxDC := setupPeerConnectionPair(t)
	defer pcSender.Close()
	defer pcReceiver.Close()

	var (
		transferCtxMu  sync.Mutex
		transferCancel context.CancelFunc
	)

	dcSender.OnClose(func() {
		transferCtxMu.Lock()
		if transferCancel != nil {
			transferCancel()
			transferCancel = nil
		}
		transferCtxMu.Unlock()
	})

	dcSender.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			dataStr := string(msg.Data)
			if strings.HasPrefix(dataStr, "OFFSET:") {
				parts := strings.SplitN(dataStr, ":", 2)
				var reqOffset int64
				if len(parts) == 2 {
					reqOffset, _ = strconv.ParseInt(parts[1], 10, 64)
				}

				transferCtxMu.Lock()
				if transferCancel != nil {
					transferCancel()
				}
				var transferCtx context.Context
				transferCtx, transferCancel = context.WithCancel(context.Background())
				transferCtxMu.Unlock()

				go func(ctx context.Context, offset int64) {
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
					if errSend := dcSender.SendText(metaHeader); errSend != nil {
						return
					}

					buffer := make([]byte, 65535)
					totalSent := offset

					for {
						select {
						case <-ctx.Done():
							return
						default:
						}

						for dcSender.BufferedAmount() > 1024*1024 {
							select {
							case <-ctx.Done():
								return
							case <-time.After(5 * time.Millisecond):
							}
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
						}
						if err != nil {
							break
						}
					}

					for dcSender.BufferedAmount() > 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(5 * time.Millisecond):
						}
					}

					if ctx.Err() != nil {
						return
					}

					_ = dcSender.SendText("EOF")
				}(transferCtx, reqOffset)
			}
		}
	})

	var receivedMutex sync.Mutex
	var metaCount int
	var chunksAfterSecondMeta [][]byte
	transferDone := make(chan struct{})
	firstChunkChan := make(chan struct{}, 1)

	rxDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		receivedMutex.Lock()
		defer receivedMutex.Unlock()
		if msg.IsString {
			str := string(msg.Data)
			if strings.HasPrefix(str, "META:") {
				metaCount++
			} else if str == "EOF" {
				close(transferDone)
			}
		} else {
			chunk := make([]byte, len(msg.Data))
			copy(chunk, msg.Data)
			if metaCount == 1 {
				select {
				case firstChunkChan <- struct{}{}:
				default:
				}
			} else if metaCount == 2 {
				chunksAfterSecondMeta = append(chunksAfterSecondMeta, chunk)
			}
		}
	})

	// Wait for connected state
	require.Eventually(t, func() bool {
		return rxDC.ReadyState() == webrtc.DataChannelStateOpen && dcSender.ReadyState() == webrtc.DataChannelStateOpen
	}, 5*time.Second, 10*time.Millisecond)

	// Receiver sends initial OFFSET:0
	require.NoError(t, rxDC.SendText("OFFSET:0"))

	// Wait for 1st chunk to be received
	select {
	case <-firstChunkChan:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for 1st chunk")
	}

	// Receiver sends duplicate OFFSET:65536 after receiving 1st chunk
	require.NoError(t, rxDC.SendText("OFFSET:65536"))

	select {
	case <-transferDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Transfer timed out waiting for EOF")
	}

	receivedMutex.Lock()
	defer receivedMutex.Unlock()

	assert.Equal(t, 2, metaCount, "Should receive 2 META headers (one for initial stream, one for offset replacement)")

	var totalSecondStreamBytes int
	for _, chunk := range chunksAfterSecondMeta {
		totalSecondStreamBytes += len(chunk)
	}

	// Remaining bytes from offset 65536 to end: 256KB - 65536 = 196608 bytes
	expectedRemainingBytes := len(fileData) - 65536
	assert.Equal(t, expectedRemainingBytes, totalSecondStreamBytes, "Second stream bytes should match requested offset remaining length")
}

func TestChunkBufferIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "testdata_isolation.bin")

	// Create test file with 128KB of known sequence data
	fileData := make([]byte, 128*1024)
	for i := range fileData {
		fileData[i] = byte(i % 256)
	}
	require.NoError(t, os.WriteFile(filePath, fileData, 0644))

	fileName := filepath.Base(filePath)
	fileSize := int64(len(fileData))

	pcSender, pcReceiver, dcSender, rxDC := setupPeerConnectionPair(t)
	defer pcSender.Close()
	defer pcReceiver.Close()

	dcSender.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString && strings.HasPrefix(string(msg.Data), "OFFSET:") {
			go func() {
				file, err := os.Open(filePath)
				if err != nil {
					return
				}
				defer file.Close()

				metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
				if errSend := dcSender.SendText(metaHeader); errSend != nil {
					return
				}

				buffer := make([]byte, 32*1024)
				for {
					n, err := file.Read(buffer)
					if n > 0 {
						chunk := make([]byte, n)
						copy(chunk, buffer[:n])
						errSend := dcSender.Send(chunk)
						if errSend != nil {
							return
						}
						// Mutate read buffer immediately to simulate buffer reuse/mutation
						for i := 0; i < n; i++ {
							buffer[i] = 0xFF
						}
					}
					if err != nil {
						break
					}
				}

				for dcSender.BufferedAmount() > 0 {
					time.Sleep(5 * time.Millisecond)
				}
				_ = dcSender.SendText("EOF")
			}()
		}
	})

	var receivedData []byte
	transferDone := make(chan struct{})

	rxDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			if string(msg.Data) == "EOF" {
				close(transferDone)
			}
		} else {
			receivedData = append(receivedData, msg.Data...)
		}
	})

	require.Eventually(t, func() bool {
		return rxDC.ReadyState() == webrtc.DataChannelStateOpen && dcSender.ReadyState() == webrtc.DataChannelStateOpen
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, rxDC.SendText("OFFSET:0"))

	select {
	case <-transferDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for EOF")
	}

	assert.Equal(t, fileData, receivedData, "Received bytes must match original file bytes despite read buffer zeroing")
}

func TestActiveBackpressureAndEOFFlush(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "testdata_backpressure.bin")

	// Create test file with 2MB of data to trigger high buffer pressure (>1MB)
	fileData := make([]byte, 2*1024*1024)
	for i := range fileData {
		fileData[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(filePath, fileData, 0644))

	fileName := filepath.Base(filePath)
	fileSize := int64(len(fileData))

	pcSender, pcReceiver, dcSender, rxDC := setupPeerConnectionPair(t)
	defer pcSender.Close()
	defer pcReceiver.Close()

	var backpressureTriggered bool
	var transferCtxMu sync.Mutex
	var transferCancel context.CancelFunc

	dcSender.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString && strings.HasPrefix(string(msg.Data), "OFFSET:") {
			transferCtxMu.Lock()
			if transferCancel != nil {
				transferCancel()
			}
			var ctx context.Context
			ctx, transferCancel = context.WithCancel(context.Background())
			transferCtxMu.Unlock()

			go func(ctx context.Context) {
				file, err := os.Open(filePath)
				if err != nil {
					return
				}
				defer file.Close()

				metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
				_ = dcSender.SendText(metaHeader)

				buffer := make([]byte, 65535)
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					for dcSender.BufferedAmount() > 1024*1024 {
						backpressureTriggered = true
						select {
						case <-ctx.Done():
							return
						case <-time.After(5 * time.Millisecond):
						}
					}

					n, err := file.Read(buffer)
					if n > 0 {
						chunk := make([]byte, n)
						copy(chunk, buffer[:n])
						_ = dcSender.Send(chunk)
					}
					if err != nil {
						break
					}
				}

				for dcSender.BufferedAmount() > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Millisecond):
					}
				}

				if ctx.Err() != nil {
					return
				}

				_ = dcSender.SendText("EOF")
			}(ctx)
		}
	})

	var receivedData []byte
	transferDone := make(chan struct{})

	rxDC.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			if string(msg.Data) == "EOF" {
				close(transferDone)
			}
		} else {
			receivedData = append(receivedData, msg.Data...)
		}
	})

	require.Eventually(t, func() bool {
		return rxDC.ReadyState() == webrtc.DataChannelStateOpen && dcSender.ReadyState() == webrtc.DataChannelStateOpen
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, rxDC.SendText("OFFSET:0"))

	select {
	case <-transferDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for EOF")
	}

	assert.True(t, backpressureTriggered, "Backpressure check should have triggered for 2MB transfer")
	assert.Equal(t, fileData, receivedData, "2MB file transfer must complete with zero corruption")
}
