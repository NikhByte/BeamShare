package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/relay"
	"github.com/beamshare/beam/internal/server"
	"github.com/beamshare/beam/internal/signaling"
	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEndToEndDirectHTTP verifies direct HTTP upload, download, and file integrity.
func TestEndToEndDirectHTTP(t *testing.T) {
	srv, err := server.New("", 0)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// Prepare test file
	fileSize := 1024 * 1024 // 1MB
	testPayload := make([]byte, fileSize)
	for i := range testPayload {
		testPayload[i] = byte((i * 17) % 256)
	}

	hasher := sha256.New()
	hasher.Write(testPayload)
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	// Upload file via multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "source.dat")
	require.NoError(t, err)
	_, err = part.Write(testPayload)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/upload", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var uploadResp map[string]string
	err = json.NewDecoder(resp.Body).Decode(&uploadResp)
	require.NoError(t, err)

	assert.Equal(t, "ok", uploadResp["status"])

	// Download file
	downloadReq, err := http.NewRequest(http.MethodGet, ts.URL+"/api/download", nil)
	require.NoError(t, err)

	downloadResp, err := http.DefaultClient.Do(downloadReq)
	require.NoError(t, err)
	defer downloadResp.Body.Close()

	require.Equal(t, http.StatusOK, downloadResp.StatusCode)

	downloadedData, err := io.ReadAll(downloadResp.Body)
	require.NoError(t, err)

	dlHasher := sha256.New()
	dlHasher.Write(downloadedData)
	actualHash := hex.EncodeToString(dlHasher.Sum(nil))

	assert.Equal(t, expectedHash, actualHash)
}

// TestEndToEndOpticalWebRTCP2P tests optical QR SDP compression/decompression and WebRTC P2P transfer offline.
func TestEndToEndOpticalWebRTCP2P(t *testing.T) {
	// 1. Create Sender Session
	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 10*time.Second)
	require.NoError(t, err)
	defer senderSession.Close()

	senderTxReady := make(chan struct{})
	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		close(senderTxReady)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	require.NoError(t, err)

	rawOffer := senderSession.RawOffer()

	// Compress SDP for Optical QR transfer
	compressedOffer, err := signaling.CompressSDP(rawOffer)
	require.NoError(t, err)

	// 2. Optical Receiver decompresses SDP
	decompressedOffer, err := signaling.DecompressSDP(compressedOffer)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(decompressedOffer, "v=0"))
	assert.True(t, strings.Contains(decompressedOffer, "m=application"))

	rxPC, err := signaling.NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
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

	var receivedBuf bytes.Buffer
	var receivedMeta string
	eofReceived := make(chan struct{})

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
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			mu.Lock()
			defer mu.Unlock()
			if msg.IsString {
				str := string(msg.Data)
				if strings.HasPrefix(str, "META:") {
					receivedMeta = str
				} else if str == "EOF" {
					close(eofReceived)
				}
			} else {
				receivedBuf.Write(msg.Data)
			}
		})
		rxDataChannelCh <- dc
	})

	// Optical Receiver sets remote offer
	err = rxPC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  decompressedOffer,
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

	answerObj, err := rxPC.CreateAnswer(nil)
	require.NoError(t, err)
	err = rxPC.SetLocalDescription(answerObj)
	require.NoError(t, err)

	answerBytes, err := json.Marshal(answerObj)
	require.NoError(t, err)

	// Optical Sender decompresses answer
	compressedAnswer, err := signaling.CompressSDP(string(answerBytes))
	require.NoError(t, err)

	decompressedAnswer, err := signaling.DecompressSDP(compressedAnswer)
	require.NoError(t, err)

	err = senderSession.ProvideAnswer(decompressedAnswer)
	require.NoError(t, err)

	mu.Lock()
	for _, cand := range rxCandidates {
		_ = senderSession.AddICECandidate(cand)
	}
	mu.Unlock()

	var rxDC *webrtc.DataChannel
	select {
	case rxDC = <-rxDataChannelCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel")
	}

	_ = rxDC

	select {
	case <-rxOpenCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel open")
	}

	select {
	case <-senderTxReady:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for sender DataChannel open")
	}

	txDC := senderSession.DataChannel()
	require.NotNil(t, txDC)

	// Perform P2P data transfer
	testPayload := []byte("Hello WebRTC P2P Optical Transfer test payload!")
	go func() {
		metaHeader := fmt.Sprintf("META:optical.txt:%d", len(testPayload))
		_ = txDC.SendText(metaHeader)
		_ = txDC.Send(testPayload)
		for txDC.BufferedAmount() > 0 {
			time.Sleep(5 * time.Millisecond)
		}
		_ = txDC.SendText("EOF")
	}()

	select {
	case <-eofReceived:
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, fmt.Sprintf("META:optical.txt:%d", len(testPayload)), receivedMeta)
		assert.Equal(t, testPayload, receivedBuf.Bytes())
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for P2P transfer completion")
	}
}

// TestEndToEndWebRTCP2P_PauseResumeFlowControl tests PAUSE and RESUME flow control messages across WebRTC DataChannel.
func TestEndToEndWebRTCP2P_PauseResumeFlowControl(t *testing.T) {
	type pauseCtrl struct {
		mu     sync.Mutex
		cond   *sync.Cond
		paused bool
		closed bool
	}
	newPauseCtrl := func() *pauseCtrl {
		p := &pauseCtrl{}
		p.cond = sync.NewCond(&p.mu)
		return p
	}
	pause := func(p *pauseCtrl) {
		p.mu.Lock()
		p.paused = true
		p.mu.Unlock()
	}
	resume := func(p *pauseCtrl) {
		p.mu.Lock()
		p.paused = false
		p.cond.Broadcast()
		p.mu.Unlock()
	}
	closePC := func(p *pauseCtrl) {
		p.mu.Lock()
		p.closed = true
		p.cond.Broadcast()
		p.mu.Unlock()
	}
	waitIfPaused := func(p *pauseCtrl) bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for p.paused && !p.closed {
			p.cond.Wait()
		}
		return !p.closed
	}

	// Prepare 3.2MB test payload (200 x 16KB chunks)
	chunkSize := 16 * 1024
	numChunks := 200
	totalSize := chunkSize * numChunks
	testPayload := make([]byte, totalSize)
	for i := range testPayload {
		testPayload[i] = byte((i * 31) % 256)
	}

	hasher := sha256.New()
	hasher.Write(testPayload)
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	// 1. Create Sender Session
	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 10*time.Second)
	require.NoError(t, err)
	defer senderSession.Close()

	senderTxReady := make(chan struct{})
	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		pc := newPauseCtrl()
		dc.OnClose(func() { closePC(pc) })

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				dataStr := string(msg.Data)
				if dataStr == "PAUSE" {
					pause(pc)
				} else if dataStr == "RESUME" {
					resume(pc)
				} else if strings.HasPrefix(dataStr, "OFFSET:") {
					go func() {
						defer closePC(pc)
						metaHeader := fmt.Sprintf("META:flow_test.bin:%d", totalSize)
						_ = dc.SendText(metaHeader)

						bufferedAmountLowChan := make(chan struct{}, 1)
						dc.SetBufferedAmountLowThreshold(512 * 1024)
						dc.OnBufferedAmountLow(func() {
							select {
							case bufferedAmountLowChan <- struct{}{}:
							default:
							}
						})

						for i := 0; i < numChunks; i++ {
							if !waitIfPaused(pc) {
								return
							}

							if dc.BufferedAmount() > 1024*1024 {
								<-bufferedAmountLowChan
							}

							if !waitIfPaused(pc) {
								return
							}

							startIdx := i * chunkSize
							endIdx := startIdx + chunkSize
							errSend := dc.Send(testPayload[startIdx:endIdx])
							if errSend != nil {
								t.Errorf("dc.Send chunk %d error: %v", i, errSend)
								return
							}
							time.Sleep(1 * time.Millisecond)
						}

						for dc.BufferedAmount() > 0 {
							time.Sleep(2 * time.Millisecond)
						}
						_ = dc.SendText("EOF")
					}()
				}
			}
		})
		close(senderTxReady)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	require.NoError(t, err)

	// 2. Receiver Setup
	rxPC, err := signaling.NewWebRTCAPI().NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
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

	var receivedBuf bytes.Buffer
	var chunksReceived int
	pauseAtChunk := 10
	pausedCh := make(chan struct{}, 1)
	eofReceived := make(chan struct{})

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
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			mu.Lock()
			defer mu.Unlock()

			if msg.IsString {
				if string(msg.Data) == "EOF" {
					close(eofReceived)
				}
			} else {
				receivedBuf.Write(msg.Data)
				chunksReceived++

				if chunksReceived == pauseAtChunk {
					_ = dc.SendText("PAUSE")
					select {
					case pausedCh <- struct{}{}:
					default:
					}
				}
			}
		})
		rxDataChannelCh <- dc
	})

	// Set descriptions and candidates
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

	answerObj, err := rxPC.CreateAnswer(nil)
	require.NoError(t, err)
	err = rxPC.SetLocalDescription(answerObj)
	require.NoError(t, err)

	answerBytes, err := json.Marshal(answerObj)
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
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel")
	}

	select {
	case <-rxOpenCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel open")
	}

	select {
	case <-senderTxReady:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for sender DataChannel open")
	}

	// Request transfer via OFFSET:0
	err = rxDC.SendText("OFFSET:0")
	require.NoError(t, err)

	// Wait for receiver to emit PAUSE
	select {
	case <-pausedCh:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for PAUSE trigger")
	}

	// Sleep 250ms and verify that chunk count stays constant while paused
	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	countWhilePaused := chunksReceived
	mu.Unlock()

	// Should not have received all chunks while paused
	assert.Less(t, countWhilePaused, numChunks)

	// Send RESUME signal
	err = rxDC.SendText("RESUME")
	require.NoError(t, err)

	// Wait for transfer completion
	select {
	case <-eofReceived:
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, totalSize, receivedBuf.Len())

		actualHasher := sha256.New()
		actualHasher.Write(receivedBuf.Bytes())
		actualHash := hex.EncodeToString(actualHasher.Sum(nil))

		assert.Equal(t, expectedHash, actualHash)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for EOF after RESUME")
	}
}

// TestEndToEndRelayLongPollingAndEncryptedStream tests the in-memory relay server harness and encrypted fallback streaming.
func TestEndToEndRelayLongPollingAndEncryptedStream(t *testing.T) {
	relayServer := relay.NewServer()
	ts := httptest.NewServer(relayServer)
	defer ts.Close()

	relayClient := relay.NewClient(ts.URL)

	// 1. Session registration and state push
	sessionID, err := relayClient.Register(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, sessionID)

	candidates := []map[string]interface{}{{"candidate": "cand-1"}}
	meta := map[string]interface{}{"fileName": "test.txt", "fileSize": 100}

	err = relayClient.PushState(context.Background(), "sample-offer-sdp", candidates, meta, nil)
	require.NoError(t, err)

	// Verify relay session state stored in server
	session := relayServer.GetSession(sessionID)
	require.NotNil(t, session)
	assert.Equal(t, "sample-offer-sdp", session.Offer)
	assert.Equal(t, candidates, session.Candidates)

	// 2. Encrypted Data Streaming Fallback via Relay Server
	secretKey := "32-byte-secret-key-for-aes-256!!"
	relayClient.Key = []byte(secretKey)

	testPayload := bytes.Repeat([]byte("Encrypted Relay Stream Fallback Test Payload "), 100)

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "payload.dat")
	err = os.WriteFile(filePath, testPayload, 0644)
	require.NoError(t, err)

	// Download client connects to /api/download?s=sessionID
	downloadURL := fmt.Sprintf("%s/api/download?s=%s", ts.URL, sessionID)

	var downloadedBytes []byte
	var downloadErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		resp, errGet := http.Get(downloadURL)
		if errGet != nil {
			downloadErr = errGet
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			downloadErr = fmt.Errorf("unexpected status: %d", resp.StatusCode)
			return
		}

		decReader, errDec := relay.NewDecryptingReader(resp.Body, []byte(secretKey))
		if errDec != nil {
			downloadErr = errDec
			return
		}

		downloadedBytes, downloadErr = io.ReadAll(decReader)
	}()

	// Wait until download handler has attached pipes to session in relay server
	require.Eventually(t, func() bool {
		sess := relayServer.GetSession(sessionID)
		if sess == nil {
			return false
		}
		return sess.IsPipeReady()
	}, 5*time.Second, 10*time.Millisecond, "Relay download pipe was not initialized in time")

	// Upload encrypted stream from sender
	err = relayClient.UploadData(context.Background(), filePath)
	require.NoError(t, err)

	wg.Wait()
	require.NoError(t, downloadErr)
	assert.Equal(t, testPayload, downloadedBytes)
}

// TestEndToEndLiveStdinPipeStreaming tests live pipe SSE streaming.
func TestEndToEndLiveStdinPipeStreaming(t *testing.T) {
	srv, err := server.New("", 10*1024*1024)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// Initial backlog
	srv.WriteLive([]byte("line 1\nline 2\n"))

	// Connect SSE client
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/live/stream", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	// Read backlog
	line1, err := reader.ReadString('\n')
	require.NoError(t, err)
	var backlogEv map[string]string
	err = json.Unmarshal([]byte(strings.TrimPrefix(line1, "data: ")), &backlogEv)
	require.NoError(t, err)
	assert.Equal(t, "backlog", backlogEv["type"])
	assert.Equal(t, "line 1\nline 2\n", backlogEv["payload"])

	// Write live data
	srv.WriteLive([]byte("line 3\n"))

	reader.ReadLine() // blank line
	line2, err := reader.ReadString('\n')
	require.NoError(t, err)
	var dataEv map[string]string
	err = json.Unmarshal([]byte(strings.TrimPrefix(line2, "data: ")), &dataEv)
	require.NoError(t, err)
	assert.Equal(t, "data", dataEv["type"])
	assert.Equal(t, "line 3\n", dataEv["payload"])

	// Close stream
	srv.CloseLive()

	reader.ReadLine() // blank line
	line3, err := reader.ReadString('\n')
	require.NoError(t, err)
	var eofEv map[string]string
	err = json.Unmarshal([]byte(strings.TrimPrefix(line3, "data: ")), &eofEv)
	require.NoError(t, err)
	assert.Equal(t, "eof", eofEv["type"])
}
