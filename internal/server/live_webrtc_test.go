package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/signaling"
	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiveStreamPipeSSE(t *testing.T) {
	srv, err := New("", 10*1024*1024) // Live Pipe mode
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// Write initial backlog
	srv.WriteLive([]byte("log line 1\nlog line 2\n"))

	// Connect Client 1 SSE
	req1, err := http.NewRequest(http.MethodGet, ts.URL+"/api/live/stream", nil)
	require.NoError(t, err)

	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	defer resp1.Body.Close()

	reader1 := bufio.NewReader(resp1.Body)

	// Read backlog event on Client 1
	line1, err := reader1.ReadString('\n')
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(line1, "data: "))
	var backlogEv map[string]string
	err = json.Unmarshal([]byte(strings.TrimPrefix(line1, "data: ")), &backlogEv)
	require.NoError(t, err)
	assert.Equal(t, "backlog", backlogEv["type"])
	assert.Equal(t, "log line 1\nlog line 2\n", backlogEv["payload"])

	// Write line 3 live
	srv.WriteLive([]byte("log line 3\n"))

	// Read data event on Client 1
	reader1.ReadLine() // blank line
	line2, err := reader1.ReadString('\n')
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(line2, "data: "))
	var dataEv map[string]string
	err = json.Unmarshal([]byte(strings.TrimPrefix(line2, "data: ")), &dataEv)
	require.NoError(t, err)
	assert.Equal(t, "data", dataEv["type"])
	assert.Equal(t, "log line 3\n", dataEv["payload"])

	// Connect Client 2 SSE and verify it receives complete accumulated backlog
	req2, err := http.NewRequest(http.MethodGet, ts.URL+"/api/live/stream", nil)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()

	reader2 := bufio.NewReader(resp2.Body)
	lineClient2Backlog, err := reader2.ReadString('\n')
	require.NoError(t, err)
	var backlogEv2 map[string]string
	err = json.Unmarshal([]byte(strings.TrimPrefix(lineClient2Backlog, "data: ")), &backlogEv2)
	require.NoError(t, err)
	assert.Equal(t, "backlog", backlogEv2["type"])
	assert.Equal(t, "log line 1\nlog line 2\nlog line 3\n", backlogEv2["payload"])

	// Close live stream and verify EOF event on both clients
	srv.CloseLive()

	reader1.ReadLine() // blank line
	lineEOF1, err := reader1.ReadString('\n')
	require.NoError(t, err)
	var eofEv1 map[string]string
	err = json.Unmarshal([]byte(strings.TrimPrefix(lineEOF1, "data: ")), &eofEv1)
	require.NoError(t, err)
	assert.Equal(t, "eof", eofEv1["type"])
}

func TestWebRTCDataChannelChunkingAndBackpressure(t *testing.T) {
	txDCReady := make(chan struct{})

	// Create sender session (offline)
	senderSession, err := signaling.NewSession([]webrtc.ICEServer{}, 2*time.Second)
	require.NoError(t, err)
	defer senderSession.Close()

	senderSession.OnOpen = func(dc *webrtc.DataChannel) {
		close(txDCReady)
	}

	// Create receiver PeerConnection (offline)
	rxPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
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
	var eofReceived = make(chan struct{})
	var chunkSizes []int

	rxDataChannelCh := make(chan *webrtc.DataChannel, 1)
	rxPC.OnDataChannel(func(dc *webrtc.DataChannel) {
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
				chunkSizes = append(chunkSizes, len(msg.Data))
				receivedBuf.Write(msg.Data)
			}
		})
		rxDataChannelCh <- dc
	})

	// Handshake step 1: Create Offer
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = senderSession.CreateOffer(ctx)
	require.NoError(t, err)

	// Handshake step 2: Set Remote Description on Receiver
	err = rxPC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  senderSession.RawOffer(),
	})
	require.NoError(t, err)

	// Add sender candidates to receiver (now valid because remote description is set)
	for _, cand := range senderSession.GetCandidates() {
		_ = rxPC.AddICECandidate(cand)
	}

	senderSession.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = rxPC.AddICECandidate(c.ToJSON())
		}
	})

	// Handshake step 3: Create and Set Local Answer on Receiver
	answer, err := rxPC.CreateAnswer(nil)
	require.NoError(t, err)
	err = rxPC.SetLocalDescription(answer)
	require.NoError(t, err)

	answerBytes, err := json.Marshal(answer)
	require.NoError(t, err)

	// Handshake step 4: Provide Answer to Sender (sets Remote Description on Sender)
	err = senderSession.ProvideAnswer(string(answerBytes))
	require.NoError(t, err)

	// Add receiver candidates to sender (now valid because remote description is set)
	mu.Lock()
	for _, cand := range rxCandidates {
		_ = senderSession.AddICECandidate(cand)
	}
	mu.Unlock()

	// Wait for DataChannel to open
	var rxDC *webrtc.DataChannel
	select {
	case rxDC = <-rxDataChannelCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel")
	}

	rxOpenCh := make(chan struct{})
	if rxDC.ReadyState() == webrtc.DataChannelStateOpen {
		close(rxOpenCh)
	} else {
		rxDC.OnOpen(func() {
			close(rxOpenCh)
		})
	}

	select {
	case <-rxOpenCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for receiver DataChannel open")
	}

	select {
	case <-txDCReady:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for sender DataChannel open")
	}

	txDC := senderSession.DataChannel()
	require.NotNil(t, txDC)
	txDC.SetBufferedAmountLowThreshold(1024)

	// Setup payload: 250KB file data
	fileSize := 250 * 1024
	testPayload := make([]byte, fileSize)
	for i := range testPayload {
		testPayload[i] = byte(i % 256)
	}

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "p2p_test.bin")
	err = os.WriteFile(filePath, testPayload, 0644)
	require.NoError(t, err)

	// Perform sender chunked transfer with backpressure check
	go func() {
		file, err := os.Open(filePath)
		if err != nil {
			return
		}
		defer file.Close()

		metaHeader := fmt.Sprintf("META:p2p_test.bin:%d", fileSize)
		_ = txDC.SendText(metaHeader)

		chunkSize := 16 * 1024
		buf := make([]byte, chunkSize)

		for {
			// Active backpressure check
			for txDC.BufferedAmount() > 1024*1024 {
				time.Sleep(5 * time.Millisecond)
			}

			n, err := file.Read(buf)
			if n > 0 {
				_ = txDC.Send(buf[:n])
				time.Sleep(2 * time.Millisecond)
			}
			if err != nil {
				break
			}
		}

		for txDC.BufferedAmount() > 0 {
			time.Sleep(5 * time.Millisecond)
		}
		_ = txDC.SendText("EOF")
	}()

	select {
	case <-eofReceived:
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, fmt.Sprintf("META:p2p_test.bin:%d", fileSize), receivedMeta)
		assert.Equal(t, testPayload, receivedBuf.Bytes())
		// Verify 64KB chunking
		for i, sz := range chunkSizes {
			if i < len(chunkSizes)-1 {
				assert.Equal(t, 16*1024, sz, "Intermediate chunks should be 16KB")
			} else {
				assert.True(t, sz <= 16*1024, "Last chunk should be <= 16KB")
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for P2P chunked transfer completion")
	}
}
