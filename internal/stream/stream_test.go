package stream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDataChannel struct {
	mu             sync.Mutex
	bufferedAmount uint64
	sentTexts      []string
	sentChunks     [][]byte
	sendHook       func(data []byte)
}

func (m *mockDataChannel) SendText(text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentTexts = append(m.sentTexts, text)
	return nil
}

func (m *mockDataChannel) Send(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Store slice reference directly (to test immutability: if caller reuses slice memory, stored data gets corrupted)
	m.sentChunks = append(m.sentChunks, data)
	if m.sendHook != nil {
		m.sendHook(data)
	}
	return nil
}

func (m *mockDataChannel) BufferedAmount() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bufferedAmount
}

func (m *mockDataChannel) SetBufferedAmount(amount uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bufferedAmount = amount
}

func TestStreamManager_SingleFlightCancellation(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_large.dat")

	// 500KB test file
	fileData := make([]byte, 500*1024)
	for i := range fileData {
		fileData[i] = byte((i * 31) % 256)
	}
	require.NoError(t, os.WriteFile(filePath, fileData, 0644))

	dc := &mockDataChannel{}
	mgr := NewManager()

	var completedCount int32
	opts1 := StreamOptions{
		FilePath: filePath,
		FileName: "test_large.dat",
		FileSize: int64(len(fileData)),
		Offset:   0,
		OnComplete: func(sentInSession int64, duration time.Duration) {
			atomic.AddInt32(&completedCount, 1)
		},
	}

	opts2 := StreamOptions{
		FilePath: filePath,
		FileName: "test_large.dat",
		FileSize: int64(len(fileData)),
		Offset:   250 * 1024,
		OnComplete: func(sentInSession int64, duration time.Duration) {
			atomic.AddInt32(&completedCount, 1)
		},
	}

	// Trigger first stream, then immediately override with duplicate OFFSET frame
	mgr.StartStream(context.Background(), dc, opts1)
	mgr.StartStream(context.Background(), dc, opts2)

	// Wait for transfer to finish
	require.Eventually(t, func() bool {
		dc.mu.Lock()
		defer dc.mu.Unlock()
		for _, text := range dc.sentTexts {
			if text == "EOF" {
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)

	// Ensure only 1 worker completed (the single-flight active worker)
	assert.Equal(t, int32(1), atomic.LoadInt32(&completedCount))

	// Verify payload received matches second half of file
	dc.mu.Lock()
	defer dc.mu.Unlock()

	var receivedBuf bytes.Buffer
	for _, chunk := range dc.sentChunks {
		receivedBuf.Write(chunk)
	}

	expectedPayload := fileData[250*1024:]
	assert.Equal(t, expectedPayload, receivedBuf.Bytes())
}

func TestStreamManager_ImmutableChunkAllocation(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_immutable.dat")

	// 128KB payload
	payload := make([]byte, 128*1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(filePath, payload, 0644))

	hasher := sha256.New()
	hasher.Write(payload)
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	dc := &mockDataChannel{}
	mgr := NewManager()

	done := make(chan struct{})
	opts := StreamOptions{
		FilePath:  filePath,
		FileName:  "test_immutable.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 16 * 1024,
		OnComplete: func(sentInSession int64, duration time.Duration) {
			close(done)
		},
	}

	mgr.StartStream(context.Background(), dc, opts)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stream completion")
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	require.Len(t, dc.sentChunks, 8) // 128KB / 16KB = 8 chunks

	// Check each chunk slice address is distinct
	for i := 0; i < len(dc.sentChunks); i++ {
		for j := i + 1; j < len(dc.sentChunks); j++ {
			p1 := &dc.sentChunks[i][0]
			p2 := &dc.sentChunks[j][0]
			assert.NotEqual(t, p1, p2, "chunk slice buffers must be distinct allocations")
		}
	}

	// Verify total payload integrity
	var fullBuf bytes.Buffer
	for _, chunk := range dc.sentChunks {
		fullBuf.Write(chunk)
	}

	dlHasher := sha256.New()
	dlHasher.Write(fullBuf.Bytes())
	actualHash := hex.EncodeToString(dlHasher.Sum(nil))

	assert.Equal(t, expectedHash, actualHash)
}

func TestStreamManager_PollingBackpressure(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_backpressure.dat")

	payload := make([]byte, 200*1024)
	require.NoError(t, os.WriteFile(filePath, payload, 0644))

	dc := &mockDataChannel{}
	// Set initial buffer amount above HighWater (1.5MB > 1MB)
	dc.SetBufferedAmount(1500 * 1024)

	mgr := NewManager()
	done := make(chan struct{})

	opts := StreamOptions{
		FilePath:     filePath,
		FileName:     "test_backpressure.dat",
		FileSize:     int64(len(payload)),
		ChunkSize:    32 * 1024,
		HighWater:    1024 * 1024, // 1MB
		LowWater:     512 * 1024,  // 512KB
		PollInterval: 5 * time.Millisecond,
		OnComplete: func(sentInSession int64, duration time.Duration) {
			close(done)
		},
	}

	mgr.StartStream(context.Background(), dc, opts)

	// Wait 50ms and check that no chunks were sent while buffer > 1MB
	time.Sleep(50 * time.Millisecond)
	dc.mu.Lock()
	assert.Empty(t, dc.sentChunks, "no chunks should be sent while backpressure is active")
	dc.mu.Unlock()

	// Drain buffer below LowWater (200KB < 512KB)
	dc.SetBufferedAmount(200 * 1024)
	time.Sleep(20 * time.Millisecond)
	dc.SetBufferedAmount(0)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stream completion after backpressure drain")
	}

	dc.mu.Lock()
	assert.NotEmpty(t, dc.sentChunks, "chunks should be sent after backpressure drops below LowWater")
	dc.mu.Unlock()
}

func TestStreamManager_DeterministicEOFFlush(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_eof.dat")

	payload := make([]byte, 64*1024)
	require.NoError(t, os.WriteFile(filePath, payload, 0644))

	dc := &mockDataChannel{}
	mgr := NewManager()

	var eofSentTime time.Time
	var bufferClearedTime time.Time

	done := make(chan struct{})

	// Hook into Send to track when buffer clears and EOF is sent
	dc.sendHook = func(data []byte) {
		// Simulation: when chunks are sent, simulate buffered data in DataChannel
		dc.bufferedAmount = 500 * 1024
	}

	opts := StreamOptions{
		FilePath:     filePath,
		FileName:     "test_eof.dat",
		FileSize:     int64(len(payload)),
		ChunkSize:    64 * 1024,
		PollInterval: 5 * time.Millisecond,
		OnComplete: func(sentInSession int64, duration time.Duration) {
			close(done)
		},
	}

	mgr.StartStream(context.Background(), dc, opts)

	// Wait 50ms while bufferedAmount is 500KB
	time.Sleep(50 * time.Millisecond)

	dc.mu.Lock()
	eofFound := false
	for _, text := range dc.sentTexts {
		if text == "EOF" {
			eofFound = true
		}
	}
	assert.False(t, eofFound, "EOF must not be sent while BufferedAmount > 0")
	dc.mu.Unlock()

	// Set bufferedAmount to 0
	bufferClearedTime = time.Now()
	dc.SetBufferedAmount(0)

	select {
	case <-done:
		eofSentTime = time.Now()
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stream completion after EOF flush")
	}

	assert.True(t, eofSentTime.After(bufferClearedTime) || eofSentTime.Equal(bufferClearedTime))

	dc.mu.Lock()
	require.Contains(t, dc.sentTexts, "EOF")
	dc.mu.Unlock()
}

func TestStreamManager_RaceCondition(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_race.dat")

	payload := make([]byte, 100*1024)
	require.NoError(t, os.WriteFile(filePath, payload, 0644))

	dc := &mockDataChannel{}
	mgr := NewManager()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			offset := int64((idx * 1000) % len(payload))
			opts := StreamOptions{
				FilePath: filePath,
				FileName: "test_race.dat",
				FileSize: int64(len(payload)),
				Offset:   offset,
			}
			mgr.StartStream(ctx, dc, opts)
			if idx%3 == 0 {
				mgr.Stop()
			}
		}(i)
	}

	wg.Wait()
	mgr.Stop()
}
