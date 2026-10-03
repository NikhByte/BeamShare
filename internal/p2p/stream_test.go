package p2p

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDataChannel struct {
	mu             sync.Mutex
	sentChunks     [][]byte
	sentTexts      []string
	bufferedAmount uint64
	lowThreshold   uint64
	lowCb          func()
}

func newMockDataChannel() *mockDataChannel {
	return &mockDataChannel{
		sentChunks: make([][]byte, 0),
		sentTexts:  make([]string, 0),
	}
}

func (m *mockDataChannel) Send(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Store a copy to verify what was received
	cpy := make([]byte, len(data))
	copy(cpy, data)
	m.sentChunks = append(m.sentChunks, cpy)
	return nil
}

func (m *mockDataChannel) SendText(text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentTexts = append(m.sentTexts, text)
	return nil
}

func (m *mockDataChannel) BufferedAmount() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bufferedAmount
}

func (m *mockDataChannel) SetBufferedAmountLowThreshold(threshold uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lowThreshold = threshold
}

func (m *mockDataChannel) OnBufferedAmountLow(f func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lowCb = f
}

func (m *mockDataChannel) setBufferedAmount(amount uint64) {
	m.mu.Lock()
	m.bufferedAmount = amount
	cb := m.lowCb
	thresh := m.lowThreshold
	m.mu.Unlock()

	if amount <= thresh && cb != nil {
		cb()
	}
}

func (m *mockDataChannel) getSentTexts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]string, len(m.sentTexts))
	copy(res, m.sentTexts)
	return res
}

func (m *mockDataChannel) getSentChunks() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([][]byte, len(m.sentChunks))
	copy(res, m.sentChunks)
	return res
}

func TestStreamSender_SingleFlight(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_single_flight.bin")
	fileData := bytes.Repeat([]byte("A"), 128*1024)
	err := os.WriteFile(filePath, fileData, 0600)
	require.NoError(t, err)

	mockDC := newMockDataChannel()
	// Hold stream in backpressure initially
	mockDC.setBufferedAmount(2 * HighWaterMark)

	sender := NewStreamSender(mockDC)

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- sender.StartStream(context.Background(), filePath, "test_single_flight.bin", int64(len(fileData)), 0)
	}()

	// Wait for streaming state to be active
	require.Eventually(t, func() bool {
		return sender.IsStreaming()
	}, 1*time.Second, 10*time.Millisecond)

	// Attempt duplicate OFFSET triggers
	var wg sync.WaitGroup
	duplicateErrs := make([]error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			duplicateErrs[idx] = sender.StartStream(context.Background(), filePath, "test_single_flight.bin", int64(len(fileData)), 0)
		}(i)
	}
	wg.Wait()

	for i, errDup := range duplicateErrs {
		assert.True(t, errors.Is(errDup, ErrAlreadyStreaming), "duplicate call %d should return ErrAlreadyStreaming", i)
	}

	// Unblock backpressure so stream completes
	mockDC.setBufferedAmount(0)

	select {
	case err1 := <-errCh1:
		require.NoError(t, err1)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for primary stream to complete")
	}

	texts := mockDC.getSentTexts()
	require.Len(t, texts, 2)
	assert.Equal(t, fmt.Sprintf("META:test_single_flight.bin:%d", len(fileData)), texts[0])
	assert.Equal(t, "EOF", texts[1])
}

func TestStreamSender_SliceIsolation(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_isolation.bin")
	fileData := make([]byte, 100*1024)
	for i := range fileData {
		fileData[i] = byte(i % 256)
	}
	err := os.WriteFile(filePath, fileData, 0600)
	require.NoError(t, err)

	mockDC := newMockDataChannel()
	sender := NewStreamSender(mockDC)

	var progressCalls int
	sender.OnProgress = func(sent, total int64) {
		progressCalls++
	}

	err = sender.StartStream(context.Background(), filePath, "test_isolation.bin", int64(len(fileData)), 0)
	require.NoError(t, err)
	assert.Greater(t, progressCalls, 0)

	chunks := mockDC.getSentChunks()
	combined := bytes.Join(chunks, nil)
	assert.Equal(t, fileData, combined)
}

func TestStreamSender_BackpressurePolling(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_backpressure.bin")
	fileData := bytes.Repeat([]byte("B"), 200*1024)
	err := os.WriteFile(filePath, fileData, 0600)
	require.NoError(t, err)

	mockDC := newMockDataChannel()
	mockDC.setBufferedAmount(2 * HighWaterMark)

	sender := NewStreamSender(mockDC)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sender.StartStream(context.Background(), filePath, "test_backpressure.bin", int64(len(fileData)), 0)
	}()

	// Ensure stream starts and enters backpressure wait loop
	require.Eventually(t, func() bool {
		return sender.IsStreaming()
	}, 1*time.Second, 10*time.Millisecond)

	// Verify it remains blocked while buffer > 1MB
	time.Sleep(100 * time.Millisecond)
	select {
	case <-errCh:
		t.Fatal("stream finished prematurely while backpressure buffer was high")
	default:
	}

	// Draining buffer to 0 without calling lowCb (simulates missed callback/polling fallback)
	mockDC.mu.Lock()
	mockDC.bufferedAmount = 0
	mockDC.mu.Unlock()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("stream failed to resume via backpressure polling fallback")
	}
}

func TestStreamSender_EOFFlush(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_eof.bin")
	fileData := []byte("Small file for EOF flush test")
	err := os.WriteFile(filePath, fileData, 0600)
	require.NoError(t, err)

	mockDC := newMockDataChannel()

	sender := NewStreamSender(mockDC)

	// Set buffer > 0 when file EOF is reached
	// We'll set buffer to 50 KB right as StartStream begins
	mockDC.mu.Lock()
	mockDC.bufferedAmount = 50 * 1024
	mockDC.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sender.StartStream(context.Background(), filePath, "test_eof.bin", int64(len(fileData)), 0)
	}()

	// Verify EOF text is not sent yet because buffer > 0
	time.Sleep(100 * time.Millisecond)
	texts := mockDC.getSentTexts()
	assert.NotContains(t, texts, "EOF")

	// Lower buffer to 0 to allow EOF flush
	mockDC.setBufferedAmount(0)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("stream timed out waiting for EOF flush")
	}

	texts = mockDC.getSentTexts()
	assert.Contains(t, texts, "EOF")
}

func TestStreamSender_ContextCancel(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_cancel.bin")
	fileData := bytes.Repeat([]byte("C"), 300*1024)
	err := os.WriteFile(filePath, fileData, 0600)
	require.NoError(t, err)

	mockDC := newMockDataChannel()
	mockDC.setBufferedAmount(2 * HighWaterMark)

	sender := NewStreamSender(mockDC)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- sender.StartStream(ctx, filePath, "test_cancel.bin", int64(len(fileData)), 0)
	}()

	require.Eventually(t, func() bool {
		return sender.IsStreaming()
	}, 1*time.Second, 10*time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	case <-time.After(2 * time.Second):
		t.Fatal("stream failed to cancel upon context cancellation")
	}

	assert.False(t, sender.IsStreaming())
}
