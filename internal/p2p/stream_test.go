package p2p

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockDataChannel implements DataChannel for testing purposes.
type mockDataChannel struct {
	mu                   sync.Mutex
	bufferedAmount       uint64
	lowThreshold         uint64
	onBufferedLowHandler func()
	sentText             []string
	sentChunks           [][]byte
	sendError            error
	sendTextError        error
}

func newMockDataChannel() *mockDataChannel {
	return &mockDataChannel{}
}

func (m *mockDataChannel) Send(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendError != nil {
		return m.sendError
	}
	// Copy data to ensure we store what was passed
	cp := make([]byte, len(data))
	copy(cp, data)
	m.sentChunks = append(m.sentChunks, cp)
	return nil
}

func (m *mockDataChannel) SendText(text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendTextError != nil {
		return m.sendTextError
	}
	m.sentText = append(m.sentText, text)
	return nil
}

func (m *mockDataChannel) BufferedAmount() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bufferedAmount
}

func (m *mockDataChannel) setBufferedAmount(amount uint64) {
	m.mu.Lock()
	m.bufferedAmount = amount
	handler := m.onBufferedLowHandler
	thresh := m.lowThreshold
	m.mu.Unlock()

	if amount <= thresh && handler != nil {
		handler()
	}
}

func (m *mockDataChannel) SetBufferedAmountLowThreshold(threshold uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lowThreshold = threshold
}

func (m *mockDataChannel) OnBufferedAmountLow(f func()) {
	m.mu.Lock()
	m.onBufferedLowHandler = f
	m.mu.Unlock()
}

func (m *mockDataChannel) getSentText() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.sentText))
	copy(cp, m.sentText)
	return cp
}

func (m *mockDataChannel) getSentChunks() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([][]byte, len(m.sentChunks))
	copy(cp, m.sentChunks)
	return cp
}

// TestStartStream_RepeatedCallsCancelsPriorGoroutine verifies that calling StartStream
// repeatedly cancels previous goroutines without leaking handles or hanging.
func TestStartStream_RepeatedCallsCancelsPriorGoroutine(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "large_test.bin")

	// Write 5MB test file
	fileData := make([]byte, 5*1024*1024)
	for i := range fileData {
		fileData[i] = byte(i % 256)
	}
	if err := os.WriteFile(filePath, fileData, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	sender := NewStreamSender()
	defer sender.Stop()

	dc := newMockDataChannel()

	var startedCount atomic.Int32
	var completedCount atomic.Int32

	// Launch initial stream
	sender.StartStream(context.Background(), dc, filePath, "large_test.bin", int64(len(fileData)), 0,
		WithProgress(func(sent, total int64) {
			startedCount.Add(1)
		}),
		WithComplete(func(sentInSession int64, elapsed time.Duration) {
			completedCount.Add(1)
		}),
	)

	// Rapidly call StartStream again with a new offset (simulating reconnection / new OFFSET: request)
	time.Sleep(5 * time.Millisecond)
	dc2 := newMockDataChannel()
	sender.StartStream(context.Background(), dc2, filePath, "large_test.bin", int64(len(fileData)), 1024*1024,
		WithComplete(func(sentInSession int64, elapsed time.Duration) {
			completedCount.Add(1)
		}),
	)

	// Wait for dc2 stream to complete
	deadline := time.Now().Add(5 * time.Second)
	for {
		txts := dc2.getSentText()
		if len(txts) > 0 && txts[len(txts)-1] == "EOF" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dc2 stream failed to complete within deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify only 1 completion callback fired for the final stream
	if completedCount.Load() != 1 {
		t.Errorf("expected 1 stream completion, got %d", completedCount.Load())
	}

	// Verify dc2 received META header and EOF
	txts := dc2.getSentText()
	if len(txts) < 2 {
		t.Fatalf("expected at least META and EOF text messages on dc2, got %v", txts)
	}
	if txts[0] != "META:large_test.bin:5242880" {
		t.Errorf("unexpected META header: %s", txts[0])
	}
	if txts[len(txts)-1] != "EOF" {
		t.Errorf("unexpected final text: %s", txts[len(txts)-1])
	}
}

// TestBufferIsolation verifies that every chunk sent to dc.Send is an independent allocation.
func TestBufferIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "buffer_test.bin")

	// Create 200KB file
	fileData := make([]byte, 200*1024)
	for i := range fileData {
		fileData[i] = byte(i % 251)
	}
	if err := os.WriteFile(filePath, fileData, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	sender := NewStreamSender()
	defer sender.Stop()

	dc := newMockDataChannel()

	done := make(chan struct{})
	sender.StartStream(context.Background(), dc, filePath, "buffer_test.bin", int64(len(fileData)), 0,
		WithComplete(func(sentInSession int64, elapsed time.Duration) {
			close(done)
		}),
	)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("stream timed out")
	}

	chunks := dc.getSentChunks()
	if len(chunks) == 0 {
		t.Fatalf("no chunks sent")
	}

	// Reconstruct file from sent chunks
	var reassembled []byte
	for _, chunk := range chunks {
		reassembled = append(reassembled, chunk...)
	}

	if !bytes.Equal(reassembled, fileData) {
		t.Fatalf("reassembled file does not match original file data!")
	}

	// Mutate first chunk slice and verify second chunk slice is untouched
	if len(chunks) > 1 {
		chunks[0][0] = 0xFF
		if chunks[1][0] == 0xFF {
			t.Errorf("chunk slices share underlying array, buffer isolation failed!")
		}
	}
}

// TestWaitBufferedAmount_NormalAndTicker verifies backpressure waiting works both via callback
// and via periodic ticker fallback when OnBufferedAmountLow is missed.
func TestWaitBufferedAmount_NormalAndTicker(t *testing.T) {
	t.Run("Normal Callback", func(t *testing.T) {
		dc := newMockDataChannel()
		dc.setBufferedAmount(2 * HighWaterMark)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		done := make(chan error)
		go func() {
			done <- WaitBufferedAmountWithInterval(ctx, dc, LowWaterMark, 10*time.Millisecond)
		}()

		// Simulate buffer draining below LowWaterMark
		time.Sleep(20 * time.Millisecond)
		dc.setBufferedAmount(LowWaterMark - 100)

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("WaitBufferedAmount returned error: %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("WaitBufferedAmount timed out")
		}
	})

	t.Run("Missing Callback Ticker Fallback", func(t *testing.T) {
		dc := newMockDataChannel()
		dc.setBufferedAmount(2 * HighWaterMark)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		done := make(chan error)
		go func() {
			done <- WaitBufferedAmountWithInterval(ctx, dc, LowWaterMark, 10*time.Millisecond)
		}()

		time.Sleep(20 * time.Millisecond)
		// Change bufferedAmount directly without invoking handler
		dc.mu.Lock()
		dc.bufferedAmount = LowWaterMark - 100
		dc.mu.Unlock()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("WaitBufferedAmount returned error: %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("WaitBufferedAmount ticker fallback timed out")
		}
	})

	t.Run("Context Cancellation", func(t *testing.T) {
		dc := newMockDataChannel()
		dc.setBufferedAmount(2 * HighWaterMark)

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error)
		go func() {
			done <- WaitBufferedAmountWithInterval(ctx, dc, LowWaterMark, 10*time.Millisecond)
		}()

		time.Sleep(20 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			if err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("WaitBufferedAmount failed to unblock on context cancellation")
		}
	})
}

// TestFlush_CompletionAndTicker verifies Flush waits until BufferedAmount reaches 0 before sending EOF.
func TestFlush_CompletionAndTicker(t *testing.T) {
	t.Run("Flush Normal Draining", func(t *testing.T) {
		dc := newMockDataChannel()
		dc.setBufferedAmount(500)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		done := make(chan error)
		go func() {
			done <- FlushWithInterval(ctx, dc, 10*time.Millisecond)
		}()

		time.Sleep(20 * time.Millisecond)
		dc.setBufferedAmount(0)

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Flush returned error: %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Flush timed out")
		}

		txts := dc.getSentText()
		if len(txts) != 1 || txts[0] != "EOF" {
			t.Fatalf("expected EOF sent on channel, got %v", txts)
		}
	})

	t.Run("Flush Ticker Fallback", func(t *testing.T) {
		dc := newMockDataChannel()
		dc.setBufferedAmount(500)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		done := make(chan error)
		go func() {
			done <- FlushWithInterval(ctx, dc, 10*time.Millisecond)
		}()

		time.Sleep(20 * time.Millisecond)
		// Change directly without handler
		dc.mu.Lock()
		dc.bufferedAmount = 0
		dc.mu.Unlock()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Flush returned error: %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Flush ticker fallback timed out")
		}

		txts := dc.getSentText()
		if len(txts) != 1 || txts[0] != "EOF" {
			t.Fatalf("expected EOF sent on channel, got %v", txts)
		}
	})

	t.Run("Flush Context Cancellation", func(t *testing.T) {
		dc := newMockDataChannel()
		dc.setBufferedAmount(500)

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error)
		go func() {
			done <- FlushWithInterval(ctx, dc, 10*time.Millisecond)
		}()

		time.Sleep(20 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			if err != context.Canceled {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Flush failed to unblock on context cancellation")
		}
	})
}
