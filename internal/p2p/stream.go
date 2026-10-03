package p2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

var (
	// ErrAlreadyStreaming is returned when a stream transfer is already active for this sender.
	ErrAlreadyStreaming = errors.New("stream transfer already in progress")
)

const (
	// DefaultChunkSize is the size of each file chunk (32 KB, below Pion 65535 SCTP limit).
	DefaultChunkSize = 32 * 1024
	// HighWaterMark is the buffer threshold above which streaming pauses (1 MB).
	HighWaterMark = 1024 * 1024
	// LowWaterMark is the buffer threshold for low buffer notifications (512 KB).
	LowWaterMark = 512 * 1024
	// PollingInterval is the ticker interval for backpressure and flush loop evaluations.
	PollingInterval = 50 * time.Millisecond
)

// DataChannel defines the interface for WebRTC DataChannel operations required by StreamSender.
type DataChannel interface {
	Send(data []byte) error
	SendText(text string) error
	BufferedAmount() uint64
	SetBufferedAmountLowThreshold(threshold uint64)
	OnBufferedAmountLow(f func())
}

// StreamSender manages single-flight file chunk streaming over a WebRTC DataChannel.
type StreamSender struct {
	dc DataChannel

	mu        sync.Mutex
	streaming bool
	cancel    context.CancelFunc

	OnProgress func(sent, total int64)
	OnComplete func(sentInSession int64, elapsed time.Duration)
}

// NewStreamSender creates a new StreamSender instance bound to a DataChannel.
func NewStreamSender(dc DataChannel) *StreamSender {
	return &StreamSender{
		dc: dc,
	}
}

// IsStreaming returns true if a transfer is currently active.
func (s *StreamSender) IsStreaming() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streaming
}

// Stop cancels any ongoing stream transfer.
func (s *StreamSender) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
}

// StartStream begins streaming the file from the given offset over the DataChannel.
// It enforces single-flight execution: if a stream is already active, it returns ErrAlreadyStreaming.
func (s *StreamSender) StartStream(ctx context.Context, filePath, fileName string, fileSize, offset int64) error {
	s.mu.Lock()
	if s.streaming {
		s.mu.Unlock()
		return ErrAlreadyStreaming
	}
	s.streaming = true
	streamCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.streaming = false
		s.cancel = nil
		s.mu.Unlock()
	}()

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	if offset > 0 {
		if _, err = file.Seek(offset, io.SeekStart); err != nil {
			return fmt.Errorf("error seeking file: %w", err)
		}
	}

	// Send META header
	metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
	if errSend := s.dc.SendText(metaHeader); errSend != nil {
		return fmt.Errorf("error sending meta header: %w", errSend)
	}

	bufferedAmountLowChan := make(chan struct{}, 1)
	s.dc.SetBufferedAmountLowThreshold(LowWaterMark)
	s.dc.OnBufferedAmountLow(func() {
		select {
		case bufferedAmountLowChan <- struct{}{}:
		default:
		}
	})

	readBuf := make([]byte, DefaultChunkSize)
	totalSent := offset
	start := time.Now()

	for {
		select {
		case <-streamCtx.Done():
			return streamCtx.Err()
		default:
		}

		// Requirement 3: Dynamic backpressure loop with 50ms polling fallback
		for s.dc.BufferedAmount() > HighWaterMark {
			select {
			case <-streamCtx.Done():
				return streamCtx.Err()
			case <-bufferedAmountLowChan:
			case <-time.After(PollingInterval):
			}
		}

		n, errRead := file.Read(readBuf)
		if n > 0 {
			// Requirement 2: Isolate byte slice via per-chunk copy
			chunk := make([]byte, n)
			copy(chunk, readBuf[:n])

			if errSend := s.dc.Send(chunk); errSend != nil {
				return fmt.Errorf("error sending chunk: %w", errSend)
			}
			totalSent += int64(n)

			if s.OnProgress != nil {
				s.OnProgress(totalSent, fileSize)
			}
		}

		if errRead != nil {
			if errors.Is(errRead, io.EOF) {
				break
			}
			return fmt.Errorf("error reading file: %w", errRead)
		}
	}

	// Requirement 4: Wait for buffer level to reach zero before sending EOF string
	s.dc.SetBufferedAmountLowThreshold(0)
	for s.dc.BufferedAmount() > 0 {
		select {
		case <-streamCtx.Done():
			return streamCtx.Err()
		case <-bufferedAmountLowChan:
		case <-time.After(PollingInterval):
		}
	}

	if errSend := s.dc.SendText("EOF"); errSend != nil {
		return fmt.Errorf("error sending EOF: %w", errSend)
	}

	if s.OnComplete != nil {
		s.OnComplete(totalSent-offset, time.Since(start))
	}

	return nil
}
