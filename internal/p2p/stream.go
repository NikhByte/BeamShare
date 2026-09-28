package p2p

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	// ChunkSize is set to 32KB for optimal WebRTC streaming throughput.
	ChunkSize = 32 * 1024

	// HighWaterMark is the buffer threshold (1MB) above which backpressure pauses reads.
	HighWaterMark = 1024 * 1024

	// LowWaterMark is the buffer threshold (512KB) below which backpressure resumes reads.
	LowWaterMark = 512 * 1024

	// FlushThreshold is a low non-zero threshold (1KB) for buffer low notification before checking zero.
	FlushThreshold = 1024

	// DefaultPollInterval is the interval for ticker checks during backpressure and flush.
	DefaultPollInterval = 20 * time.Millisecond
)

// DataChannel represents the WebRTC DataChannel operations required for streaming.
type DataChannel interface {
	Send(data []byte) error
	SendText(text string) error
	BufferedAmount() uint64
	SetBufferedAmountLowThreshold(threshold uint64)
	OnBufferedAmountLow(f func())
}

// StreamOptions contains optional callbacks and parameters for StartStream.
type StreamOptions struct {
	OnProgress   func(sent int64, total int64)
	OnComplete   func(sentInSession int64, elapsed time.Duration)
	OnError      func(err error)
	PollInterval time.Duration
}

// StreamOption is a functional option for configuring StreamOptions.
type StreamOption func(*StreamOptions)

// WithProgress sets the progress callback.
func WithProgress(f func(sent int64, total int64)) StreamOption {
	return func(o *StreamOptions) {
		o.OnProgress = f
	}
}

// WithComplete sets the completion callback.
func WithComplete(f func(sentInSession int64, elapsed time.Duration)) StreamOption {
	return func(o *StreamOptions) {
		o.OnComplete = f
	}
}

// WithError sets the error callback.
func WithError(f func(err error)) StreamOption {
	return func(o *StreamOptions) {
		o.OnError = f
	}
}

// WithPollInterval sets custom ticker poll interval for backpressure and flush checks.
func WithPollInterval(d time.Duration) StreamOption {
	return func(o *StreamOptions) {
		o.PollInterval = d
	}
}

// StreamSender manages WebRTC file streaming lifecycle and state thread-safely.
type StreamSender struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewStreamSender creates a new thread-safe StreamSender instance.
func NewStreamSender() *StreamSender {
	return &StreamSender{}
}

// Stop cancels any active stream context.
func (s *StreamSender) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
}

// StartStream cancels any prior active stream context before launching a new worker goroutine.
func (s *StreamSender) StartStream(parentCtx context.Context, dc DataChannel, filePath string, fileName string, fileSize int64, offset int64, opts ...StreamOption) {
	options := StreamOptions{
		PollInterval: DefaultPollInterval,
	}
	for _, opt := range opts {
		opt(&options)
	}

	if parentCtx == nil {
		parentCtx = context.Background()
	}

	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	if s.done != nil {
		done := s.done
		s.mu.Unlock()
		<-done
		s.mu.Lock()
	}
	ctx, cancel := context.WithCancel(parentCtx)
	s.cancel = cancel
	s.done = make(chan struct{})
	currentDone := s.done
	s.mu.Unlock()

	go func() {
		defer close(currentDone)
		defer cancel()

		if dc == nil {
			if options.OnError != nil {
				options.OnError(fmt.Errorf("data channel is nil"))
			}
			return
		}

		file, err := os.Open(filePath)
		if err != nil {
			if options.OnError != nil {
				options.OnError(fmt.Errorf("error opening file: %w", err))
			}
			return
		}
		defer file.Close()

		if offset > 0 {
			if _, err := file.Seek(offset, io.SeekStart); err != nil {
				if options.OnError != nil {
					options.OnError(fmt.Errorf("error seeking file: %w", err))
				}
				return
			}
		}

		// Send META header
		metaHeader := fmt.Sprintf("META:%s:%d", fileName, fileSize)
		if errSend := dc.SendText(metaHeader); errSend != nil {
			if options.OnError != nil {
				options.OnError(fmt.Errorf("error sending meta header: %w", errSend))
			}
			return
		}

		readBuf := make([]byte, ChunkSize)
		totalSent := offset
		start := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Backpressure check
			if dc.BufferedAmount() > HighWaterMark {
				if err := WaitBufferedAmountWithInterval(ctx, dc, LowWaterMark, options.PollInterval); err != nil {
					if options.OnError != nil && ctx.Err() == nil {
						options.OnError(err)
					}
					return
				}
			}

			n, readErr := file.Read(readBuf)
			if n > 0 {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Per-chunk buffer isolation: allocate independent chunk slice for every read
				chunk := make([]byte, n)
				copy(chunk, readBuf[:n])

				if errSend := dc.Send(chunk); errSend != nil {
					if options.OnError != nil && ctx.Err() == nil {
						options.OnError(fmt.Errorf("error sending chunk: %w", errSend))
					}
					return
				}

				totalSent += int64(n)
				if options.OnProgress != nil {
					options.OnProgress(totalSent, fileSize)
				}
			}

			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				if options.OnError != nil && ctx.Err() == nil {
					options.OnError(fmt.Errorf("error reading file: %w", readErr))
				}
				return
			}
		}

		// Flush buffer and send EOF
		if err := FlushWithInterval(ctx, dc, options.PollInterval); err != nil {
			if options.OnError != nil && ctx.Err() == nil {
				options.OnError(err)
			}
			return
		}

		if options.OnComplete != nil {
			sentInSession := totalSent - offset
			options.OnComplete(sentInSession, time.Since(start))
		}
	}()
}

// WaitBufferedAmount waits until dc.BufferedAmount() drops to targetThreshold or below.
// It combines OnBufferedAmountLow notifications with periodic ticker polling to prevent hangs.
func WaitBufferedAmount(ctx context.Context, dc DataChannel, targetThreshold uint64) error {
	return WaitBufferedAmountWithInterval(ctx, dc, targetThreshold, DefaultPollInterval)
}

// WaitBufferedAmountWithInterval is WaitBufferedAmount with custom ticker poll interval.
func WaitBufferedAmountWithInterval(ctx context.Context, dc DataChannel, targetThreshold uint64, pollInterval time.Duration) error {
	if dc == nil {
		return nil
	}
	if dc.BufferedAmount() <= targetThreshold {
		return nil
	}

	lowChan := make(chan struct{}, 1)
	dc.SetBufferedAmountLowThreshold(targetThreshold)
	dc.OnBufferedAmountLow(func() {
		select {
		case lowChan <- struct{}{}:
		default:
		}
	})

	if dc.BufferedAmount() <= targetThreshold {
		return nil
	}

	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-lowChan:
			if dc.BufferedAmount() <= targetThreshold {
				return nil
			}
		case <-ticker.C:
			if dc.BufferedAmount() <= targetThreshold {
				return nil
			}
		}
	}
}

// Flush waits until dc.BufferedAmount() reaches 0 using a low non-zero threshold combined
// with ticker polling before sending "EOF".
func Flush(ctx context.Context, dc DataChannel) error {
	return FlushWithInterval(ctx, dc, DefaultPollInterval)
}

// FlushWithInterval is Flush with custom ticker poll interval.
func FlushWithInterval(ctx context.Context, dc DataChannel, pollInterval time.Duration) error {
	if dc == nil {
		return nil
	}
	if dc.BufferedAmount() == 0 {
		return dc.SendText("EOF")
	}

	lowChan := make(chan struct{}, 1)
	dc.SetBufferedAmountLowThreshold(FlushThreshold)
	dc.OnBufferedAmountLow(func() {
		select {
		case lowChan <- struct{}{}:
		default:
		}
	})

	if dc.BufferedAmount() == 0 {
		return dc.SendText("EOF")
	}

	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-lowChan:
			if dc.BufferedAmount() == 0 {
				return dc.SendText("EOF")
			}
		case <-ticker.C:
			if dc.BufferedAmount() == 0 {
				return dc.SendText("EOF")
			}
		}
	}
}
