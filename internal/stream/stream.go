package stream

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// DataChannel defines the interface for WebRTC data channel operations.
type DataChannel interface {
	SendText(text string) error
	Send(data []byte) error
	BufferedAmount() uint64
}

// Manager manages single-flight WebRTC file stream transfers per session.
type Manager struct {
	mu         sync.Mutex
	cancelFunc context.CancelFunc
}

// NewManager creates a new single-flight stream manager.
func NewManager() *Manager {
	return &Manager{}
}

// StreamOptions contains configuration and callbacks for a file stream transfer.
type StreamOptions struct {
	FilePath     string
	FileName     string
	FileSize     int64
	Offset       int64
	ChunkSize    int           // Chunk size in bytes (default: 64KB)
	HighWater    uint64        // Pause reads if BufferedAmount > HighWater (default: 1MB = 1024*1024)
	LowWater     uint64        // Resume reads when BufferedAmount <= LowWater (default: 512KB = 512*1024)
	PollInterval time.Duration // Polling sleep duration (default: 5ms)
	OnProgress   func(totalSent int64, fileSize int64)
	OnComplete   func(sentInSession int64, duration time.Duration)
	OnError      func(err error)
}

// StartStream cancels any active worker goroutine and starts a new worker streaming from the requested offset.
func (m *Manager) StartStream(parentCtx context.Context, dc DataChannel, opts StreamOptions) {
	m.mu.Lock()
	if m.cancelFunc != nil {
		m.cancelFunc()
	}

	if parentCtx == nil {
		parentCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(parentCtx)
	m.cancelFunc = cancel
	m.mu.Unlock()

	go m.runWorker(ctx, dc, opts)
}

func (m *Manager) runWorker(ctx context.Context, dc DataChannel, opts StreamOptions) {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 64 * 1024
	}
	if opts.HighWater == 0 {
		opts.HighWater = 1024 * 1024 // 1MB
	}
	if opts.LowWater == 0 {
		opts.LowWater = 512 * 1024 // 512KB
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 5 * time.Millisecond
	}

	file, err := os.Open(opts.FilePath)
	if err != nil {
		if opts.OnError != nil {
			opts.OnError(fmt.Errorf("open file: %w", err))
		}
		return
	}
	defer file.Close()

	if opts.Offset > 0 {
		_, err = file.Seek(opts.Offset, io.SeekStart)
		if err != nil {
			if opts.OnError != nil {
				opts.OnError(fmt.Errorf("seek file: %w", err))
			}
			return
		}
	}

	select {
	case <-ctx.Done():
		return
	default:
	}

	// Send META header
	metaHeader := fmt.Sprintf("META:%s:%d", opts.FileName, opts.FileSize)
	if errSend := dc.SendText(metaHeader); errSend != nil {
		if opts.OnError != nil {
			opts.OnError(fmt.Errorf("send meta header: %w", errSend))
		}
		return
	}

	readBuf := make([]byte, opts.ChunkSize)
	totalSent := opts.Offset
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Active Backpressure Check (Polling)
		// When buffered data exceeds HighWater (1MB), poll until buffered data drops below LowWater (512KB) or context cancels.
		if dc.BufferedAmount() > opts.HighWater {
			for dc.BufferedAmount() > opts.LowWater {
				select {
				case <-ctx.Done():
					return
				default:
					time.Sleep(opts.PollInterval)
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		n, errRead := file.Read(readBuf)
		if n > 0 {
			// Immutable Chunk Buffer Allocation:
			// Allocate a fresh byte slice per chunk send before calling dc.Send()
			chunk := make([]byte, n)
			copy(chunk, readBuf[:n])

			errSend := dc.Send(chunk)
			if errSend != nil {
				if opts.OnError != nil {
					opts.OnError(fmt.Errorf("send chunk: %w", errSend))
				}
				return
			}

			totalSent += int64(n)
			if opts.OnProgress != nil {
				opts.OnProgress(totalSent, opts.FileSize)
			}
		}

		if errRead != nil {
			if errRead != io.EOF && opts.OnError != nil {
				opts.OnError(fmt.Errorf("read file: %w", errRead))
			}
			break
		}
	}

	// Deterministic EOF Flush (Polling)
	// Poll until dc.BufferedAmount() == 0 or context cancels before sending EOF
	for dc.BufferedAmount() > 0 {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(opts.PollInterval)
		}
	}

	select {
	case <-ctx.Done():
		return
	default:
	}

	if errSend := dc.SendText("EOF"); errSend != nil {
		if opts.OnError != nil {
			opts.OnError(fmt.Errorf("send EOF: %w", errSend))
		}
		return
	}

	elapsed := time.Since(start)
	sentInSession := totalSent - opts.Offset
	if opts.OnComplete != nil {
		opts.OnComplete(sentInSession, elapsed)
	}
}

// Stop cancels any currently active streaming worker.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancelFunc != nil {
		m.cancelFunc()
		m.cancelFunc = nil
	}
}
