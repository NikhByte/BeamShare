package p2p

import (
	"context"
	"time"
)

// DataChannel defines the interface required for WebRTC flow control.
type DataChannel interface {
	BufferedAmount() uint64
	SetBufferedAmountLowThreshold(threshold uint64)
	OnBufferedAmountLow(f func())
}

// WaitBufferedAmount waits until dc.BufferedAmount() falls to or below target threshold.
// If maxAllowed > 0 and dc.BufferedAmount() <= maxAllowed, it returns immediately without waiting.
// It attaches the OnBufferedAmountLow callback BEFORE double-checking dc.BufferedAmount(),
// supplemented by a periodic polling safety net (20ms ticker) to eliminate race condition deadlocks.
func WaitBufferedAmount(ctx context.Context, dc DataChannel, lowThreshold uint64, maxAllowed uint64) error {
	if dc == nil {
		return nil
	}

	// Immediate check if under maxAllowed threshold (when maxAllowed > 0)
	if maxAllowed > 0 && dc.BufferedAmount() <= maxAllowed {
		return nil
	}
	if maxAllowed == 0 && dc.BufferedAmount() <= lowThreshold {
		return nil
	}

	dc.SetBufferedAmountLowThreshold(lowThreshold)

	signalChan := make(chan struct{}, 1)

	// 1. Attach listener BEFORE checking buffer levels
	dc.OnBufferedAmountLow(func() {
		select {
		case signalChan <- struct{}{}:
		default:
		}
	})
	defer dc.OnBufferedAmountLow(nil)

	// 2. Double-check buffer level RIGHT AFTER registering listener
	if dc.BufferedAmount() <= lowThreshold {
		return nil
	}

	// 3. Periodic polling safety net (20ms ticker)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-signalChan:
			if dc.BufferedAmount() <= lowThreshold {
				return nil
			}
		case <-ticker.C:
			if dc.BufferedAmount() <= lowThreshold {
				return nil
			}
		}
	}
}
