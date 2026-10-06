package p2p_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beamshare/beam/internal/p2p"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDataChannel struct {
	mu            sync.Mutex
	buffered      uint64
	threshold     uint64
	callback      func()
	callbackCount int32
}

func (m *mockDataChannel) BufferedAmount() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buffered
}

func (m *mockDataChannel) SetBufferedAmountLowThreshold(threshold uint64) {
	m.mu.Lock()
	m.threshold = threshold
	m.mu.Unlock()
}

func (m *mockDataChannel) OnBufferedAmountLow(f func()) {
	m.mu.Lock()
	m.callback = f
	if f != nil {
		atomic.AddInt32(&m.callbackCount, 1)
	}
	m.mu.Unlock()
}

func (m *mockDataChannel) setBufferAndTrigger(amount uint64) {
	m.mu.Lock()
	m.buffered = amount
	cb := m.callback
	thresh := m.threshold
	m.mu.Unlock()

	if amount <= thresh && cb != nil {
		cb()
	}
}

func TestWaitBufferedAmount_ImmediateUnderMax(t *testing.T) {
	dc := &mockDataChannel{buffered: 500 * 1024}
	ctx := context.Background()

	err := p2p.WaitBufferedAmount(ctx, dc, 512*1024, 1024*1024)
	assert.NoError(t, err)
	assert.Nil(t, dc.callback)
}

func TestWaitBufferedAmount_DoubleCheckResolution(t *testing.T) {
	dc := &mockDataChannel{buffered: 2000 * 1024}
	ctx := context.Background()

	// Drain buffer right when threshold is set
	done := make(chan error, 1)
	go func() {
		done <- p2p.WaitBufferedAmount(ctx, dc, 512*1024, 1024*1024)
	}()

	// Simulating buffer draining right before wait loop
	time.Sleep(5 * time.Millisecond)
	dc.setBufferAndTrigger(400 * 1024)

	err := <-done
	assert.NoError(t, err)
	assert.Nil(t, dc.callback, "Callback should be cleaned up on completion")
}

func TestWaitBufferedAmount_CallbackTrigger(t *testing.T) {
	dc := &mockDataChannel{buffered: 2000 * 1024}
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		done <- p2p.WaitBufferedAmount(ctx, dc, 512*1024, 1024*1024)
	}()

	time.Sleep(10 * time.Millisecond)
	dc.setBufferAndTrigger(100 * 1024)

	select {
	case err := <-done:
		assert.NoError(t, err)
		assert.Nil(t, dc.callback, "Callback should be cleaned up")
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for WaitBufferedAmount")
	}
}

func TestWaitBufferedAmount_PollingSafetyFallback(t *testing.T) {
	dc := &mockDataChannel{buffered: 2000 * 1024}
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		done <- p2p.WaitBufferedAmount(ctx, dc, 512*1024, 1024*1024)
	}()

	time.Sleep(10 * time.Millisecond)
	// Lower buffer WITHOUT calling callback, testing polling fallback
	dc.mu.Lock()
	dc.buffered = 100 * 1024
	dc.mu.Unlock()

	select {
	case err := <-done:
		assert.NoError(t, err)
		assert.Nil(t, dc.callback, "Callback should be cleaned up")
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for WaitBufferedAmount polling fallback")
	}
}

func TestWaitBufferedAmount_ContextCancellation(t *testing.T) {
	dc := &mockDataChannel{buffered: 2000 * 1024}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := p2p.WaitBufferedAmount(ctx, dc, 512*1024, 1024*1024)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, dc.callback, "Callback should be cleaned up after context cancellation")
}

func TestWaitBufferedAmount_EOFFlush(t *testing.T) {
	dc := &mockDataChannel{buffered: 100 * 1024}
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		done <- p2p.WaitBufferedAmount(ctx, dc, 0, 0)
	}()

	time.Sleep(10 * time.Millisecond)
	dc.setBufferAndTrigger(0)

	select {
	case err := <-done:
		assert.NoError(t, err)
		assert.Nil(t, dc.callback)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for EOF flush")
	}
}
