package relay

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// 150KB to test multi-chunk encryption (chunk size is 64KB)
	originalData := make([]byte, 150*1024)
	if _, err := io.ReadFull(rand.Reader, originalData); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(originalData), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	if bytes.Equal(encryptedData, originalData) {
		t.Fatal("encrypted data matches plaintext")
	}

	decReader, err := NewDecryptingReader(bytes.NewReader(encryptedData), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	decryptedData, err := io.ReadAll(decReader)
	if err != nil {
		t.Fatalf("reading decrypted data failed: %v", err)
	}

	if !bytes.Equal(decryptedData, originalData) {
		t.Fatal("decrypted data does not match original data")
	}
}

func TestEncryptDecryptEmptyData(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader([]byte{}), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	decReader, err := NewDecryptingReader(bytes.NewReader(encryptedData), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	decryptedData, err := io.ReadAll(decReader)
	if err != nil {
		t.Fatalf("reading decrypted data failed: %v", err)
	}

	if len(decryptedData) != 0 {
		t.Fatalf("expected empty decrypted data, got %d bytes", len(decryptedData))
	}
}

func TestInvalidKeyLengths(t *testing.T) {
	invalidKeyLengths := []int{1, 10, 16, 24, 31, 33, 64}
	for _, length := range invalidKeyLengths {
		invalidKey := make([]byte, length)
		_, err := NewEncryptingReader(bytes.NewReader([]byte("test")), invalidKey)
		if err == nil {
			t.Fatalf("expected error for key length %d in NewEncryptingReader, got nil", length)
		}

		_, err = NewDecryptingReader(bytes.NewReader([]byte("test")), invalidKey)
		if err == nil {
			t.Fatalf("expected error for key length %d in NewEncryptingReader, got nil", length)
		}
	}
}

func TestTamperedCiphertextVerification(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	originalData := []byte("secret payload that should be tamper-proof")
	encReader, err := NewEncryptingReader(bytes.NewReader(originalData), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	// Tamper with the last byte (authentication tag or ciphertext)
	encryptedData[len(encryptedData)-1] ^= 0xFF

	decReader, err := NewDecryptingReader(bytes.NewReader(encryptedData), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	_, err = io.ReadAll(decReader)
	if err == nil {
		t.Fatal("expected decryption/tag verification error on tampered data, got nil")
	}
}

func TestTruncatedFrame(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Create a frame claiming length of 5 bytes, which is less than GCM NonceSize (12 bytes)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(5))
	buf.Write([]byte("short"))

	decReader, err := NewDecryptingReader(buf, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF for frame shorter than nonce, got %v", err)
	}
}

func TestNonceUniquenessAcrossChunks(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// 130KB will produce at least two 64KB chunks
	data := make([]byte, 130*1024)
	encReader, err := NewEncryptingReader(bytes.NewReader(data), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	// Extract first frame's nonce
	if len(encryptedData) < 16 {
		t.Fatal("encrypted data too short")
	}
	len1 := binary.BigEndian.Uint32(encryptedData[0:4])
	nonce1 := encryptedData[4 : 4+12]

	offset2 := 4 + int(len1)
	if len(encryptedData) < offset2+16 {
		t.Fatal("second frame missing")
	}
	nonce2 := encryptedData[offset2+4 : offset2+4+12]

	if bytes.Equal(nonce1, nonce2) {
		t.Fatal("consecutive frames reused the same nonce")
	}
}

func TestMaxFrameSizeExceeded(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	testCases := []struct {
		name        string
		claimLength uint32
	}{
		{name: "One Byte Over MaxFrameSize", claimLength: MaxFrameSize + 1},
		{name: "10MB Oversized Frame", claimLength: 10 * 1024 * 1024},
		{name: "Max Uint32 Oversized Frame", claimLength: 0xFFFFFFFF},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, tc.claimLength)

			decReader, err := NewDecryptingReader(buf, key)
			if err != nil {
				t.Fatalf("NewDecryptingReader failed: %v", err)
			}

			out := make([]byte, 64)
			_, err = decReader.Read(out)
			if err == nil {
				t.Fatalf("expected error for frame size %d exceeding MaxFrameSize, got nil", tc.claimLength)
			}
			if !errors.Is(err, ErrFrameTooLarge) {
				t.Fatalf("expected ErrFrameTooLarge (%v), got %v", ErrFrameTooLarge, err)
			}
		})
	}

	t.Run("Exact MaxFrameSize boundary passes length check", func(t *testing.T) {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, uint32(MaxFrameSize))

		decReader, err := NewDecryptingReader(buf, key)
		if err != nil {
			t.Fatalf("NewDecryptingReader failed: %v", err)
		}

		out := make([]byte, 64)
		_, err = decReader.Read(out)
		if errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("expected length check to pass for MaxFrameSize, but got ErrFrameTooLarge")
		}
	})
}

type trackedReader struct {
	header    []byte
	bytesRead int
}

func (tr *trackedReader) Read(p []byte) (int, error) {
	if tr.bytesRead < len(tr.header) {
		n := copy(p, tr.header[tr.bytesRead:])
		tr.bytesRead += n
		return n, nil
	}
	tr.bytesRead += len(p)
	return len(p), nil
}

func TestFrameSizeExceedsLimit(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Header claiming 2MB frame length (> 1MB MaxFrameSize)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 2*1024*1024)

	tr := &trackedReader{header: header}
	decReader, err := NewDecryptingReader(tr, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if err == nil {
		t.Fatal("expected error for frame exceeding 1MB limit, got nil")
	}

	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge error, got: %v", err)
	}

	// Verify that ONLY the 4-byte header was read from the underlying reader
	if tr.bytesRead != 4 {
		t.Fatalf("expected 4 bytes read (header only), but %d bytes were read from reader", tr.bytesRead)
	}
}

func TestFrameSizeZeroOrSmallerThanNonce(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Header claiming 0 length
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 0)

	tr := &trackedReader{header: header}
	decReader, err := NewDecryptingReader(tr, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF for zero length frame, got %v", err)
	}

	if tr.bytesRead != 4 {
		t.Fatalf("expected 4 bytes read (header only), but %d bytes were read from reader", tr.bytesRead)
	}
}

type repeatingReader struct {
	data   []byte
	offset int
}

func (r *repeatingReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n := 0
	for n < len(p) {
		if r.offset >= len(r.data) {
			r.offset = 0
		}
		copied := copy(p[n:], r.data[r.offset:])
		r.offset += copied
		n += copied
	}
	return n, nil
}

func TestDecryptingReader_BufferReuseAndZeroAllocations(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	data := make([]byte, 64*1024*10) // 640KB
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("failed to generate data: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(data), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("io.ReadAll failed: %v", err)
	}

	repeater := &repeatingReader{data: encryptedData}
	decReader, err := NewDecryptingReader(repeater, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	outBuf := make([]byte, 64*1024)
	// Warm up first read call
	_, err = decReader.Read(outBuf)
	if err != nil {
		t.Fatalf("warmup read failed: %v", err)
	}

	allocs := testing.AllocsPerRun(10, func() {
		_, err := decReader.Read(outBuf)
		if err != nil {
			t.Fatalf("Read failed during alloc test: %v", err)
		}
	})

	if allocs > 0 {
		t.Fatalf("expected 0 allocations per frame read call, got %f", allocs)
	}
}

func BenchmarkDecryptingReaderRead(b *testing.B) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		b.Fatalf("failed to generate key: %v", err)
	}

	payload := make([]byte, 64*1024)
	if _, err := io.ReadFull(rand.Reader, payload); err != nil {
		b.Fatalf("failed to generate payload: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(payload), key)
	if err != nil {
		b.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		b.Fatalf("io.ReadAll failed: %v", err)
	}

	repeater := &repeatingReader{data: encryptedData}
	decReader, err := NewDecryptingReader(repeater, key)
	if err != nil {
		b.Fatalf("NewDecryptingReader failed: %v", err)
	}

	readBuf := make([]byte, 64*1024)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := decReader.Read(readBuf)
		if err != nil {
			b.Fatalf("Read failed during benchmark: %v", err)
		}
	}
}

func TestMaxFrameSizeBoundary(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// 64KB plaintext payload matches the max chunk size
	payload := make([]byte, 65536)
	if _, err := io.ReadFull(rand.Reader, payload); err != nil {
		t.Fatalf("failed to generate payload: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(payload), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	// Verify header length is <= MaxFrameSize
	frameLen := binary.BigEndian.Uint32(encryptedData[0:4])
	if frameLen > MaxFrameSize {
		t.Fatalf("expected frame length %d to be <= MaxFrameSize (%d)", frameLen, MaxFrameSize)
	}

	decReader, err := NewDecryptingReader(bytes.NewReader(encryptedData), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	decryptedData, err := io.ReadAll(decReader)
	if err != nil {
		t.Fatalf("decryption of frame failed: %v", err)
	}

	if !bytes.Equal(decryptedData, payload) {
		t.Fatal("decrypted payload does not match original payload")
	}
}

func TestBufferReuseZeroAllocations(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	data := make([]byte, 64*1024*10) // 10 chunks of 64KB
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(data), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	decReader, err := NewDecryptingReader(bytes.NewReader(encryptedData), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64*1024)

	allocs := testing.AllocsPerRun(8, func() {
		_, _ = decReader.Read(out)
	})

	if allocs > 0 {
		t.Fatalf("expected 0 heap allocations per frame Read call, got %f", allocs)
	}
}

