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

	// Create a frame claiming length of 5 bytes, which is less than MinFrameLength (28 bytes)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(5))
	buf.Write([]byte("short"))

	decReader, err := NewDecryptingReader(buf, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if !errors.Is(err, ErrInvalidFrameLength) {
		t.Fatalf("expected ErrInvalidFrameLength for frame shorter than MinFrameLength, got %v", err)
	}
}

func TestTruncatedStream(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Create a frame claiming length of 30 bytes (valid bound), but only provide 10 bytes of body
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(30))
	buf.Write(make([]byte, 10))

	decReader, err := NewDecryptingReader(buf, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF for truncated body, got %v", err)
	}
}

func TestFrameHeaderOutOfBoundsLow(t *testing.T) {
	key := make([]byte, 32)
	io.ReadFull(rand.Reader, key)

	for _, invalidLen := range []uint32{0, 1, 5, 27} {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, invalidLen)
		buf.Write(make([]byte, 100)) // Extra trailing bytes that should NOT be read

		decReader, err := NewDecryptingReader(buf, key)
		if err != nil {
			t.Fatalf("NewDecryptingReader failed: %v", err)
		}

		out := make([]byte, 64)
		_, err = decReader.Read(out)
		if !errors.Is(err, ErrInvalidFrameLength) {
			t.Fatalf("expected ErrInvalidFrameLength for length %d, got %v", invalidLen, err)
		}

		// Verify no payload bytes past 4-byte uint32 header were read
		if buf.Len() != 100 {
			t.Fatalf("expected 100 unread bytes remaining in stream for length %d, got %d", invalidLen, buf.Len())
		}
	}
}

func TestFrameHeaderOutOfBoundsHigh(t *testing.T) {
	key := make([]byte, 32)
	io.ReadFull(rand.Reader, key)

	for _, invalidLen := range []uint32{65565, 100000, 0xFFFFFFFF} {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, invalidLen)
		buf.Write(make([]byte, 100)) // Extra trailing bytes that should NOT be read

		decReader, err := NewDecryptingReader(buf, key)
		if err != nil {
			t.Fatalf("NewDecryptingReader failed: %v", err)
		}

		out := make([]byte, 64)
		_, err = decReader.Read(out)
		if !errors.Is(err, ErrInvalidFrameLength) {
			t.Fatalf("expected ErrInvalidFrameLength for length %d, got %v", invalidLen, err)
		}

		// Verify no payload bytes past 4-byte uint32 header were read
		if buf.Len() != 100 {
			t.Fatalf("expected 100 unread bytes remaining in stream for length %d, got %d", invalidLen, buf.Len())
		}
	}
}

func TestSimulated4GBHeaderZeroAllocations(t *testing.T) {
	key := make([]byte, 32)
	io.ReadFull(rand.Reader, key)

	headerBytes := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	r := bytes.NewReader(headerBytes)
	decReader, err := NewDecryptingReader(r, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)

	allocs := testing.AllocsPerRun(100, func() {
		r.Reset(headerBytes)
		_, err := decReader.Read(out)
		if !errors.Is(err, ErrInvalidFrameLength) {
			t.Fatalf("expected ErrInvalidFrameLength, got %v", err)
		}
	})

	if allocs > 0 {
		t.Fatalf("expected 0 allocations on 4GB header rejection, got %f", allocs)
	}
}

func TestZeroHeapAllocationsPerChunk(t *testing.T) {
	key := make([]byte, 32)
	io.ReadFull(rand.Reader, key)

	// Generate encrypted stream containing 200 frames of 64KB
	encReader, err := NewEncryptingReader(bytes.NewReader(make([]byte, 64*1024*200)), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}
	multiChunkData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	rMulti := bytes.NewReader(multiChunkData)
	decReader, err := NewDecryptingReader(rMulti, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64*1024)

	allocs := testing.AllocsPerRun(100, func() {
		n, err := decReader.Read(out)
		if err != nil || n != 64*1024 {
			t.Fatalf("chunk read failed: %v, n=%d", err, n)
		}
	})

	if allocs > 0 {
		t.Fatalf("expected 0 heap allocations per decrypted frame, got %f", allocs)
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
		wantErr     error
	}{
		{name: "One Byte Over MaxFrameSize", claimLength: MaxFrameSize + 1, wantErr: ErrFrameTooLarge},
		{name: "10MB Oversized Frame", claimLength: 10 * 1024 * 1024, wantErr: ErrFrameTooLarge},
		{name: "Max Uint32 Oversized Frame", claimLength: 0xFFFFFFFF, wantErr: ErrFrameTooLarge},
		{name: "Frame Size Exactly MaxFrameSize", claimLength: MaxFrameSize, wantErr: nil},
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
			if tc.wantErr == ErrFrameTooLarge {
				if !errors.Is(err, ErrFrameTooLarge) {
					t.Fatalf("expected ErrFrameTooLarge, got %v", err)
				}
			} else {
				if errors.Is(err, ErrFrameTooLarge) {
					t.Fatalf("did not expect ErrFrameTooLarge, got %v", err)
				}
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

type trackingReader struct {
	data    []byte
	readPos int
}

func (tr *trackingReader) Read(p []byte) (int, error) {
	if tr.readPos >= len(tr.data) {
		return 0, io.EOF
	}
	n := copy(p, tr.data[tr.readPos:])
	tr.readPos += n
	return n, nil
}

func TestOversizedFrameHeader_ImmediateErrorAndNoPayloadRead(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	oversizedLength := uint32(MaxFrameSize + 1024)
	buf := make([]byte, 4+100)
	binary.BigEndian.PutUint32(buf[0:4], oversizedLength)

	tr := &trackingReader{data: buf}
	decReader, err := NewDecryptingReader(tr, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 1024)
	_, err = decReader.Read(out)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}

	// Verify that ONLY the 4-byte header was read from the underlying stream
	if tr.readPos != 4 {
		t.Fatalf("expected 4 bytes read (header only), but %d bytes were read from payload", tr.readPos)
	}
}

func TestOversizedFrameHeader_NoAllocationOrOOM(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Max uint32 length header (4 GB frame length)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf[0:4], 0xFFFFFFFF)

	decReader, err := NewDecryptingReader(bytes.NewReader(buf), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge for 4GB frame header, got %v", err)
	}
}

func TestFrameHeader_LessThanNonceSize_UnexpectedEOF(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Frame length = 11 bytes (GCM nonce is 12 bytes, MinFrameLength is 28 bytes)
	buf := make([]byte, 4+11)
	binary.BigEndian.PutUint32(buf[0:4], 11)

	decReader, err := NewDecryptingReader(bytes.NewReader(buf), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if !errors.Is(err, ErrInvalidFrameLength) {
		t.Fatalf("expected ErrInvalidFrameLength, got %v", err)
	}
}

func TestZeroAllocationsPerFrame(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Prepare multiple chunks of data encrypted with EncryptingReader
	plaintext := make([]byte, 64*1024*5) // 5 frames of 64KB
	if _, err := io.ReadFull(rand.Reader, plaintext); err != nil {
		t.Fatalf("failed to generate plaintext: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(plaintext), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encrypted, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	decReader, err := NewDecryptingReader(bytes.NewReader(encrypted), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	outBuf := make([]byte, 64*1024)

	// Measure heap allocations during frame read loop
	allocs := testing.AllocsPerRun(5, func() {
		_, err := decReader.Read(outBuf)
		if err != nil && err != io.EOF {
			t.Fatalf("unexpected error during read: %v", err)
		}
	})

	if allocs > 0 {
		t.Fatalf("expected 0 per-frame heap allocations, got %f allocs/run", allocs)
	}
}

func TestFrameTooSmall(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	undersizedLengths := []uint32{0, 1, 5, 11} // Less than NonceSize (12)

	for _, size := range undersizedLengths {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, size)

		decReader, err := NewDecryptingReader(bytes.NewReader(header), key)
		if err != nil {
			t.Fatalf("NewDecryptingReader failed: %v", err)
		}

		out := make([]byte, 64)
		_, err = decReader.Read(out)
		if !errors.Is(err, ErrInvalidFrameLength) {
			t.Fatalf("expected ErrInvalidFrameLength for length %d, got %v", size, err)
		}
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

func TestDecryptingReaderZeroAllocations(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	payload := make([]byte, 64*1024)
	if _, err := io.ReadFull(rand.Reader, payload); err != nil {
		t.Fatalf("failed to generate payload: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(payload), key)
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

	readBuf := make([]byte, 64*1024)

	// Warmup 1 read
	n, err := decReader.Read(readBuf)
	if err != nil || n != len(payload) {
		t.Fatalf("warmup read failed: n=%d, err=%v", n, err)
	}

	allocs := testing.AllocsPerRun(100, func() {
		n, err := decReader.Read(readBuf)
		if err != nil {
			t.Fatalf("Read failed during alloc test: %v", err)
		}
		if n != len(payload) {
			t.Fatalf("unexpected read size: %d", n)
		}
	})

	if allocs > 0 {
		t.Fatalf("expected 0 allocations per frame read, got %f", allocs)
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
