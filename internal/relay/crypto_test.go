package relay

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
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

type trackingReader struct {
	r     io.Reader
	nRead int
}

func (tr *trackingReader) Read(p []byte) (int, error) {
	n, err := tr.r.Read(p)
	tr.nRead += n
	return n, err
}

func TestDecryptingReader_FrameTooLarge(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Create a length header of MaxFrameSize + 1 (131,073 bytes) followed by payload data
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(MaxFrameSize+1))
	buf.Write(make([]byte, 2000)) // trailing payload that shouldn't be read

	tr := &trackingReader{r: buf}
	decReader, err := NewDecryptingReader(tr, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if err != ErrFrameTooLarge {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}

	if tr.nRead != 4 {
		t.Fatalf("expected only 4 bytes (the length header) to be read, but read %d bytes", tr.nRead)
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

func TestDecryptingReader_FrameTooShortBeforeRead(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Create a length header claiming 11 bytes (< 12-byte NonceSize)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(11))
	buf.Write(make([]byte, 20)) // trailing payload that shouldn't be read

	tr := &trackingReader{r: buf}
	decReader, err := NewDecryptingReader(tr, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}

	if tr.nRead != 4 {
		t.Fatalf("expected only 4 bytes (the length header) to be read, but read %d bytes", tr.nRead)
	}
}

func TestDecryptingReader_ExactMaxFrameSize(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("aes.NewCipher failed: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("cipher.NewGCM failed: %v", err)
	}

	// Plaintext length such that nonce (12) + len(plaintext) + tag (16) == MaxFrameSize
	// So frame payload length == MaxFrameSize = 131,072 bytes.
	plaintextLen := MaxFrameSize - gcm.NonceSize() - gcm.Overhead()
	plaintext := make([]byte, plaintextLen)
	if _, err := io.ReadFull(rand.Reader, plaintext); err != nil {
		t.Fatalf("failed to generate plaintext: %v", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		t.Fatalf("failed to generate nonce: %v", err)
	}

	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(nonce)+len(ciphertext)))
	buf.Write(nonce)
	buf.Write(ciphertext)

	if buf.Len() != 4+MaxFrameSize {
		t.Fatalf("expected frame buffer len %d, got %d", 4+MaxFrameSize, buf.Len())
	}

	decReader, err := NewDecryptingReader(buf, key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	decrypted, err := io.ReadAll(decReader)
	if err != nil {
		t.Fatalf("failed to decrypt max frame size payload: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Fatal("decrypted payload does not match original plaintext for max frame size")
	}
}

func TestDecryptingReader_ZeroAllocationsPerFrame(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Generate 10 frames
	data := make([]byte, 10*64*1024)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("failed to generate data: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(data), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("failed to read encrypted data: %v", err)
	}

	decReader, err := NewDecryptingReader(bytes.NewReader(encData), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 65536)
	allocs := testing.AllocsPerRun(10, func() {
		_, _ = decReader.Read(out)
	})

	if allocs > 0 {
		t.Fatalf("expected 0 allocs per frame read, got %f", allocs)
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
		if err != io.ErrUnexpectedEOF {
			t.Fatalf("expected io.ErrUnexpectedEOF for length %d, got %v", size, err)
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
