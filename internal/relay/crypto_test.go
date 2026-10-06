package relay

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
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
	invalidKey := make([]byte, 10) // Invalid for AES (requires 16, 24, or 32)
	_, err := NewEncryptingReader(bytes.NewReader([]byte("test")), invalidKey)
	if err == nil {
		t.Fatal("expected error for invalid key length in NewEncryptingReader, got nil")
	}

	_, err = NewDecryptingReader(bytes.NewReader([]byte("test")), invalidKey)
	if err == nil {
		t.Fatal("expected error for invalid key length in NewDecryptingReader, got nil")
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

func TestDecryptingReaderFrameTooLarge(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	testCases := []struct {
		name   string
		length uint32
	}{
		{"MaxFrameSize + 1", MaxFrameSize + 1},
		{"4GB malicious frame", 4 * 1024 * 1024 * 1024 - 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, tc.length)

			decReader, err := NewDecryptingReader(buf, key)
			if err != nil {
				t.Fatalf("NewDecryptingReader failed: %v", err)
			}

			out := make([]byte, 64)
			_, err = decReader.Read(out)
			if err != ErrFrameTooLarge {
				t.Fatalf("expected ErrFrameTooLarge for length %d, got %v", tc.length, err)
			}
		})
	}
}

func TestDecryptingReaderFrameTooSmall(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	testCases := []uint32{0, 5, 11}
	for _, lenVal := range testCases {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, lenVal)
		buf.Write(make([]byte, lenVal))

		decReader, err := NewDecryptingReader(buf, key)
		if err != nil {
			t.Fatalf("NewDecryptingReader failed: %v", err)
		}

		out := make([]byte, 64)
		_, err = decReader.Read(out)
		if err != io.ErrUnexpectedEOF {
			t.Fatalf("expected io.ErrUnexpectedEOF for length %d, got %v", lenVal, err)
		}
	}
}

func TestDecryptingReaderExactMaxFrameSize(t *testing.T) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// 65536 bytes plaintext + 28 bytes nonce+tag = 65564 bytes total frame size
	plaintext := make([]byte, 65536)
	if _, err := io.ReadFull(rand.Reader, plaintext); err != nil {
		t.Fatalf("failed to generate plaintext: %v", err)
	}

	encReader, err := NewEncryptingReader(bytes.NewReader(plaintext), key)
	if err != nil {
		t.Fatalf("NewEncryptingReader failed: %v", err)
	}

	encryptedData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("reading encrypted data failed: %v", err)
	}

	if len(encryptedData) != MaxFrameSize+4 { // 4-byte len header + MaxFrameSize
		t.Fatalf("expected encrypted frame total size %d, got %d", MaxFrameSize+4, len(encryptedData))
	}

	decReader, err := NewDecryptingReader(bytes.NewReader(encryptedData), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	decryptedData, err := io.ReadAll(decReader)
	if err != nil {
		t.Fatalf("reading decrypted data failed: %v", err)
	}

	if !bytes.Equal(decryptedData, plaintext) {
		t.Fatal("decrypted data does not match original plaintext")
	}
}

func BenchmarkDecryptingReader(b *testing.B) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		b.Fatalf("failed to generate key: %v", err)
	}

	plaintext := make([]byte, 65536)
	encReader, err := NewEncryptingReader(bytes.NewReader(plaintext), key)
	if err != nil {
		b.Fatalf("NewEncryptingReader failed: %v", err)
	}
	frameData, err := io.ReadAll(encReader)
	if err != nil {
		b.Fatalf("reading encrypted frame failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(frameData)
		decReader, err := NewDecryptingReader(reader, key)
		if err != nil {
			b.Fatalf("NewDecryptingReader failed: %v", err)
		}

		out := make([]byte, 65536)
		n, err := decReader.Read(out)
		if err != nil && err != io.EOF {
			b.Fatalf("Read failed: %v", err)
		}
		if n != 65536 {
			b.Fatalf("expected 65536 bytes, got %d", n)
		}
	}
}

type repeatableReader struct {
	data []byte
	pos  int
}

func (rr *repeatableReader) Read(p []byte) (int, error) {
	if rr.pos >= len(rr.data) {
		rr.pos = 0
	}
	n := copy(p, rr.data[rr.pos:])
	rr.pos += n
	return n, nil
}

func BenchmarkDecryptingReaderPerFrameNoInitAlloc(b *testing.B) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		b.Fatalf("failed to generate key: %v", err)
	}

	plaintext := make([]byte, 65536)
	encReader, err := NewEncryptingReader(bytes.NewReader(plaintext), key)
	if err != nil {
		b.Fatalf("NewEncryptingReader failed: %v", err)
	}
	frameData, err := io.ReadAll(encReader)
	if err != nil {
		b.Fatalf("reading encrypted frame failed: %v", err)
	}

	rr := &repeatableReader{data: frameData}
	decReader, err := NewDecryptingReader(rr, key)
	if err != nil {
		b.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 65536)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decReader.Read(out)
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}

