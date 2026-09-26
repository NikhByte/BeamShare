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

	// Create header claiming payload length > 128KB (e.g., 200KB)
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

	// Frame length = 11 bytes (GCM nonce is 12 bytes)
	buf := make([]byte, 4+11)
	binary.BigEndian.PutUint32(buf[0:4], 11)

	decReader, err := NewDecryptingReader(bytes.NewReader(buf), key)
	if err != nil {
		t.Fatalf("NewDecryptingReader failed: %v", err)
	}

	out := make([]byte, 64)
	_, err = decReader.Read(out)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestDecryptingReader_ZeroPerFrameAllocations(t *testing.T) {
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
