package relay

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
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

	// Plaintext length such that 4 + nonce (12) + len(plaintext) + tag (16) == MaxFrameSize
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

