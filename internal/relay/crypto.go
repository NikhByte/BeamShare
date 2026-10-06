package relay

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

// MaxFrameSize is the maximum allowable payload size for an encrypted frame (1MB).
const MaxFrameSize = 1 * 1024 * 1024

var (
	// ErrFrameTooLarge is returned when a frame length header exceeds MaxFrameSize.
	ErrFrameTooLarge = errors.New("frame size exceeds maximum limit")
	// ErrMaxFrameSizeExceeded is an alias for ErrFrameTooLarge.
	ErrMaxFrameSizeExceeded = ErrFrameTooLarge

	// ErrInvalidKeySize is returned when an encryption key is not 32 bytes.
	ErrInvalidKeySize = ErrInvalidKeyLength
)

type EncryptingReader struct {
	r     io.Reader
	gcm   cipher.AEAD
	buf   []byte // current chunk buffer
	chunk []byte // plaintext read buffer
}

func NewEncryptingReader(r io.Reader, key []byte) (*EncryptingReader, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &EncryptingReader{
		r:     r,
		gcm:   gcm,
		chunk: make([]byte, 65536), // 64KB chunks
	}, nil
}

func (er *EncryptingReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(er.buf) > 0 {
		n := copy(p, er.buf)
		er.buf = er.buf[n:]
		return n, nil
	}

	n, err := er.r.Read(er.chunk)
	if n > 0 {
		nonce := make([]byte, er.gcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return 0, err
		}

		ciphertext := er.gcm.Seal(nil, nonce, er.chunk[:n], nil)

		frame := make([]byte, 4+len(nonce)+len(ciphertext))
		binary.BigEndian.PutUint32(frame[0:4], uint32(len(nonce)+len(ciphertext)))
		copy(frame[4:4+len(nonce)], nonce)
		copy(frame[4+len(nonce):], ciphertext)

		er.buf = frame

		cp := copy(p, er.buf)
		er.buf = er.buf[cp:]
		return cp, nil
	}

	return 0, err
}

type EncryptingWriter struct {
	w   io.Writer
	gcm cipher.AEAD
}

func NewEncryptingWriter(w io.Writer, key []byte) (*EncryptingWriter, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &EncryptingWriter{
		w:   w,
		gcm: gcm,
	}, nil
}

func (ew *EncryptingWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	nonce := make([]byte, ew.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return 0, err
	}

	ciphertext := ew.gcm.Seal(nil, nonce, p, nil)

	frame := make([]byte, 4+len(nonce)+len(ciphertext))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(nonce)+len(ciphertext)))
	copy(frame[4:4+len(nonce)], nonce)
	copy(frame[4+len(nonce):], ciphertext)

	if _, err := ew.w.Write(frame); err != nil {
		return 0, err
	}
	return len(p), nil
}

type DecryptingReader struct {
	r            io.Reader
	gcm          cipher.AEAD
	buf          []byte
	frameBuf     []byte
	plaintextBuf []byte
	headerBuf    [4]byte
}

func NewDecryptingReader(r io.Reader, key []byte) (*DecryptingReader, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &DecryptingReader{
		r:            r,
		gcm:          gcm,
		frameBuf:     make([]byte, MaxFrameSize),
		plaintextBuf: make([]byte, MaxFrameSize),
	}, nil
}

func (dr *DecryptingReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(dr.buf) > 0 {
		n := copy(p, dr.buf)
		dr.buf = dr.buf[n:]
		return n, nil
	}

	if _, err := io.ReadFull(dr.r, dr.headerBuf[:]); err != nil {
		return 0, err
	}
	length := binary.BigEndian.Uint32(dr.headerBuf[:])

	if length > MaxFrameSize {
		return 0, ErrFrameTooLarge
	}

	nonceSize := dr.gcm.NonceSize()
	if int(length) < nonceSize {
		return 0, io.ErrUnexpectedEOF
	}

	frameData := dr.frameBuf[:length]
	if _, err := io.ReadFull(dr.r, frameData); err != nil {
		return 0, err
	}

	nonce := frameData[:nonceSize]
	ciphertext := frameData[nonceSize:]

	plaintext, err := dr.gcm.Open(dr.plaintextBuf[:0], nonce, ciphertext, nil)
	if err != nil {
		return 0, err
	}

	dr.buf = plaintext
	n := copy(p, dr.buf)
	dr.buf = dr.buf[n:]
	return n, nil
}
