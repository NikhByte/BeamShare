package relay

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

// MaxFrameSize defines the maximum allowed encrypted frame size (64KB payload + 12B nonce + 16B GCM tag).
const MaxFrameSize = 65564

// ErrFrameTooLarge is returned when an incoming encrypted frame exceeds MaxFrameSize.
var ErrFrameTooLarge = errors.New("frame too large")

type EncryptingReader struct {
	r     io.Reader
	gcm   cipher.AEAD
	buf   []byte // current chunk buffer
	chunk []byte // plaintext read buffer
}

func NewEncryptingReader(r io.Reader, key []byte) (*EncryptingReader, error) {
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

type DecryptingReader struct {
	r   io.Reader
	gcm cipher.AEAD
	buf []byte
}

func NewDecryptingReader(r io.Reader, key []byte) (*DecryptingReader, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &DecryptingReader{
		r:   r,
		gcm: gcm,
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

	var length uint32
	if err := binary.Read(dr.r, binary.BigEndian, &length); err != nil {
		return 0, err
	}

	if length > MaxFrameSize {
		return 0, ErrFrameTooLarge
	}

	nonceSize := dr.gcm.NonceSize()
	if int(length) < nonceSize {
		return 0, io.ErrUnexpectedEOF
	}

	frameData := make([]byte, length)
	if _, err := io.ReadFull(dr.r, frameData); err != nil {
		return 0, err
	}

	nonce := frameData[:nonceSize]
	ciphertext := frameData[nonceSize:]

	plaintext, err := dr.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return 0, err
	}

	dr.buf = plaintext
	n := copy(p, dr.buf)
	dr.buf = dr.buf[n:]
	return n, nil
}
