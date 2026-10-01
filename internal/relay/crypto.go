package relay

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

const MaxFrameSize = 65564

var (
	ErrFrameTooLarge = errors.New("frame size exceeds maximum allowed limit")
	ErrInvalidFrame  = io.ErrUnexpectedEOF
)

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
	r        io.Reader
	gcm      cipher.AEAD
	buf      []byte
	frameBuf []byte
	plainBuf []byte
	lenBuf   [4]byte
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
		r:        r,
		gcm:      gcm,
		frameBuf: make([]byte, MaxFrameSize),
		plainBuf: make([]byte, MaxFrameSize),
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

	if _, err := io.ReadFull(dr.r, dr.lenBuf[:]); err != nil {
		return 0, err
	}
	length := binary.BigEndian.Uint32(dr.lenBuf[:])

	if length > MaxFrameSize {
		return 0, ErrFrameTooLarge
	}

	nonceSize := dr.gcm.NonceSize()
	if int(length) < nonceSize {
		return 0, ErrInvalidFrame
	}

	frameData := dr.frameBuf[:length]
	if _, err := io.ReadFull(dr.r, frameData); err != nil {
		return 0, err
	}

	nonce := frameData[:nonceSize]
	ciphertext := frameData[nonceSize:]

	plaintext, err := dr.gcm.Open(dr.plainBuf[:0], nonce, ciphertext, nil)
	if err != nil {
		return 0, err
	}

	dr.buf = plaintext
	n := copy(p, dr.buf)
	dr.buf = dr.buf[n:]
	return n, nil
}

