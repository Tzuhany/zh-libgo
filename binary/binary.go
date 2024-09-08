package binary

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

type PartLength uint32

type PartFlag uint8

const (
	Binary   PartFlag = 1
	Metadata PartFlag = 2
	Header   PartFlag = 3
)

var (
	ErrDataTooLarge = errors.New("binary data too large")
	ErrNilWriter    = errors.New("binary writer is nil")
)

type BinaryFormatter struct {
	bufMaxSize int64
	bufWriter  *bufio.Writer
}

func NewBinaryFormatter() *BinaryFormatter {
	return &BinaryFormatter{
		bufMaxSize: 1 << 20,
	}
}

func (b *BinaryFormatter) SetBufSize(s int64) {
	b.bufMaxSize = s
}

func (b *BinaryFormatter) NewWriter(w io.Writer) {
	bufWriter := bufio.NewWriterSize(w, 1<<20)
	b.bufWriter = bufWriter
}

func (b *BinaryFormatter) WritePart(flag PartFlag, data []byte) error {
	if b.bufWriter == nil {
		return ErrNilWriter
	}

	byteLen := uint32(len(data))
	if byteLen > ^uint32(0) {
		return ErrDataTooLarge
	}

	err := binary.Write(b.bufWriter, binary.LittleEndian, byteLen)
	if err != nil {
		return err
	}

	err = binary.Write(b.bufWriter, binary.LittleEndian, flag)
	if err != nil {
		return err
	}

	_, err = b.bufWriter.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (b *BinaryFormatter) Sync() error { return b.bufWriter.Flush() }
