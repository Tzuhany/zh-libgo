package binary

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

type PartLength uint32

type PartFlag uint8

const bufDefaultSize = 1 << 20

const (
	Binary   PartFlag = 1
	Metadata PartFlag = 2
	Header   PartFlag = 3
)

var (
	ErrDataTooLarge = errors.New("binary data too large")
	ErrNilWriter    = errors.New("binary writer is nil")
	ErrNilReader    = errors.New("binary reader is nil")
)

// BinaryFormatter provides functionality for formatting binary data with buffering
type BinaryFormatter struct {
	bufMaxSize int
	bufWriter  *bufio.Writer
	bufReader  *bufio.Reader
}

func NewBinaryFormatter() *BinaryFormatter {
	return &BinaryFormatter{
		bufMaxSize: 1 << 20,
	}
}

func (b *BinaryFormatter) SetBufSize(s int) {
	b.bufMaxSize = s
}

func (b *BinaryFormatter) NewWriter(w io.Writer) {
	if b.bufMaxSize == 0 {
		b.bufMaxSize = bufDefaultSize
	}
	b.bufWriter = bufio.NewWriterSize(w, int(b.bufMaxSize))
}

// WritePart writes a data part to the buffered writer, including the flag and data length
func (b *BinaryFormatter) WritePart(flag PartFlag, data []byte) error {
	if b.bufWriter == nil {
		return ErrNilWriter
	}

	byteLen := uint32(len(data))
	if byteLen > ^uint32(0) {
		return ErrDataTooLarge
	}

	err := binary.Write(b.bufWriter, binary.LittleEndian, flag)
	if err != nil {
		return err
	}

	err = binary.Write(b.bufWriter, binary.LittleEndian, byteLen)
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

func (b *BinaryFormatter) NewReader(r io.Reader) {
	if b.bufMaxSize == 0 {
		b.bufMaxSize = bufDefaultSize
	}
	b.bufReader = bufio.NewReaderSize(r, b.bufMaxSize)
}

// ReadPart reads a data part from the buffered reader, including the flag and data
func (b *BinaryFormatter) ReadPart() (flag PartFlag, data []byte, err error) {
	if b.bufReader == nil {
		return 0, nil, ErrNilReader
	}

	// read flag
	err = binary.Read(b.bufReader, binary.LittleEndian, &flag)
	if err != nil {
		return 0, nil, err
	}

	// read data length
	var byteLen PartLength
	err = binary.Read(b.bufReader, binary.LittleEndian, &byteLen)
	if err != nil {
		return 0, nil, err
	}

	if uint32(byteLen) > ^uint32(0) {
		return 0, nil, ErrDataTooLarge
	}

	// read data
	data = make([]byte, byteLen)
	_, err = io.ReadFull(b.bufReader, data)
	if err == io.EOF {
		return 0, nil, io.EOF
	} else if err != nil {
		return 0, nil, err
	}

	return flag, data, nil
}
