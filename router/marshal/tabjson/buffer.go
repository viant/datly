package tabjson

import (
	"io"
)

// Buffer represents itemBuffer
type Buffer struct {
	buffer     []byte
	dataLength int
	offset     int
}

// NewBuffer creates a itemBuffer instance with given initial size
func NewBuffer(size int) *Buffer {
	return &Buffer{
		buffer: make([]byte, size),
	}
}

// writeString add string to the itemBuffer
func (b *Buffer) writeString(value string) {
	if len(value)+b.dataLength > len(b.buffer) {
		b.buffer = append(b.buffer[:b.dataLength], []byte(value)...)
		b.dataLength = len(b.buffer)
		return
	}

	b.dataLength += copy(b.buffer[b.dataLength:], value)
}

// len returns actual itemBuffer dataLength
func (b *Buffer) len() int {
	return b.dataLength
}

// reset sets actual itemBuffer dataLength and offset to 0
func (b *Buffer) reset() {
	b.dataLength = 0
	b.offset = 0
}

// Read reads current item itemBuffer to dest
func (b *Buffer) Read(dest []byte) (int, error) {
	n := copy(dest, b.buffer[b.offset:b.dataLength])
	b.offset += n

	if b.offset == b.dataLength {
		return n, io.EOF
	}
	return n, nil
}
