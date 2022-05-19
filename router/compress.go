package router

import (
	"bytes"
	"compress/gzip"
	"io"
)

//Compress compresses input using gzip
func Compress(reader io.Reader) (*bytes.Buffer, error) {
	buffer := new(bytes.Buffer)
	writer := gzip.NewWriter(buffer)
	_, err := io.Copy(writer, reader)
	if err != nil {
		return nil, err
	}

	_ = writer.Flush()
	err = writer.Close()
	return buffer, err
}
