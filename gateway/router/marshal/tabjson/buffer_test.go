package tabjson

import (
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestBuffer_WriteString(t *testing.T) {
	testCases := []struct {
		description    string
		initialSize    int
		data           []string
		buffer         []byte
		expectedOffset int
		expectedData   string
	}{
		{
			description:    "initialSize size greater than data size ",
			initialSize:    1024,
			data:           []string{"foo name", ",", "123"},
			buffer:         make([]byte, 12),
			expectedOffset: 12,
			expectedData:   "foo name,123",
		},
		{
			description:    "initialSize size equal data size",
			initialSize:    12,
			data:           []string{"foo name", ",", "123"},
			buffer:         make([]byte, 1024),
			expectedOffset: 12,
			expectedData:   "foo name,123",
		},
		{
			description:    "initialSize size lower data size",
			initialSize:    1,
			data:           []string{"foo name", ",", "123"},
			buffer:         make([]byte, 1024),
			expectedOffset: 12,
			expectedData:   "foo name,123",
		},
		{
			description:    "initialSize size 0, data size 0",
			initialSize:    0,
			data:           []string{""},
			buffer:         make([]byte, 1024),
			expectedOffset: 0,
			expectedData:   "",
		},
	}

	for _, testCase := range testCases {
		testedBuffer := NewBuffer(testCase.initialSize)
		for _, value := range testCase.data {
			testedBuffer.writeString(value)
		}

		offset := copy(testCase.buffer, testedBuffer.buffer)
		assert.Equal(t, testCase.expectedOffset, offset, testCase.description)
		assert.Equal(t, testCase.expectedData, string(testCase.buffer[:offset]))
	}
}

func TestBuffer_Read(t *testing.T) {
	testCases := []struct {
		description    string
		data           []string
		dstBuffer      []byte
		expectedOffset int
		expectedData   string
		expectedErr    error
	}{
		{
			description:    "dst buffer size greater than data size",
			data:           []string{"foo name", ",", "123"},
			dstBuffer:      make([]byte, 1024),
			expectedOffset: 12,
			expectedData:   "foo name,123",
			expectedErr:    io.EOF,
		},
		{
			description:    "dst buffer size equal data size",
			data:           []string{"foo name", ",", "123"},
			dstBuffer:      make([]byte, 12),
			expectedOffset: 12,
			expectedData:   "foo name,123",
			expectedErr:    io.EOF,
		},
		{
			description:    "dst buffer size lower than data size",
			data:           []string{"foo name", ",", "123"},
			dstBuffer:      make([]byte, 1),
			expectedOffset: 1,
			expectedData:   "f",
			expectedErr:    nil,
		},
	}

	for _, testCase := range testCases {
		buffer := NewBuffer(1024)
		for _, value := range testCase.data {
			buffer.writeString(value)
		}

		offset, err := buffer.Read(testCase.dstBuffer)

		assert.Equal(t, testCase.expectedOffset, offset, testCase.description)
		assert.Equal(t, testCase.expectedErr, err, testCase.description)
		assert.Equal(t, testCase.expectedData, string(testCase.dstBuffer[:offset]))
	}
}
